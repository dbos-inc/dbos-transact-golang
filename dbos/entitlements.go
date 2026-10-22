package dbos

// Transact Enterprise entitlements

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
)

type enterpriseFeature struct {
	entitlement string
	description string
}

var autoUpgradeFeature = enterpriseFeature{entitlement: "autoUpgrade", description: "Auto-upgrading workflows"}

const conductorFeature = "Connecting to DBOS Conductor"

func enabledFeatures(config *Config) []enterpriseFeature {
	var features []enterpriseFeature
	if config.AutoUpgrade {
		features = append(features, autoUpgradeFeature)
	}
	return features
}

const (
	_ENTITLEMENTS_PATH    = "/v2/entitlements"
	_ENTITLEMENT_TIMEOUT  = 10 * time.Second
	_ERROR_CODE_HEADER    = "X-DBOS-Error"
	_PAST_LIMIT_CODE      = "past_limit"
	_HOSTED_CONDUCTOR_URL = "wss://cloud.dbos.dev/conductor/v1alpha1"
	_KEY_HINT             = "Set TransactEnterpriseKey or ConductorAPIKey in your DBOS configuration."
)

func defaultConductorURL() string {
	domain := os.Getenv("DBOS_DOMAIN")
	if domain == "" {
		domain = _DBOS_DOMAIN
	}
	return fmt.Sprintf("wss://%s/conductor/v1alpha1", domain)
}

func entitlementURL(conductorURL string) (string, error) {
	parsed, err := url.Parse(conductorURL)
	if err != nil {
		return "", err
	}
	scheme := parsed.Scheme
	switch scheme {
	case "wss":
		scheme = "https"
	case "ws":
		scheme = "http"
	}
	// Trim the API version from the hosted websocket URL
	path := strings.TrimSuffix(strings.TrimRight(parsed.Path, "/"), "/v1alpha1")
	route := url.URL{Scheme: scheme, Host: parsed.Host, Path: path + _ENTITLEMENTS_PATH}
	return route.String(), nil
}

func parseEntitlements(body []byte) (map[string]bool, error) {
	var answer struct {
		Entitlements map[string]any `json:"entitlements"`
	}
	if err := json.Unmarshal(body, &answer); err != nil {
		return nil, fmt.Errorf("the response is not JSON: %w", err)
	}
	if answer.Entitlements == nil {
		return nil, fmt.Errorf("the response has no entitlements")
	}
	granted := make(map[string]bool, len(answer.Entitlements))
	for name, value := range answer.Entitlements {
		granted[name] = value == true
	}
	return granted, nil
}

func problemDetail(body []byte) string {
	var problem struct {
		Detail  string `json:"detail"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &problem); err != nil {
		return ""
	}
	if problem.Detail != "" {
		return problem.Detail
	}
	return problem.Message
}

func joinFeatures(features []enterpriseFeature, name func(enterpriseFeature) string) string {
	names := make([]string, 0, len(features))
	for _, f := range features {
		names = append(names, name(f))
	}
	return strings.Join(names, " and ")
}

func requireEnterprise(features []enterpriseFeature, enterpriseKey, conductorKey, conductorURL string) error {
	if len(features) == 0 || os.Getenv("DBOS__CLOUD") == "true" {
		return nil
	}
	names := joinFeatures(features, func(f enterpriseFeature) string { return f.description })
	entitlements := joinFeatures(features, func(f enterpriseFeature) string { return f.entitlement })
	key := enterpriseKey
	if key == "" {
		key = conductorKey
	}
	target := conductorURL
	if target == "" {
		target = defaultConductorURL()
	}
	// Only a Conductor named explicitly is asked without a key, and never DBOS's hosted one.
	if key == "" && (conductorURL == "" || target == _HOSTED_CONDUCTOR_URL) {
		return models.NewInitializationError(fmt.Sprintf("%s requires a DBOS key. %s", names, _KEY_HINT))
	}
	route, err := entitlementURL(target)
	if err != nil {
		return models.NewInitializationError(fmt.Sprintf("%s requires an entitled DBOS key, but the Conductor URL is invalid: %v", names, err))
	}
	req, err := http.NewRequest(http.MethodGet, route, nil)
	if err != nil {
		return models.NewInitializationError(fmt.Sprintf("%s requires an entitled DBOS key, but the Conductor URL is invalid: %v", names, err))
	}
	req.Header.Set("User-Agent", "dbos-transact-golang/"+getDBOSVersion())
	req.Header.Set("Accept", "application/json")
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	client := &http.Client{
		Timeout: _ENTITLEMENT_TIMEOUT,
		// Refuse redirects
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	var failure string
	resp, err := client.Do(req)
	if err != nil {
		failure = err.Error()
	} else {
		defer resp.Body.Close()
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		switch {
		case readErr != nil:
			failure = readErr.Error()
		case resp.StatusCode >= 200 && resp.StatusCode < 300:
			granted, parseErr := parseEntitlements(body)
			if parseErr != nil {
				failure = parseErr.Error()
				break
			}
			var missing []enterpriseFeature
			for _, f := range features {
				if !granted[f.entitlement] {
					missing = append(missing, f)
				}
			}
			if len(missing) > 0 {
				return models.NewInitializationError(fmt.Sprintf(
					"Your DBOS organization is not entitled to %s (%s): its plan does not include it. Contact DBOS to enable it.",
					joinFeatures(missing, func(f enterpriseFeature) string { return f.entitlement }),
					joinFeatures(missing, func(f enterpriseFeature) string { return f.description })))
			}
			return nil
		default:
			detail := problemDetail(body)
			reason := detail
			if reason == "" {
				reason = http.StatusText(resp.StatusCode)
			}
			code := resp.StatusCode
			switch {
			case code == http.StatusForbidden && resp.Header.Get(_ERROR_CODE_HEADER) == _PAST_LIMIT_CODE:
				return models.NewInitializationError(fmt.Sprintf(
					"Your DBOS organization is past its plan limit, so it is not entitled to %s (%s). Upgrade its plan to continue.",
					entitlements, names))
			case code == http.StatusNotFound && detail == "":
				// A bare 404 is a missing route: a Conductor predating per-feature entitlements.
				return models.NewInitializationError(fmt.Sprintf(
					"%s requires an entitled DBOS key, but Conductor at %s has no entitlements route (HTTP 404). Upgrade Conductor, or check its URL.",
					names, route))
			case code == http.StatusUnauthorized && key == "":
				return models.NewInitializationError(fmt.Sprintf(
					"%s requires a DBOS key: Conductor at %s refused to check without one (HTTP 401): %s. %s",
					names, route, reason, _KEY_HINT))
			case code == http.StatusUnauthorized:
				// An unknown key, or a user credential instead of an API key.
				return models.NewInitializationError(fmt.Sprintf(
					"%s requires a valid DBOS key. Conductor at %s rejected the key (HTTP 401): %s",
					names, route, reason))
			case code < 500:
				// Any other refusal, such as the key's organization being gone, or a redirect.
				return models.NewInitializationError(fmt.Sprintf(
					"%s requires an entitled DBOS key, but Conductor at %s refused the check (HTTP %d): %s",
					names, route, code, reason))
			case detail != "":
				failure = fmt.Sprintf("HTTP %d: %s", code, detail)
			default:
				failure = fmt.Sprintf("HTTP %d", code)
			}
		}
	}
	// Fail closed, so an unreachable URL cannot stand in for a key.
	return models.NewInitializationError(fmt.Sprintf(
		"%s requires an entitled DBOS key, but checking it with Conductor at %s failed (%s). Retry once Conductor is reachable.",
		names, route, failure))
}
