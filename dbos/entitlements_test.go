package dbos

// The entitlement check every enterprise feature goes through, against a local stand-in
// for Conductor's entitlements route.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A feature for exercising the check itself, entitled under a name Conductor may not know.
var someFeature = enterpriseFeature{entitlement: "someFeature", description: "Some feature"}

func allEntitled() map[string]any {
	return map[string]any{"sharding": true, "autoUpgrade": true, "suspendWorkflows": true}
}

type entitlementRequest struct {
	path          string
	authorization string // the Authorization header, "" when none was sent
}

// entitlementsStandIn is a local stand-in for Conductor's entitlements route. Tests name
// it as their Conductor URL, and the SDK finds /v2/entitlements beside it.
type entitlementsStandIn struct {
	server *httptest.Server

	mu       sync.Mutex
	status   int
	body     []byte
	headers  map[string]string
	requests []entitlementRequest
}

func newEntitlementsStandIn(t *testing.T) *entitlementsStandIn {
	t.Helper()
	s := &entitlementsStandIn{}
	entitlements := allEntitled()
	entitlements["someFeature"] = true
	s.entitle(entitlements)
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.requests = append(s.requests, entitlementRequest{path: r.URL.Path, authorization: r.Header.Get("Authorization")})
		for name, value := range s.headers {
			w.Header().Set(name, value)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(s.status)
		_, _ = w.Write(s.body)
	}))
	t.Cleanup(s.server.Close)
	return s
}

// url is the websocket URL an application would configure for this Conductor.
func (s *entitlementsStandIn) url() string {
	return "ws://" + strings.TrimPrefix(s.server.URL, "http://")
}

func (s *entitlementsStandIn) answer(status int, body any, headers map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = status
	s.headers = headers
	switch b := body.(type) {
	case []byte:
		s.body = b
	case string:
		s.body = []byte(b)
	default:
		encoded, err := json.Marshal(body)
		if err != nil {
			panic(err)
		}
		s.body = encoded
	}
}

// reject answers every request with an RFC 9457 problem carrying detail.
func (s *entitlementsStandIn) reject(status int, detail string, headers map[string]string) {
	s.answer(status, map[string]any{"title": "Rejected", "status": status, "detail": detail}, headers)
}

func (s *entitlementsStandIn) entitle(entitlements map[string]any) {
	s.answer(http.StatusOK, map[string]any{"entitlements": entitlements}, nil)
}

// bearers lists the Authorization headers of the requests so far.
func (s *entitlementsStandIn) bearers() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.requests))
	for _, r := range s.requests {
		out = append(out, r.authorization)
	}
	return out
}

func (s *entitlementsStandIn) seen() []entitlementRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]entitlementRequest(nil), s.requests...)
}

func TestEntitlementURL(t *testing.T) {
	for conductorURL, want := range map[string]string{
		"wss://cloud.dbos.dev/conductor/v1alpha1":  "https://cloud.dbos.dev/conductor/v2/entitlements",
		"wss://cloud.dbos.dev/conductor/v1alpha1/": "https://cloud.dbos.dev/conductor/v2/entitlements",
		// Credentials in the URL are dropped, so they are never sent or quoted in errors.
		"wss://user:secret@conductor.example.com:8443/conductor/v1alpha1": "https://conductor.example.com:8443/conductor/v2/entitlements",
		"ws://user:secret@[::1]:8090":                                     "http://[::1]:8090/v2/entitlements",
		// A self-hosted Conductor serves its routes from the root.
		"ws://localhost:8090":            "http://localhost:8090/v2/entitlements",
		"https://conductor.example.com/": "https://conductor.example.com/v2/entitlements",
	} {
		got, err := entitlementURL(conductorURL)
		require.NoError(t, err, conductorURL)
		assert.Equal(t, want, got, conductorURL)
	}
}

func TestDefaultConductorFollowsTheDBOSDomain(t *testing.T) {
	t.Setenv("DBOS_DOMAIN", "")
	assert.Equal(t, _HOSTED_CONDUCTOR_URL, defaultConductorURL())
	t.Setenv("DBOS_DOMAIN", "staging.dbos.dev")
	assert.Equal(t, "wss://staging.dbos.dev/conductor/v1alpha1", defaultConductorURL())
}

func TestEntitlements(t *testing.T) {
	t.Setenv("DBOS__CLOUD", "")
	t.Setenv("DBOS_DOMAIN", "")

	check := func(conductor *entitlementsStandIn, enterpriseKey, conductorKey string, features ...enterpriseFeature) error {
		if features == nil {
			features = []enterpriseFeature{someFeature}
		}
		return requireEnterprise(features, enterpriseKey, conductorKey, conductor.url())
	}
	initializationError := func(t *testing.T, err error) string {
		t.Helper()
		require.Error(t, err)
		var dbosErr *Error
		require.True(t, assert.ErrorAs(t, err, &dbosErr), "expected *Error, got %T", err)
		assert.Equal(t, ErrorCodeInitialization, dbosErr.Code)
		return dbosErr.Message
	}

	t.Run("AnEntitledKeyUnlocksEnterpriseFeatures", func(t *testing.T) {
		conductor := newEntitlementsStandIn(t)
		require.NoError(t, check(conductor, "enterprise-key", ""))
		require.NoError(t, check(conductor, "", "conductor-key"))
		// The enterprise key is checked when both are set.
		require.NoError(t, check(conductor, "enterprise-key", "conductor-key"))
		assert.Equal(t, []entitlementRequest{
			{"/v2/entitlements", "Bearer enterprise-key"},
			{"/v2/entitlements", "Bearer conductor-key"},
			{"/v2/entitlements", "Bearer enterprise-key"},
		}, conductor.seen())
	})

	t.Run("EachFeatureNeedsItsOwnEntitlement", func(t *testing.T) {
		conductor := newEntitlementsStandIn(t)
		conductor.entitle(map[string]any{"sharding": true, "autoUpgrade": false, "someFeature": true})
		require.NoError(t, check(conductor, "enterprise-key", ""))
		message := initializationError(t, check(conductor, "enterprise-key", "", someFeature, autoUpgradeFeature))
		// Names only what the org lacks, by entitlement and by what it does.
		assert.Contains(t, message, "not entitled to autoUpgrade (Auto-upgrading workflows)")
		assert.Contains(t, message, "plan does not include it")
		assert.NotContains(t, message, "someFeature")
		assert.NotContains(t, message, "enterprise-key")
		// One request answers for every feature.
		assert.Len(t, conductor.seen(), 2)
	})

	t.Run("APastLimitOrgIsToldToUpgrade", func(t *testing.T) {
		conductor := newEntitlementsStandIn(t)
		// Conductor refuses a past-limit org outright, tagging the 403 with a code.
		conductor.reject(http.StatusForbidden, "your DBOS account is past the free trial limit", map[string]string{"X-DBOS-Error": "past_limit"})
		message := initializationError(t, check(conductor, "enterprise-key", ""))
		assert.Contains(t, message, "past its plan limit, so it is not entitled to someFeature")
		assert.Contains(t, message, "Upgrade its plan")
		// The code, not the status or the prose, marks it: a bare 403 is another refusal.
		conductor.reject(http.StatusForbidden, "your DBOS account is past the free trial limit", nil)
		message = initializationError(t, check(conductor, "enterprise-key", ""))
		assert.Contains(t, message, "refused the check (HTTP 403)")
		assert.NotContains(t, message, "Upgrade its plan")
	})

	t.Run("AnEntitlementConductorDoesNotReportIsNotGranted", func(t *testing.T) {
		conductor := newEntitlementsStandIn(t)
		// A Conductor older than the feature doesn't list it, which grants nothing.
		conductor.entitle(allEntitled())
		assert.Contains(t, initializationError(t, check(conductor, "enterprise-key", "")), "not entitled to someFeature")
		// Anything but true grants nothing either.
		conductor.entitle(map[string]any{"someFeature": "yes"})
		assert.Contains(t, initializationError(t, check(conductor, "enterprise-key", "")), "not entitled to someFeature")
	})

	t.Run("EnterpriseFeaturesRequireAKey", func(t *testing.T) {
		conductor := newEntitlementsStandIn(t)
		// DBOS's hosted Conductor, implicit or named, is never asked without a key.
		for _, url := range []string{"", _HOSTED_CONDUCTOR_URL} {
			message := initializationError(t, requireEnterprise([]enterpriseFeature{someFeature}, "", "", url))
			assert.Contains(t, message, ": Some feature requires a DBOS key. Set TransactEnterpriseKey")
		}
		assert.Empty(t, conductor.seen())
	})

	t.Run("ASelfHostedConductorIsAskedWithoutAKey", func(t *testing.T) {
		conductor := newEntitlementsStandIn(t)
		// Without OAuth, a self-hosted Conductor grants every feature to any caller.
		require.NoError(t, check(conductor, "", ""))
		assert.Equal(t, []entitlementRequest{{"/v2/entitlements", ""}}, conductor.seen())
		// With OAuth, it wants a key.
		conductor.reject(http.StatusUnauthorized, "this endpoint requires a DBOS API key", nil)
		message := initializationError(t, check(conductor, "", ""))
		assert.Contains(t, message, "requires a DBOS key: Conductor at")
		assert.Contains(t, message, "(HTTP 401): this endpoint requires a DBOS API key")
		assert.Contains(t, message, "Set TransactEnterpriseKey")
	})

	t.Run("ConductorCredentialsNeverReachErrors", func(t *testing.T) {
		conductor := newEntitlementsStandIn(t)
		conductor.reject(http.StatusUnauthorized, "invalid API key", nil)
		withCredentials := strings.Replace(conductor.url(), "ws://", "ws://user:secret@", 1)
		message := initializationError(t, requireEnterprise([]enterpriseFeature{someFeature}, "enterprise-key", "", withCredentials))
		assert.Contains(t, message, "rejected the key (HTTP 401)")
		assert.NotContains(t, message, "secret")
		assert.NotContains(t, message, "user")
	})

	t.Run("ARejectedKeyIsRefused", func(t *testing.T) {
		conductor := newEntitlementsStandIn(t)
		// An unknown key, or a user credential, which the route refuses in favor of an API key.
		for _, detail := range []string{"invalid API key", "this endpoint requires a DBOS API key"} {
			conductor.reject(http.StatusUnauthorized, detail, nil)
			message := initializationError(t, check(conductor, "enterprise-key", ""))
			assert.Contains(t, message, "requires a valid DBOS key.")
			assert.Contains(t, message, "rejected the key (HTTP 401): "+detail)
			assert.NotContains(t, message, "enterprise-key")
		}
	})

	t.Run("AnyOtherRefusalQuotesConductor", func(t *testing.T) {
		conductor := newEntitlementsStandIn(t)
		for status, detail := range map[int]string{
			http.StatusForbidden: "forbidden",
			http.StatusNotFound:  "organization not found",
			http.StatusConflict:  "conflict",
			// Throttling is a refusal too: the check is not retried, and never skipped.
			http.StatusTooManyRequests: "slow down",
		} {
			conductor.reject(status, detail, nil)
			message := initializationError(t, check(conductor, "enterprise-key", ""))
			assert.Contains(t, message, fmt.Sprintf("refused the check (HTTP %d): %s", status, detail))
			assert.NotContains(t, message, "rejected the key")
			assert.NotContains(t, message, "enterprise-key")
		}
	})

	t.Run("AConductorWithoutTheRouteIsRefused", func(t *testing.T) {
		conductor := newEntitlementsStandIn(t)
		// An older Conductor's router answers an unknown route with no problem detail.
		conductor.answer(http.StatusNotFound, "404 page not found", nil)
		assert.Contains(t, initializationError(t, check(conductor, "enterprise-key", "")), "has no entitlements route (HTTP 404)")
	})

	t.Run("ARedirectIsRefusedAndNeverCarriesTheKey", func(t *testing.T) {
		conductor := newEntitlementsStandIn(t)
		// Following a redirect would re-send the request, headers included, to the host
		// the Location names: that would hand the key to whoever answered.
		elsewhere := newEntitlementsStandIn(t)
		conductor.answer(http.StatusFound, "", map[string]string{"Location": elsewhere.server.URL + "/v2/entitlements"})
		assert.Contains(t, initializationError(t, check(conductor, "enterprise-key", "")), "refused the check (HTTP 302)")
		// The redirect target never saw the key, or anything at all.
		assert.Empty(t, elsewhere.seen())
	})

	t.Run("AMalformedAnswerBlocksStartup", func(t *testing.T) {
		conductor := newEntitlementsStandIn(t)
		for _, body := range []any{[]any{}, map[string]any{"entitlements": nil}, map[string]any{"entitled": true}, "not json"} {
			conductor.answer(http.StatusOK, body, nil)
			assert.Contains(t, initializationError(t, check(conductor, "enterprise-key", "")), "Retry once Conductor")
		}
	})

	t.Run("AnUnreachableConductorBlocksStartup", func(t *testing.T) {
		conductor := newEntitlementsStandIn(t)
		conductor.reject(http.StatusServiceUnavailable, "unavailable", nil)
		message := initializationError(t, check(conductor, "enterprise-key", ""))
		assert.Contains(t, message, "checking it with Conductor at")
		assert.Contains(t, message, "failed (HTTP 503: unavailable)")
		// A Conductor that does not answer at all.
		conductor.server.Close()
		message = initializationError(t, check(conductor, "enterprise-key", ""))
		assert.Contains(t, message, "Retry once Conductor is reachable")
	})

	t.Run("DBOSCloudPassesWithoutACheck", func(t *testing.T) {
		conductor := newEntitlementsStandIn(t)
		t.Setenv("DBOS__CLOUD", "true")
		require.NoError(t, check(conductor, "", ""))
		assert.Empty(t, conductor.seen())
	})

	t.Run("NoFeatureNeedsNoCheck", func(t *testing.T) {
		conductor := newEntitlementsStandIn(t)
		require.NoError(t, requireEnterprise(nil, "", "", conductor.url()))
		assert.Empty(t, conductor.seen())
	})
}
