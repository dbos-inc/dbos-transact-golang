package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/google/go-github/v74/github"
	"golang.org/x/oauth2"
)

type ReleaseManager struct {
	repo        *git.Repository
	githubRepo  string
	githubOwner string
	githubToken string
	client      *github.Client
}

func NewReleaseManager(repoPath string) (*ReleaseManager, error) {
	repo, err := git.PlainOpen(repoPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open repository: %w", err)
	}

	// Get GitHub token from environment
	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		return nil, fmt.Errorf("GITHUB_TOKEN environment variable is not set")
	}

	// Parse repository info from remote
	remote, err := repo.Remote("origin")
	if err != nil {
		return nil, fmt.Errorf("failed to get origin remote: %w", err)
	}

	owner, repoName := parseGitHubURL(remote.Config().URLs[0])
	if owner == "" || repoName == "" {
		return nil, fmt.Errorf("failed to parse GitHub repository from remote URL")
	}

	// Create GitHub client
	ctx := context.Background()
	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: token},
	)
	tc := oauth2.NewClient(ctx, ts)
	client := github.NewClient(tc)

	return &ReleaseManager{
		repo:        repo,
		githubRepo:  repoName,
		githubOwner: owner,
		githubToken: token,
		client:      client,
	}, nil
}

// parseGitHubURL extracts owner and repo name from GitHub URL
func parseGitHubURL(url string) (owner, repo string) {
	// Handle both SSH and HTTPS URLs
	url = strings.TrimSuffix(url, ".git")

	if strings.Contains(url, "github.com:") {
		// SSH URL: git@github.com:owner/repo
		parts := strings.Split(url, ":")
		if len(parts) == 2 {
			repoParts := strings.Split(parts[1], "/")
			if len(repoParts) == 2 {
				return repoParts[0], repoParts[1]
			}
		}
	} else if strings.Contains(url, "github.com/") {
		// HTTPS URL: https://github.com/owner/repo
		parts := strings.Split(url, "github.com/")
		if len(parts) == 2 {
			repoParts := strings.Split(parts[1], "/")
			if len(repoParts) >= 2 {
				return repoParts[0], repoParts[1]
			}
		}
	}

	return "", ""
}

// CheckPreConditions verifies the repository is ready for release
func (rm *ReleaseManager) CheckPreConditions() error {
	// Check for clean working directory
	worktree, err := rm.repo.Worktree()
	if err != nil {
		return fmt.Errorf("failed to get worktree: %w", err)
	}

	status, err := worktree.Status()
	if err != nil {
		return fmt.Errorf("failed to get status: %w", err)
	}

	if !status.IsClean() {
		return fmt.Errorf("working directory is not clean - please commit or stash changes")
	}

	// Check we're on main branch
	head, err := rm.repo.Head()
	if err != nil {
		return fmt.Errorf("failed to get HEAD: %w", err)
	}

	branchName := head.Name().Short()
	if branchName != "main" {
		return fmt.Errorf("can only make releases from main branch (current: %s)", branchName)
	}

	// Check local main is in sync with origin/main
	localCommit := head.Hash()

	remoteRef, err := rm.repo.Reference(plumbing.NewRemoteReferenceName("origin", "main"), true)
	if err != nil {
		return fmt.Errorf("failed to get origin/main reference: %w", err)
	}

	remoteCommit := remoteRef.Hash()

	if localCommit != remoteCommit {
		return fmt.Errorf("local main (%s) is not in sync with origin/main (%s)",
			localCommit.String()[:7], remoteCommit.String()[:7])
	}

	return nil
}

// GuessNextVersion determines the next version based on existing tags
func (rm *ReleaseManager) GuessNextVersion() (*semver.Version, error) {
	tags, err := rm.repo.Tags()
	if err != nil {
		return nil, fmt.Errorf("failed to get tags: %w", err)
	}

	var latestVersion *semver.Version

	// Find the highest valid semver tag
	err = tags.ForEach(func(ref *plumbing.Reference) error {
		tagName := ref.Name().Short()

		version, err := semver.NewVersion(tagName)
		if err != nil {
			return nil // Skip non-semver tags
		}

		if latestVersion == nil || version.GreaterThan(latestVersion) {
			latestVersion = version
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error iterating tags: %w", err)
	}

	if latestVersion == nil {
		return nil, fmt.Errorf("no existing semver tags found")
	}

	return semver.New(
		latestVersion.Major(),
		latestVersion.Minor()+1,
		0,
		"",
		"",
	), nil
}

// CreateAndPushTag creates a new tag and pushes it to origin
func (rm *ReleaseManager) CreateAndPushTag(version string) error {
	// Get HEAD commit
	head, err := rm.repo.Head()
	if err != nil {
		return fmt.Errorf("failed to get HEAD: %w", err)
	}

	// Create the tag locally
	_, err = rm.repo.CreateTag(version, head.Hash(), &git.CreateTagOptions{
		Message: fmt.Sprintf("Release %s", version),
		Tagger: &object.Signature{
			Name: "DBOS Go Release Action",
			When: time.Now(),
		},
	})
	if err != nil {
		return err
	}

	// Push tag using token authentication
	auth := &http.BasicAuth{
		Username: "x-access-token",
		Password: rm.githubToken,
	}

	err = rm.repo.Push(&git.PushOptions{
		RemoteName: "origin",
		RefSpecs: []config.RefSpec{
			config.RefSpec(fmt.Sprintf("refs/tags/%s:refs/tags/%s", version, version)),
		},
		Auth: auth,
	})

	if err != nil {
		// Delete the local tag if push fails
		if deleteErr := rm.repo.DeleteTag(version); deleteErr != nil {
			fmt.Printf("Warning: failed to cleanup local tag %s: %v\n", version, deleteErr)
		}
		return fmt.Errorf("failed to push tag: %w", err)
	}

	fmt.Printf("✓ Tag %s created and pushed\n", version)
	return nil
}

// CreateAndPushReleaseBranch creates a release branch and pushes it
func (rm *ReleaseManager) CreateAndPushReleaseBranch(version string) error {
	branchName := fmt.Sprintf("release/%s", version)

	// Get HEAD commit
	head, err := rm.repo.Head()
	if err != nil {
		return fmt.Errorf("failed to get HEAD: %w", err)
	}

	// Create branch reference
	branchRef := plumbing.NewBranchReferenceName(branchName)
	ref := plumbing.NewHashReference(branchRef, head.Hash())

	err = rm.repo.Storer.SetReference(ref)
	if err != nil {
		return fmt.Errorf("failed to create branch: %w", err)
	}

	// Push branch using token authentication
	auth := &http.BasicAuth{
		Username: "x-access-token",
		Password: rm.githubToken,
	}

	refSpec := config.RefSpec(fmt.Sprintf("refs/heads/%s:refs/heads/%s", branchName, branchName))
	err = rm.repo.Push(&git.PushOptions{
		RemoteName: "origin",
		RefSpecs:   []config.RefSpec{refSpec},
		Auth:       auth,
	})

	if err != nil {
		return fmt.Errorf("failed to push branch: %w", err)
	}

	fmt.Printf("✓ Branch %s created and pushed\n", branchName)
	return nil
}

// DeleteRemoteTag deletes a tag from the remote repository
func (rm *ReleaseManager) DeleteRemoteTag(version string) error {
	auth := &http.BasicAuth{
		Username: "x-access-token",
		Password: rm.githubToken,
	}

	// Push empty reference to delete remote tag
	refSpec := config.RefSpec(fmt.Sprintf(":refs/tags/%s", version))
	err := rm.repo.Push(&git.PushOptions{
		RemoteName: "origin",
		RefSpecs:   []config.RefSpec{refSpec},
		Auth:       auth,
	})

	if err != nil {
		return fmt.Errorf("failed to delete remote tag: %w", err)
	}

	return nil
}

// CreateGitHubRelease creates a GitHub release (optional)
func (rm *ReleaseManager) CreateGitHubRelease(version string) error {
	ctx := context.Background()

	release := &github.RepositoryRelease{
		TagName:              github.String(version),
		TargetCommitish:      github.String("main"),
		Name:                 github.String(fmt.Sprintf("Release %s", version)),
		Prerelease:           github.Bool(false),
		GenerateReleaseNotes: github.Bool(true),
	}

	_, _, err := rm.client.Repositories.CreateRelease(
		ctx,
		rm.githubOwner,
		rm.githubRepo,
		release,
	)

	if err != nil {
		return fmt.Errorf("failed to create GitHub release: %w", err)
	}

	fmt.Printf("✓ GitHub release %s created\n", version)
	return nil
}

func main() {
	var (
		versionFlag    = flag.String("version", "", "Version number (e.g., 1.2.3)")
		repoPathFlag   = flag.String("repo", ".", "Path to git repository")
		areYouSureFlag = flag.String("are-you-sure", "", "Confirmation to proceed")
	)
	flag.Parse()

	if *areYouSureFlag != "YES!!!" {
		log.Fatalf("Confirmation not provided. Please use -are-you-sure='YES!!!'")
	}

	// Initialize release manager
	rm, err := NewReleaseManager(*repoPathFlag)
	if err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}

	// Check pre-conditions
	fmt.Println("Checking pre-conditions...")
	if err := rm.CheckPreConditions(); err != nil {
		log.Fatalf("Pre-condition check failed: %v", err)
	}
	fmt.Println("✓ All pre-conditions met")

	// Determine version
	var version string
	if *versionFlag != "" {
		// Validate provided version
		v, err := semver.NewVersion(*versionFlag)
		if err != nil {
			log.Fatalf("Invalid version format: %v", err)
		}
		version = "v" + v.String()
		fmt.Printf("Using provided version: %s\n", version)
	} else {
		// Guess next version
		fmt.Println("Determining next version...")
		nextVersion, err := rm.GuessNextVersion()
		if err != nil {
			log.Fatalf("Failed to determine next version: %v", err)
		}
		version = "v" + nextVersion.String()
		fmt.Printf("Next version: %s\n", version)
	}

	// Create and push tag
	fmt.Printf("\nCreating tag %s...\n", version)
	if err := rm.CreateAndPushTag(version); err != nil {
		log.Fatalf("Failed to create tag: %v", err)
	}

	// Create and push release branch
	fmt.Printf("\nCreating release branch...\n")
	if err := rm.CreateAndPushReleaseBranch(version); err != nil {
		// Delete the remote tag first
		if deleteErr := rm.DeleteRemoteTag(version); deleteErr != nil {
			fmt.Printf("Warning: failed to cleanup remote tag %s: %v\n", version, deleteErr)
		}
		// Delete the local tag
		if deleteErr := rm.repo.DeleteTag(version); deleteErr != nil {
			fmt.Printf("Warning: failed to cleanup local tag %s: %v\n", version, deleteErr)
		}
		log.Fatalf("Failed to create release branch: %v", err)
	}

	// Optionally create GitHub release
	fmt.Printf("\nCreating GitHub release...\n")
	if err := rm.CreateGitHubRelease(version); err != nil {
		log.Fatalf("Failed to create GitHub release: %v", err)
	}

	fmt.Printf("\n🎉 Release %s completed successfully!\n", version)
}
