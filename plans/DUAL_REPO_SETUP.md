# Dual Repository Setup Guide

**Version:** 1.0
**Date:** 2025-10-26
**Purpose:** Manage local development repo + periodic pushes to BioNewsInc company repo

---

## Overview

This guide explains how to maintain two Git repositories:
1. **Personal/Development Repo** (`origin`): Your current GitHub repo for daily development
2. **Company Repo** (`company`): BioNewsInc repository for periodic releases/deployments

### Workflow Summary

```
Local Development → Push to origin (daily) → Push to company (periodic/releases)
```

---

## Initial Setup

### Step 1: Verify Current Remote

```bash
cd c:\orchestrator

# Check current remote
git remote -v
# Should show:
# origin  https://github.com/bmacinnis/orchestrator.git (fetch)
# origin  https://github.com/bmacinnis/orchestrator.git (push)
```

### Step 2: Add Company Remote

```bash
# Add BioNewsInc company repository as a second remote
git remote add company https://github.com/BioNewsInc/orchestrator.git

# Verify both remotes
git remote -v
# Should now show:
# origin   https://github.com/bmacinnis/orchestrator.git (fetch)
# origin   https://github.com/bmacinnis/orchestrator.git (push)
# company  https://github.com/BioNewsInc/orchestrator.git (fetch)
# company  https://github.com/BioNewsInc/orchestrator.git (push)
```

**Alternative:** If using SSH keys:
```bash
git remote add company git@github.com:BioNewsInc/orchestrator.git
```

### Step 3: Configure Company Remote (Optional)

```bash
# Set up separate push URL if needed
git remote set-url --push company https://github.com/BioNewsInc/orchestrator.git

# Or use different credentials
git config remote.company.pushurl https://YOUR_USERNAME@github.com/BioNewsInc/orchestrator.git
```

---

## Daily Development Workflow

### Normal Development (Push to Personal Repo)

```bash
# Make changes
git add -A
git commit -m "Your commit message"

# Push to your personal repo (default)
git push
# or explicitly:
git push origin main
```

This keeps your development repo up-to-date with all work-in-progress changes.

---

## Periodic Company Deployment Workflow

### Option 1: Push Specific Commits (Recommended)

**When ready to deploy a stable version to company repo:**

```bash
# 1. Ensure you're on main branch and up-to-date
git checkout main
git pull origin main

# 2. Create a release tag (optional but recommended)
git tag -a v1.1.0 -m "Release v1.1.0 - Job chaining and machine tracking"

# 3. Push to company repo
git push company main

# 4. Push tags to company repo
git push company --tags

# 5. Verify push
git log company/main --oneline -5
```

### Option 2: Push Specific Branches

**For feature branches or specific development:**

```bash
# Create a release branch
git checkout -b release-1.1.0

# Cherry-pick specific commits if needed
git cherry-pick abc123  # specific commit

# Push release branch to company
git push company release-1.1.0

# On company repo, merge when ready
# (or create PR on GitHub)
```

### Option 3: Sync Entire History

**First-time sync or full update:**

```bash
# Push all branches and tags
git push company --all
git push company --tags

# Or mirror the entire repo
git push company --mirror
```

---

## Best Practices

### 1. Use Tags for Company Releases

```bash
# Before pushing to company, tag the release
git tag -a v1.1.0 -m "Production release v1.1.0
- Job chaining features
- Machine tracking
- Bug fixes"

# Push tag to both repos
git push origin v1.1.0
git push company v1.1.0

# List all tags
git tag -l
```

### 2. Use Release Branches

```bash
# Create release branch for company deployment
git checkout -b release/v1.1.0

# Make any company-specific changes (if needed)
# e.g., Update README with company info

# Push to company
git push company release/v1.1.0

# Merge back to main if needed
git checkout main
git merge release/v1.1.0
```

### 3. Maintain CHANGELOG

Create `CHANGELOG.md` for company releases:

```markdown
# Changelog

## [1.1.0] - 2025-10-26
### Added
- Job chaining with --on-success, --on-failure, --on-finish
- Machine tracking for cross-platform monitoring
- Job lineage tracking with parent_job_id

### Fixed
- Machine tracking on Linux with parameterized queries
- Facebook API token switching issues

## [1.0.0] - 2025-10-23
### Added
- Initial release
- Facebook and WordPress extractors
- BigQuery integration
```

### 4. Pre-Push Checklist

Before pushing to company repo, verify:

```bash
# Run tests
python run_tests.py

# Check for sensitive data
git diff company/main..main | grep -i "password\|secret\|token\|key"

# Verify no local paths or personal info
git diff company/main..main | grep -i "c:\\\\Users\\\\bmaci"

# Clean up any local-only files
# (ensure .gitignore is correct)
```

---

## Handling Divergence

### If Company Repo Gets Ahead

```bash
# Fetch from company repo
git fetch company

# Check differences
git log main..company/main

# Merge company changes into your main
git merge company/main

# Or rebase your changes on top of company
git rebase company/main

# Push merged changes to origin
git push origin main
```

### If You Need to Sync Changes FROM Company

```bash
# Pull changes from company repo
git pull company main

# Resolve any conflicts
# Push to your personal repo
git push origin main
```

---

## Common Commands Reference

### View Remote Information

```bash
# List all remotes
git remote -v

# Show remote details
git remote show origin
git remote show company

# See what's on company repo
git ls-remote company
```

### Fetch Without Merging

```bash
# Fetch from company repo
git fetch company

# See what would be merged
git log HEAD..company/main

# Compare branches
git diff main company/main
```

### Push Specific Commits

```bash
# Push only up to a specific commit
git push company abc123:main

# Push specific commit range
git push company commit1..commit2:main
```

### Delete Remote Branch

```bash
# Delete a branch from company repo
git push company --delete feature-branch

# Delete tag from company repo
git push company --delete v1.0.0
```

---

## Deployment Workflow Example

### Scenario: Deploy v1.1.0 to Production (Company Repo)

```bash
# 1. Finish all development, commit to origin
git add -A
git commit -m "Final changes for v1.1.0"
git push origin main

# 2. Run full test suite
python run_tests.py
# Ensure all tests pass

# 3. Update version in code
# - README.md: Update version to 1.1.0
# - CHANGELOG.md: Document all changes
git add README.md CHANGELOG.md
git commit -m "Prepare release v1.1.0"
git push origin main

# 4. Create and push tag
git tag -a v1.1.0 -m "Production release v1.1.0

Features:
- Job chaining automation
- Machine tracking
- Job lineage

Tested on: Windows 11, Ubuntu 22.04
Deployment: Production"

git push origin v1.1.0

# 5. Push to company repo
git push company main
git push company v1.1.0

# 6. Verify on company repo
git log company/main --oneline -5
git tag -l

# 7. Document deployment
echo "Deployed v1.1.0 to BioNewsInc repo on $(date)" >> DEPLOYMENTS.log
git add DEPLOYMENTS.log
git commit -m "Log deployment v1.1.0"
git push origin main
```

---

## Security Considerations

### 1. Separate .env Files

**Local Development** (`.env`):
```bash
# Development credentials
FACEBOOK_ACCESS_TOKEN=dev_token_123
GCP_PROJECT_ID=dev-project-123
```

**Company/Production** (`.env.production`):
```bash
# Production credentials (never commit!)
FACEBOOK_ACCESS_TOKEN=${PROD_FB_TOKEN}
GCP_PROJECT_ID=bionews-production
```

Add to `.gitignore`:
```
.env
.env.production
.env.local
*.env.backup
```

### 2. Scrub Sensitive Data Before Company Push

```bash
# Check for secrets
git diff company/main..main | grep -i "password\|secret\|token|api_key"

# Use git-secrets (optional)
git secrets --scan
```

### 3. Use Environment-Specific Branches

```bash
# Development branch (personal)
git checkout develop
git push origin develop

# Production branch (company)
git checkout production
git merge main --no-ff
git push company production
```

---

## Troubleshooting

### Problem: "Repository not found" when pushing to company

**Solution:**
```bash
# Verify remote URL
git remote get-url company

# Update if wrong
git remote set-url company https://github.com/BioNewsInc/orchestrator.git

# Test connection
git ls-remote company
```

### Problem: Authentication failed

**Solution:**
```bash
# If using HTTPS, update credentials
git config credential.helper manager

# Or use SSH keys
git remote set-url company git@github.com:BioNewsInc/orchestrator.git

# Test SSH
ssh -T git@github.com
```

### Problem: Diverged branches

**Solution:**
```bash
# Force push (use with caution!)
git push company main --force

# Or reset company to match your main
git push company +main:main
```

### Problem: Accidentally pushed to wrong remote

**Solution:**
```bash
# Undo last push to company (if no one pulled yet)
git push company +HEAD~1:main

# Or revert specific commit
git revert abc123
git push company main
```

---

## Automation Scripts

### Script 1: Push to Company (with checks)

Create `scripts/push_to_company.sh`:

```bash
#!/bin/bash
# Push current branch to company repo with safety checks

set -e  # Exit on error

BRANCH=${1:-main}

echo "Preparing to push $BRANCH to company repo..."

# 1. Check working directory is clean
if [[ -n $(git status --porcelain) ]]; then
  echo "ERROR: Working directory not clean. Commit changes first."
  exit 1
fi

# 2. Run tests
echo "Running tests..."
python run_tests.py
if [ $? -ne 0 ]; then
  echo "ERROR: Tests failed. Fix issues before pushing to company."
  exit 1
fi

# 3. Check for sensitive data
echo "Checking for sensitive data..."
if git diff company/$BRANCH..$BRANCH | grep -qi "password\|secret\|token"; then
  echo "WARNING: Potential sensitive data found. Review diff:"
  git diff company/$BRANCH..$BRANCH | grep -i "password\|secret\|token"
  read -p "Continue anyway? (y/N) " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 1
  fi
fi

# 4. Show what will be pushed
echo "Changes to be pushed:"
git log company/$BRANCH..$BRANCH --oneline

read -p "Push these commits to company repo? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
  git push company $BRANCH
  echo "✅ Successfully pushed to company/$BRANCH"
else
  echo "Aborted."
  exit 1
fi
```

### Script 2: Sync Repos

Create `scripts/sync_repos.sh`:

```bash
#!/bin/bash
# Sync changes between personal and company repos

set -e

echo "Syncing repositories..."

# Fetch from both
git fetch origin
git fetch company

# Show status
echo "Origin (personal) status:"
git log origin/main --oneline -5

echo ""
echo "Company status:"
git log company/main --oneline -5

echo ""
echo "Commits in personal not in company:"
git log company/main..origin/main --oneline

echo ""
echo "Commits in company not in personal:"
git log origin/main..company/main --oneline
```

### Make Scripts Executable

```bash
chmod +x scripts/push_to_company.sh
chmod +x scripts/sync_repos.sh
```

---

## Summary

### Daily Workflow
1. Develop locally
2. Commit and push to `origin` (personal repo)
3. Test on Linux development server

### Periodic Deployment
1. Run full test suite
2. Create release tag
3. Push to `company` remote
4. Deploy on Linux production server

### Commands Quick Reference

```bash
# Setup
git remote add company https://github.com/BioNewsInc/orchestrator.git

# Daily
git push                          # Push to personal repo

# Periodic
git push company main             # Push to company repo
git push company --tags           # Push tags

# Check status
git remote -v                     # List remotes
git log company/main --oneline -5 # View company commits
git diff company/main..main       # Compare repos
```

---

## Related Documentation

- [LINUX_DEPLOYMENT_GUIDE.md](LINUX_DEPLOYMENT_GUIDE.md) - Linux server deployment
- [CI_CD_GUIDE.md](CI_CD_GUIDE.md) - CI/CD testing procedures
- [GITHUB_COMPLIANCE.md](GITHUB_COMPLIANCE.md) - GitHub best practices

---

**End of Dual Repository Setup Guide**
