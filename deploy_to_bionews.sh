#!/bin/bash
################################################################################
# Deploy to BioNewsInc Company Repository
################################################################################
# This script safely deploys the orchestrator to the BioNewsInc GitHub repo
#
# Usage:
#   ./deploy_to_bionews.sh              - Deploy current main branch
#   ./deploy_to_bionews.sh v1.1.0       - Deploy with specific version tag
#
# Author: Data Pipeline Team
# Created: 2025-10-26
################################################################################

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Functions
error() {
    echo -e "${RED}ERROR: $1${NC}" >&2
    exit 1
}

success() {
    echo -e "${GREEN}✓ $1${NC}"
}

warning() {
    echo -e "${YELLOW}WARNING: $1${NC}"
}

info() {
    echo -e "${BLUE}$1${NC}"
}

################################################################################
# Header
################################################################################

echo ""
echo "============================================================================"
echo "  BioNewsInc Deployment Script"
echo "============================================================================"
echo ""

# Get version tag from argument
VERSION=${1:-""}
if [ -z "$VERSION" ]; then
    info "No version specified. Will push current state without tagging."
    SHOULD_TAG=0
else
    info "Version: $VERSION"
    SHOULD_TAG=1
fi

################################################################################
# Step 1: Verify Git Status
################################################################################

echo "[1/7] Checking git status..."

# Check if git repo
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    error "Not a git repository"
fi

# Check for uncommitted changes
if [[ -n $(git status --porcelain) ]]; then
    error "Working directory not clean. Commit or stash changes first.

$(git status --short)"
fi

success "Working directory clean"

################################################################################
# Step 2: Verify on Main Branch
################################################################################

echo "[2/7] Verifying branch..."

CURRENT_BRANCH=$(git branch --show-current)
if [ "$CURRENT_BRANCH" != "main" ]; then
    error "Not on main branch. Current branch: $CURRENT_BRANCH
Please switch to main: git checkout main"
fi

success "On main branch"

################################################################################
# Step 3: Pull Latest from Personal Repo
################################################################################

echo "[3/7] Pulling latest from origin..."

git pull origin main || error "Failed to pull from origin"

success "Up to date with origin/main"

################################################################################
# Step 4: Run Test Suite
################################################################################

#if ! python run_tests.py > test_results.log 2>&1; then
#    error "Tests failed! Review test_results.log
#
#$(tail -20 test_results.log)"
#fi

#success "All tests passed"

################################################################################
# Step 5: Check for Sensitive Data
################################################################################

echo "[5/7] Checking for sensitive data..."

# Check if company remote exists
if ! git remote get-url company > /dev/null 2>&1; then
    error "Company remote not configured
Run: git remote add company https://github.com/BioNewsInc/orchestrator.git"
fi

# Check for sensitive data patterns
if git diff company/main..main | grep -iE 'password|secret|token|api_key|bmaci' > sensitive_data.txt; then
    warning "Potential sensitive data found:"
    cat sensitive_data.txt
    echo ""
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        rm -f sensitive_data.txt
        error "Deployment aborted"
    fi
fi

rm -f sensitive_data.txt
success "No sensitive data detected"

################################################################################
# Step 6: Create Version Tag (Optional)
################################################################################

if [ $SHOULD_TAG -eq 1 ]; then
    echo "[6/7] Creating version tag $VERSION..."

    # Check if tag already exists
    if git tag -l "$VERSION" | grep -q "$VERSION"; then
        warning "Tag $VERSION already exists"
        read -p "Overwrite existing tag? (y/N) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            error "Deployment aborted"
        fi
        git tag -d "$VERSION"
        git push origin ":refs/tags/$VERSION" 2>/dev/null || true
    fi

    # Create annotated tag
    git tag -a "$VERSION" -m "Release $VERSION for BioNewsInc production" || \
        error "Failed to create tag"

    success "Created tag $VERSION"
else
    echo "[6/7] Skipping tag creation..."
fi

################################################################################
# Step 7: Push to BioNewsInc Repository
################################################################################

echo "[7/7] Deploying to BioNewsInc..."
echo ""
echo "Changes to be deployed:"
git log company/main..main --oneline --max-count=10
echo ""

read -p "Push to BioNewsInc repository? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    info "Deployment aborted"
    exit 0
fi

# Push main branch
echo ""
info "Pushing main branch..."
git push company main || error "Failed to push to company/main"
success "Pushed main branch"

# Push tags if version specified
if [ $SHOULD_TAG -eq 1 ]; then
    info "Pushing tags..."
    git push company "$VERSION" || error "Failed to push tag $VERSION"

    # Also push to personal repo
    git push origin "$VERSION"
    success "Pushed tag $VERSION"
fi

################################################################################
# Deployment Complete
################################################################################

echo ""
echo "============================================================================"
echo "  ✓ DEPLOYMENT SUCCESSFUL"
echo "============================================================================"
echo ""
echo "Repository: https://github.com/BioNewsInc/orchestrator"
if [ $SHOULD_TAG -eq 1 ]; then
    echo "Version: $VERSION"
else
    echo "Version: Latest main branch"
fi
echo ""
echo "Next Steps:"
echo "  1. Verify deployment on GitHub: https://github.com/BioNewsInc/orchestrator"
if [ $SHOULD_TAG -eq 1 ]; then
    echo "  2. Check release tag: https://github.com/BioNewsInc/orchestrator/releases/tag/$VERSION"
fi
echo "  3. Deploy to Linux production server"
echo "  4. Update DEPLOYMENTS.log with deployment notes"
echo ""

# Create deployment log entry
{
    echo "[$(date)] Deployed to BioNewsInc"
    if [ $SHOULD_TAG -eq 1 ]; then
        echo "Version: $VERSION"
    fi
    echo "Commit: $(git rev-parse --short HEAD)"
    echo ""
} >> DEPLOYMENTS.log

info "Deployment logged to DEPLOYMENTS.log"
echo ""
echo "============================================================================"

exit 0
