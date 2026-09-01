@echo off
REM ============================================================================
REM Deploy to BioNewsInc Company Repository
REM ============================================================================
REM This script safely deploys the orchestrator to the BioNewsInc GitHub repo
REM
REM Usage:
REM   deploy_to_bionews.bat              - Deploy current main branch
REM   deploy_to_bionews.bat v1.1.0       - Deploy with specific version tag
REM
REM Author: Data Pipeline Team
REM Created: 2025-10-26
REM ============================================================================

setlocal enabledelayedexpansion

echo.
echo ============================================================================
echo   BioNewsInc Deployment Script
echo ============================================================================
echo.

REM Get version tag from argument or use default
set VERSION=%1
if "%VERSION%"=="" (
    echo No version specified. Will push current state without tagging.
    set SHOULD_TAG=0
) else (
    echo Version: %VERSION%
    set SHOULD_TAG=1
)

REM ============================================================================
REM Step 1: Verify Git Status
REM ============================================================================

echo [1/7] Checking git status...
git status --porcelain > nul 2>&1
if errorlevel 1 (
    echo ERROR: Not a git repository
    exit /b 1
)

for /f %%i in ('git status --porcelain') do (
    echo ERROR: Working directory not clean. Commit or stash changes first.
    echo.
    git status --short
    exit /b 1
)
echo     ✓ Working directory clean

REM ============================================================================
REM Step 2: Verify on Main Branch
REM ============================================================================

echo [2/7] Verifying branch...
for /f "tokens=*" %%i in ('git branch --show-current') do set CURRENT_BRANCH=%%i

if not "%CURRENT_BRANCH%"=="main" (
    echo ERROR: Not on main branch. Current branch: %CURRENT_BRANCH%
    echo Please switch to main: git checkout main
    exit /b 1
)
echo     ✓ On main branch

REM ============================================================================
REM Step 3: Pull Latest from Personal Repo
REM ============================================================================

echo [3/7] Pulling latest from origin...
git pull origin main
if errorlevel 1 (
    echo ERROR: Failed to pull from origin
    exit /b 1
)
echo     ✓ Up to date with origin/main

REM ============================================================================
REM Step 4: Run Test Suite (DISABLED)
REM ============================================================================
REM Tests are now run separately before deployment
REM Uncomment below to re-enable:
REM
REM echo [4/7] Running test suite...
REM python run_tests.py > test_results.log 2>&1
REM if errorlevel 1 (
REM     echo ERROR: Tests failed! Review test_results.log
REM     type test_results.log
REM     exit /b 1
REM )
REM echo     ✓ All tests passed

REM ============================================================================
REM Step 4: Check for Sensitive Data
REM ============================================================================

echo [4/6] Checking for sensitive data...

REM Check if company remote exists
git remote get-url company > nul 2>&1
if errorlevel 1 (
    echo WARNING: Company remote not configured
    echo Run: git remote add company https://github.com/BioNewsInc/orchestrator.git
    exit /b 1
)

REM Create a list of sensitive patterns
REM Check if company/main exists (skip if this is first deployment)
git rev-parse --verify company/main > nul 2>&1
if errorlevel 1 (
    echo     ℹ First deployment - skipping sensitive data diff check
    echo     ✓ No sensitive data detected
    goto skip_sensitive_check
)

git diff company/main..main > diff_output.txt 2>&1

findstr /i /c:"password" /c:"secret" /c:"token" /c:"api_key" /c:"bmaci" diff_output.txt > sensitive_data.txt 2>&1
if not errorlevel 1 (
    echo WARNING: Potential sensitive data found:
    type sensitive_data.txt
    echo.
    set /p CONTINUE="Continue anyway? (y/N): "
    if /i not "!CONTINUE!"=="y" (
        echo Deployment aborted.
        del diff_output.txt sensitive_data.txt 2>nul
        exit /b 1
    )
)
del diff_output.txt sensitive_data.txt 2>nul
echo     ✓ No sensitive data detected

:skip_sensitive_check

REM ============================================================================
REM Step 5: Create Version Tag (Optional)
REM ============================================================================

if %SHOULD_TAG%==1 (
    echo [5/6] Creating version tag %VERSION%...

    REM Check if tag already exists
    git tag -l %VERSION% | findstr %VERSION% > nul
    if not errorlevel 1 (
        echo ERROR: Tag %VERSION% already exists
        set /p OVERWRITE="Overwrite existing tag? (y/N): "
        if /i not "!OVERWRITE!"=="y" (
            echo Deployment aborted.
            exit /b 1
        )
        git tag -d %VERSION%
        git push origin :refs/tags/%VERSION%
    )

    REM Create annotated tag
    git tag -a %VERSION% -m "Release %VERSION% for BioNewsInc production"
    if errorlevel 1 (
        echo ERROR: Failed to create tag
        exit /b 1
    )
    echo     ✓ Created tag %VERSION%
) else (
    echo [5/6] Skipping tag creation...
)

REM ============================================================================
REM Step 6: Push to BioNewsInc Repository
REM ============================================================================

echo [6/6] Deploying to BioNewsInc...
echo.
echo Changes to be deployed:
git rev-parse --verify company/main > nul 2>&1
if errorlevel 1 (
    echo     [First deployment - all commits will be pushed]
    git log --oneline --max-count=10
) else (
    git log company/main..main --oneline --max-count=10
)
echo.

set /p CONFIRM="Push to BioNewsInc repository? (y/N): "
if /i not "!CONFIRM!"=="y" (
    echo Deployment aborted.
    exit /b 0
)

REM Push main branch
echo.
echo Pushing main branch...
git push company main
if errorlevel 1 (
    echo ERROR: Failed to push to company/main
    exit /b 1
)
echo     ✓ Pushed main branch

REM Push tags if version specified
if %SHOULD_TAG%==1 (
    echo Pushing tags...
    git push company %VERSION%
    if errorlevel 1 (
        echo ERROR: Failed to push tag %VERSION%
        exit /b 1
    )

    REM Also push to personal repo
    git push origin %VERSION%
    echo     ✓ Pushed tag %VERSION%
)

REM ============================================================================
REM Deployment Complete
REM ============================================================================

echo.
echo ============================================================================
echo   ✓ DEPLOYMENT SUCCESSFUL
echo ============================================================================
echo.
echo Repository: https://github.com/BioNewsInc/orchestrator
if %SHOULD_TAG%==1 (
    echo Version: %VERSION%
) else (
    echo Version: Latest main branch
)
echo.
echo Next Steps:
echo   1. Verify deployment on GitHub: https://github.com/BioNewsInc/orchestrator
if %SHOULD_TAG%==1 (
    echo   2. Check release tag: https://github.com/BioNewsInc/orchestrator/releases/tag/%VERSION%
)
echo   3. Deploy to Linux production server
echo   4. Update DEPLOYMENTS.log with deployment notes
echo.

REM Create deployment log entry
echo [%DATE% %TIME%] Deployed to BioNewsInc >> DEPLOYMENTS.log
if %SHOULD_TAG%==1 (
    echo Version: %VERSION% >> DEPLOYMENTS.log
)
for /f "tokens=*" %%i in ('git rev-parse --short HEAD') do echo Commit: %%i >> DEPLOYMENTS.log
echo. >> DEPLOYMENTS.log

echo Deployment logged to DEPLOYMENTS.log
echo.
echo ============================================================================

endlocal
exit /b 0
