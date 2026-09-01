@echo off
REM ===========================================================================
REM  pull_configs.bat -- pull ONLY config changes from the repo to this laptop.
REM
REM  The prod box auto-commits schema-discovery changes to configs/*.yaml and
REM  pushes them to both repos (shared/config_autostage.py emails you when it
REM  does). This pulls JUST the config files -- not any other code changes --
REM  so you can take a config update without dragging in unrelated commits.
REM
REM  How it stays config-only: it fetches (touches nothing), then checks out
REM  ONLY the configs/ paths from the remote. Your other files and any
REM  in-progress work are left exactly as they are. HEAD is not moved.
REM
REM  Safe with a dirty tree: only ever writes files under configs/. If you have
REM  uncommitted edits to a config it would change, it warns and skips that file
REM  (pass --force to overwrite).
REM
REM  For a normal full pull instead, just run:  git pull
REM
REM  Usage:  scripts\pull_configs.bat   [--force]
REM ===========================================================================
setlocal EnableDelayedExpansion

set "REMOTE=origin"
set "BRANCH=main"
set "FORCE=0"

:parse
if "%~1"=="" goto endparse
if /I "%~1"=="--force" set "FORCE=1" & shift & goto parse
echo Unknown option: %~1
exit /b 1
:endparse

for /f "delims=" %%R in ('git rev-parse --show-toplevel 2^>nul') do set "REPO=%%R"
if not defined REPO ( echo Not inside a git checkout. & exit /b 1 )
pushd "%REPO%"

echo pull_configs: fetching %REMOTE%/%BRANCH% ^(no files changed yet^)...
git fetch %REMOTE% %BRANCH% >nul 2>&1
if errorlevel 1 ( echo git fetch failed. & popd & exit /b 1 )

set "UPSTREAM=%REMOTE%/%BRANCH%"

REM Which config files differ between our HEAD and the remote?
set "CHANGED="
for /f "delims=" %%F in ('git diff --name-only "HEAD..%UPSTREAM%" -- "configs/*.yaml" 2^>nul') do (
  set "CHANGED=!CHANGED! %%F"
)

if not defined CHANGED (
  echo pull_configs: no config changes waiting -- already up to date.
  popd & endlocal & exit /b 0
)

echo pull_configs: config files changed upstream:
for %%F in (!CHANGED!) do echo     %%F

REM Don't clobber configs you're actively editing (unless --force).
set "LOCALMOD=|"
for /f "tokens=1,*" %%A in ('git status --porcelain -- "configs/*.yaml"') do set "LOCALMOD=!LOCALMOD!%%B|"

set "APPLY="
for %%F in (!CHANGED!) do (
  set "HIT=0"
  echo(!LOCALMOD! | findstr /C:"|%%F|" >nul && set "HIT=1"
  if "!HIT!"=="1" if "%FORCE%"=="0" (
    echo   SKIP %%F -- you have local edits. Commit/stash it, or use --force.
  ) else (
    set "APPLY=!APPLY! %%F"
  )
)

if not defined APPLY (
  echo pull_configs: nothing applied ^(differing configs are locally modified^). Use --force.
  popd & endlocal & exit /b 0
)

REM Surgical: writes ONLY these config paths from the remote. Nothing else is
REM touched, and HEAD is not moved (so unrelated code changes do NOT come down).
git checkout %UPSTREAM% --!APPLY!
if errorlevel 1 ( echo checkout failed -- nothing changed. & popd & endlocal & exit /b 1 )

echo.
echo pull_configs: updated ONLY these config file^(s^) from %UPSTREAM%:
for %%F in (!APPLY!) do echo     %%F
echo.
echo They are now staged. Review with 'git diff --cached', then commit.
echo ^(To also fast-forward your branch later for everything else: git pull^)
popd
endlocal
exit /b 0
