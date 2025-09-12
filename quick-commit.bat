@echo off
git add -A
git commit -m "auto-commit: %date% %time%"
echo Committed! Use quick-push.bat to push to GitHub.
pause >nul

