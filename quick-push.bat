@echo off
git add -A
git commit -m "auto-commit: %date% %time%"
git push origin HEAD:main
echo Done! Press any key to exit.
pause >nul

