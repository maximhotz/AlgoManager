@echo off
echo ========================================
echo 🚀 STARTING DAILY MLOPS PIPELINE
echo ========================================

:: Move to the script's directory
cd /d "%~dp0"

echo.
echo [1/3] Running Night Shift Labeler...
python night_shift_labeler.py

echo.
echo [2/3] Running Production AI Retraining...
python ML_Pipeline\production_retrainer.py

echo.
echo [3/3] Generating AI Performance Report...
python performance_report.py

echo.
echo ========================================
echo ✅ MLOPS PIPELINE COMPLETE
echo ========================================
pause