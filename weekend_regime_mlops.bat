@echo off
echo ========================================
echo 🏛️ STARTING WEEKLY REGIME RETRAINING
echo ========================================

:: Move to the script's directory, then step into the Regime_Filter folder
cd /d "%~dp0\Regime_Filter"

echo.
echo [1/2] Running Production Oracle (HMM 100%% Data Labeling)...
python oracle_labeler.py

echo.
echo [2/2] Running Production Soldier (Random Forest Retraining)...
python train_soldier.py

echo.
echo ========================================
echo ✅ WEEKLY REGIME MLOPS COMPLETE
echo ========================================
pause