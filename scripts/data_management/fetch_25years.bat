@echo off
REM 25年分のヒストリカルデータを取得

echo ===================================================
echo 25-Year Historical Data Fetch
echo ===================================================
echo.
echo WARNING: This will fetch 25 years of data!
echo This process may take several hours.
echo The script will process data year by year and save progress.
echo If interrupted, you can resume from where it stopped.
echo.
echo Data to be fetched:
echo - Generic-Actual contract mappings
echo - LME copper prices and inventory
echo - SHFE/COMEX data
echo - Market indicators
echo.

set /p confirm=Are you sure you want to continue? (Y/N): 

if /i not "%confirm%"=="Y" (
    echo Process cancelled
    pause
    exit /b 0
)

echo.
echo Starting 25-year data fetch...
echo Check fetch_25years.log for detailed progress
echo.

REM Execute with specific year range if needed
REM python src/fetch_25years_data.py 2000 2024

REM Default: last 25 years
python src/fetch_25years_data.py

if %errorlevel% neq 0 (
    echo.
    echo Process interrupted or failed.
    echo You can resume by running this script again.
    pause
    exit /b 1
)

echo.
echo ===================================================
echo 25-year data fetch completed successfully!
echo ===================================================
pause