@echo off
REM 直近6か月分のマッピングデータを取得

echo ===================================================
echo Fetching 6 months of Generic Contract Mapping Data
echo ===================================================

REM 今日の日付と6か月前の日付を計算
powershell -Command "$end = Get-Date -Format 'yyyy-MM-dd'; $start = (Get-Date).AddMonths(-6).ToString('yyyy-MM-dd'); Write-Host \"Period: $start to $end\"; Set-Content -Path 'temp_dates.txt' -Value \"$start,$end\""

REM Read dates from temp file
for /f "tokens=1,2 delims=," %%a in (temp_dates.txt) do (
    set start_date=%%a
    set end_date=%%b
)
del temp_dates.txt

echo Period: %start_date% to %end_date%
echo.

REM Execute Python script
echo Executing: python src/historical_mapping_updater.py %start_date% %end_date%
python src/historical_mapping_updater.py %start_date% %end_date%

if %errorlevel% neq 0 (
    echo.
    echo ERROR: Failed to fetch mapping data
    pause
    exit /b 1
)

echo.
echo ===================================================
echo Process completed successfully
echo ===================================================
echo.
pause