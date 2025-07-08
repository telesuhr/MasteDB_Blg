@echo off
REM 直近6か月分の価格データとマッピングデータを一括取得

echo ===================================================
echo Fetching 6 months of Price Data and Mapping Data
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
echo This will fetch:
echo - Generic-Actual contract mappings
echo - LME copper prices (Cash, Tom-Next, 3M, LP1-LP36)
echo - LME inventory data
echo - SHFE/COMEX prices and inventory
echo - Market indicators
echo.

set /p confirm=Continue? (Y/N): 

if /i not "%confirm%"=="Y" (
    echo Process cancelled
    pause
    exit /b 0
)

echo.
echo Starting data fetch...
python src/fetch_historical_with_mapping.py %start_date% %end_date%

if %errorlevel% neq 0 (
    echo.
    echo ERROR: Failed to fetch data
    pause
    exit /b 1
)

echo.
echo ===================================================
echo All data fetched successfully!
echo ===================================================
echo.
echo You can verify the results with:
echo SELECT * FROM V_CommodityPriceWithMaturityEx WHERE GenericTicker = 'LP1 Comdty' AND TradeDate ^>= '%start_date%' ORDER BY TradeDate DESC;
echo.
pause