@echo off
REM Reset and reload Generic Contract Mapping data

echo ===================================================
echo Reset and Reload Generic Contract Mapping Data
echo ===================================================

REM Get today's date and calculate 1 month ago
powershell -Command "$end = Get-Date -Format 'yyyy-MM-dd'; $start = (Get-Date).AddMonths(-1).ToString('yyyy-MM-dd'); Write-Host \"Period: $start to $end\"; Set-Content -Path 'temp_dates.txt' -Value \"$start,$end\""

REM Read dates from temp file
for /f "tokens=1,2 delims=," %%a in (temp_dates.txt) do (
    set start_date=%%a
    set end_date=%%b
)
del temp_dates.txt

echo Period: %start_date% to %end_date%
echo.

REM Check SQL file exists
if not exist "sql\reset_generic_mappings.sql" (
    echo ERROR: sql\reset_generic_mappings.sql not found
    pause
    exit /b 1
)

echo Step 1: Delete existing mapping data
echo WARNING: All data in T_GenericContractMapping will be deleted!
echo.
set /p confirm=Continue? (Y/N): 

if /i not "%confirm%"=="Y" (
    echo Process cancelled
    pause
    exit /b 0
)

REM Execute SQL
echo.
echo Please execute the following file in SQL Server Management Studio:
echo sql\reset_generic_mappings.sql
echo.
echo Press Enter when completed...
pause

echo.
echo Step 2: Reload mapping data for the last month
echo Command: python src/historical_mapping_updater.py %start_date% %end_date%
echo.

REM Execute Python script
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
echo You can verify the results with:
echo SELECT * FROM V_CommodityPriceWithMaturityEx WHERE GenericTicker = 'LP1 Comdty' AND TradeDate ^>= '%start_date%' ORDER BY TradeDate DESC;
echo.
pause