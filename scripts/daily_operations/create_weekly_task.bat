@echo off
REM ================================================
REM 週次補完タスクを手動で登録するスクリプト
REM ================================================

echo Creating Bloomberg Weekly Catch-up Task...

REM XMLファイルを作成
(
echo ^<?xml version="1.0" encoding="UTF-16"?^>
echo ^<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task"^>
echo   ^<RegistrationInfo^>
echo     ^<Description^>Bloomberg data weekly catch-up - 過去1週間の欠損データを補完^</Description^>
echo   ^</RegistrationInfo^>
echo   ^<Triggers^>
echo     ^<CalendarTrigger^>
echo       ^<StartBoundary^>2025-01-27T11:30:00^</StartBoundary^>
echo       ^<Enabled^>true^</Enabled^>
echo       ^<ScheduleByWeek^>
echo         ^<DaysOfWeek^>
echo           ^<Monday /^>
echo         ^</DaysOfWeek^>
echo         ^<WeeksInterval^>1^</WeeksInterval^>
echo       ^</ScheduleByWeek^>
echo     ^</CalendarTrigger^>
echo   ^</Triggers^>
echo   ^<Principals^>
echo     ^<Principal id="Author"^>
echo       ^<LogonType^>InteractiveToken^</LogonType^>
echo       ^<RunLevel^>HighestAvailable^</RunLevel^>
echo     ^</Principal^>
echo   ^</Principals^>
echo   ^<Settings^>
echo     ^<MultipleInstancesPolicy^>IgnoreNew^</MultipleInstancesPolicy^>
echo     ^<DisallowStartIfOnBatteries^>false^</DisallowStartIfOnBatteries^>
echo     ^<StopIfGoingOnBatteries^>false^</StopIfGoingOnBatteries^>
echo     ^<AllowHardTerminate^>true^</AllowHardTerminate^>
echo     ^<StartWhenAvailable^>true^</StartWhenAvailable^>
echo     ^<RunOnlyIfNetworkAvailable^>false^</RunOnlyIfNetworkAvailable^>
echo     ^<IdleSettings^>
echo       ^<StopOnIdleEnd^>true^</StopOnIdleEnd^>
echo       ^<RestartOnIdle^>false^</RestartOnIdle^>
echo     ^</IdleSettings^>
echo     ^<AllowStartOnDemand^>true^</AllowStartOnDemand^>
echo     ^<Enabled^>true^</Enabled^>
echo     ^<Hidden^>false^</Hidden^>
echo     ^<RunOnlyIfIdle^>false^</RunOnlyIfIdle^>
echo     ^<WakeToRun^>true^</WakeToRun^>
echo     ^<ExecutionTimeLimit^>PT2H^</ExecutionTimeLimit^>
echo     ^<Priority^>7^</Priority^>
echo   ^</Settings^>
echo   ^<Actions Context="Author"^>
echo     ^<Exec^>
echo       ^<Command^>C:\Users\09848\Git\MasteDB_Blg\scripts\daily_operations\weekly_catchup.bat^</Command^>
echo       ^<WorkingDirectory^>C:\Users\09848\Git\MasteDB_Blg^</WorkingDirectory^>
echo     ^</Exec^>
echo   ^</Actions^>
echo ^</Task^>
) > weekly_task_temp.xml

REM タスクを登録
schtasks /create /tn "BloombergDataWeekly" /xml weekly_task_temp.xml /f

REM 一時ファイルを削除
del weekly_task_temp.xml

if %errorlevel% equ 0 (
    echo.
    echo Task created successfully!
    echo.
    echo Task name: BloombergDataWeekly
    echo Schedule: Every Monday at 11:30 AM
    echo.
    echo To test the task now, run:
    echo schtasks /run /tn "BloombergDataWeekly"
    echo.
    echo To view task details:
    echo schtasks /query /tn "BloombergDataWeekly" /v
) else (
    echo Error creating task. Please run as administrator.
)