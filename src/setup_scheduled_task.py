"""
定期実行タスクのセットアップスクリプト
Windowsタスクスケジューラーに日次・週次タスクを登録
"""
import os
import sys
import subprocess
from pathlib import Path

def create_scheduled_tasks():
    """定期実行タスクを作成"""
    
    # プロジェクトのパス
    project_dir = Path(__file__).parent.parent
    
    # 日次タスク（平日のみ、午前6時に実行）
    daily_task_xml = f"""<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>Bloomberg data daily update with mapping</Description>
  </RegistrationInfo>
  <Triggers>
    <CalendarTrigger>
      <StartBoundary>2025-01-01T06:00:00</StartBoundary>
      <Enabled>true</Enabled>
      <ScheduleByWeek>
        <DaysOfWeek>
          <Monday />
          <Tuesday />
          <Wednesday />
          <Thursday />
          <Friday />
        </DaysOfWeek>
        <WeeksInterval>1</WeeksInterval>
      </ScheduleByWeek>
    </CalendarTrigger>
  </Triggers>
  <Actions>
    <Exec>
      <Command>{project_dir}\\scripts\\daily_operations\\run_daily.bat</Command>
      <WorkingDirectory>{project_dir}</WorkingDirectory>
    </Exec>
  </Actions>
</Task>"""
    
    # 週次タスク（土曜日午前7時に実行、過去1週間の欠損データを補完）
    weekly_task_xml = f"""<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>Bloomberg data weekly catch-up</Description>
  </RegistrationInfo>
  <Triggers>
    <CalendarTrigger>
      <StartBoundary>2025-01-01T07:00:00</StartBoundary>
      <Enabled>true</Enabled>
      <ScheduleByWeek>
        <DaysOfWeek>
          <Saturday />
        </DaysOfWeek>
        <WeeksInterval>1</WeeksInterval>
      </ScheduleByWeek>
    </CalendarTrigger>
  </Triggers>
  <Actions>
    <Exec>
      <Command>powershell</Command>
      <Arguments>-Command "cd '{project_dir}'; $end = Get-Date -Format 'yyyy-MM-dd'; $start = (Get-Date).AddDays(-7).ToString('yyyy-MM-dd'); python src/fetch_historical_with_mapping.py $start $end"</Arguments>
      <WorkingDirectory>{project_dir}</WorkingDirectory>
    </Exec>
  </Actions>
</Task>"""
    
    # XMLファイルを一時保存
    with open('daily_task.xml', 'w', encoding='utf-16') as f:
        f.write(daily_task_xml)
    
    with open('weekly_task.xml', 'w', encoding='utf-16') as f:
        f.write(weekly_task_xml)
    
    print("Windowsタスクスケジューラーにタスクを登録します...")
    
    try:
        # 日次タスクを登録
        subprocess.run([
            'schtasks', '/create',
            '/tn', 'BloombergDataDaily',
            '/xml', 'daily_task.xml',
            '/f'
        ], check=True)
        print("✓ 日次タスク登録完了")
        
        # 週次タスクを登録
        subprocess.run([
            'schtasks', '/create',
            '/tn', 'BloombergDataWeekly',
            '/xml', 'weekly_task.xml',
            '/f'
        ], check=True)
        print("✓ 週次タスク登録完了")
        
    finally:
        # 一時ファイルを削除
        if os.path.exists('daily_task.xml'):
            os.remove('daily_task.xml')
        if os.path.exists('weekly_task.xml'):
            os.remove('weekly_task.xml')
    
    print("\n登録されたタスク:")
    print("1. BloombergDataDaily - 平日午前6時に実行")
    print("2. BloombergDataWeekly - 土曜日午前7時に過去1週間のデータを補完")
    print("\nタスクの確認: schtasks /query /tn BloombergDataDaily")
    print("タスクの削除: schtasks /delete /tn BloombergDataDaily")

if __name__ == "__main__":
    create_scheduled_tasks()