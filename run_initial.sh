#!/bin/bash
# 初回データロード実行スクリプト

echo "Starting Bloomberg data initial load..."
echo "This may take several hours to complete."

# 仮想環境の有効化（存在する場合）
if [ -d "venv" ]; then
    source venv/bin/activate
    echo "Virtual environment activated"
fi

# 依存パッケージのチェック
python -c "import blpapi; import pyodbc; import pandas; print('Dependencies check passed')" || {
    echo "Error: Required packages not installed. Please run: pip install -r requirements.txt"
    exit 1
}

# Bloomberg Terminalの接続チェック
echo "Checking Bloomberg Terminal connection..."
python -c "
import blpapi
try:
    session = blpapi.Session()
    if session.start():
        print('Bloomberg connection successful')
        session.stop()
    else:
        print('Bloomberg connection failed')
        exit(1)
except Exception as e:
    print(f'Bloomberg connection error: {e}')
    exit(1)
" || {
    echo "Error: Cannot connect to Bloomberg Terminal. Please ensure Bloomberg Terminal is running."
    exit 1
}

# SQL Server接続チェック
echo "Checking SQL Server connection..."
python -c "
import pyodbc
from config.database_config import get_connection_string
try:
    conn = pyodbc.connect(get_connection_string())
    print('SQL Server connection successful')
    conn.close()
except Exception as e:
    print(f'SQL Server connection error: {e}')
    exit(1)
" || {
    echo "Error: Cannot connect to SQL Server. Please check connection settings."
    exit 1
}

# メイン処理の実行
echo "Starting initial data load..."
python src/main.py --mode initial

if [ $? -eq 0 ]; then
    echo "Initial data load completed successfully!"
else
    echo "Initial data load failed. Please check logs for details."
    exit 1
fi