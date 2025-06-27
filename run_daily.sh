#!/bin/bash
# 日次更新実行スクリプト

echo "Starting Bloomberg data daily update..."

# 仮想環境の有効化（存在する場合）
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Bloomberg Terminal接続チェック
python -c "
import blpapi
try:
    session = blpapi.Session()
    if not session.start():
        print('Bloomberg connection failed')
        exit(1)
    session.stop()
except Exception as e:
    print(f'Bloomberg connection error: {e}')
    exit(1)
" || {
    echo "Error: Cannot connect to Bloomberg Terminal"
    exit 1
}

# メイン処理の実行
python src/main.py --mode daily

if [ $? -eq 0 ]; then
    echo "Daily update completed successfully!"
else
    echo "Daily update failed. Please check logs for details."
    exit 1
fi