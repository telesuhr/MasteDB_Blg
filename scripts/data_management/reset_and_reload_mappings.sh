#!/bin/bash
# 既存のマッピングデータを削除して、直近1か月分を再取得するスクリプト

echo "==================================================="
echo "Generic Contract Mappingデータのリセットと再取得"
echo "==================================================="

# 今日の日付と1か月前の日付を取得
end_date=$(date +%Y-%m-%d)
start_date=$(date -d "1 month ago" +%Y-%m-%d)

echo "対象期間: $start_date から $end_date"
echo ""

# SQLファイルの存在確認
if [ ! -f "sql/reset_generic_mappings.sql" ]; then
    echo "エラー: sql/reset_generic_mappings.sql が見つかりません"
    exit 1
fi

echo "ステップ1: 既存のマッピングデータを削除します"
echo "警告: T_GenericContractMappingテーブルの全データが削除されます！"
echo ""
read -p "続行しますか？ (Y/N): " confirm

if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
    echo "処理を中止しました"
    exit 0
fi

echo ""
echo "SQL Server Management Studioで以下のファイルを実行してください:"
echo "sql/reset_generic_mappings.sql"
echo ""
read -p "実行が完了したら、Enterキーを押してください..."

echo ""
echo "ステップ2: 直近1か月のマッピングデータを再取得します"
echo "実行コマンド: python src/historical_mapping_updater.py $start_date $end_date"
echo ""

# Pythonスクリプトを実行
python src/historical_mapping_updater.py "$start_date" "$end_date"

if [ $? -ne 0 ]; then
    echo ""
    echo "エラー: マッピングデータの取得に失敗しました"
    exit 1
fi

echo ""
echo "==================================================="
echo "処理が完了しました"
echo "==================================================="
echo ""
echo "次のSQLで結果を確認できます:"
echo "SELECT * FROM V_CommodityPriceWithMaturityEx WHERE GenericTicker = 'LP1 Comdty' AND TradeDate >= '$start_date' ORDER BY TradeDate DESC;"
echo ""