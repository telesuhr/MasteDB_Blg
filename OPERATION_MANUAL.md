# Bloomberg Data Ingestion System 運用マニュアル

## 目次
1. [システム概要](#1-システム概要)
2. [初回セットアップ](#2-初回セットアップ)
3. [定期実行の設定](#3-定期実行の設定)
4. [日常運用](#4-日常運用)
5. [トラブルシューティング](#5-トラブルシューティング)
6. [メンテナンス](#6-メンテナンス)

---

## 1. システム概要

### 1.1 目的
Bloomberg APIから金融市場データ（銅先物価格、在庫、市場指標等）を取得し、Azure SQL Serverに格納するシステムです。

### 1.2 主要機能
- **初回データロード**: 過去20年分のヒストリカルデータを一括取得
- **日次更新**: 過去3日分のデータを毎日更新
- **週次補完**: 土曜日に過去7日間の欠損データを自動補完
- **自動ロールオーバー**: 先物契約の自動切り替え管理

### 1.3 データ取得対象
- LME/SHFE/COMEX銅価格（現物、先物、スプレッド）
- 在庫データ（地域別）
- 市場指標（金利、為替、指数）
- マクロ経済指標（PMI、GDP等）
- COTRレポート（週次）
- ポジション分布データ

---

## 2. 初回セットアップ

### 2.1 前提条件
- Bloomberg Terminal実行中（BBComm起動）
- SQL Server接続可能
- Python 3.9以上インストール済み

### 2.2 環境構築
```bash
# 1. リポジトリのクローン
git clone https://github.com/your-org/MasteDB_Blg.git
cd MasteDB_Blg

# 2. 仮想環境の作成
python -m venv venv
venv\Scripts\activate

# 3. 依存パッケージのインストール
pip install -r requirements.txt
```

### 2.3 初回データロード（20年分）
```bash
# 管理者権限でコマンドプロンプトを開く
cd C:\Users\09848\Git\MasteDB_Blg

# 初回データロード実行（約6時間かかります）
python src\main.py --mode initial

# またはバッチファイル使用
run_initial.bat
```

**注意**: 初回ロードは大量のデータを取得するため、週末や夜間の実行を推奨します。

---

## 3. 定期実行の設定

### 3.1 自動設定（推奨）
管理者権限でコマンドプロンプトを開いて実行：
```bash
cd C:\Users\09848\Git\MasteDB_Blg
python src\setup_scheduled_task.py
```

これにより以下が自動登録されます：
- **日次タスク**: 平日6:00に実行
- **週次タスク**: 土曜日7:00に実行

### 3.2 手動設定（カスタマイズが必要な場合）

#### 日次タスクの登録
1. タスクスケジューラーを開く（Win + R → `taskschd.msc`）
2. 「タスクの作成」をクリック
3. 以下を設定：
   - **名前**: BloombergDataDaily
   - **トリガー**: 毎日18:00（推奨）または6:00
   - **操作**: `C:\Users\09848\Git\MasteDB_Blg\scripts\daily_operations\run_daily.bat`

#### 週次タスクの登録
```bash
# バッチファイルで簡単登録
scripts\daily_operations\create_weekly_task.bat
```

### 3.3 推奨実行時刻
| タスク | 推奨時刻 | 理由 |
|--------|----------|------|
| 日次更新 | 18:00 | LMEクローズ（17:00 GMT）後 |
| 週次補完 | 土曜 7:00 | 週末にデータ整合性確保 |

---

## 4. 日常運用

### 4.1 通常運用フロー
```
月～金: 日次タスクが自動実行（18:00）
  ↓
土曜日: 週次タスクが欠損データを補完（7:00）
  ↓
データベースに最新データが反映
```

### 4.2 手動実行方法

#### 日次更新の手動実行
```bash
cd C:\Users\09848\Git\MasteDB_Blg
scripts\daily_operations\run_daily.bat
```

#### 特定期間のデータ取得
```bash
# 例: 2025年7月18日～25日のデータを取得
python scripts\data_management\run_with_dates.py 2025-07-18 2025-07-25
```

#### 欠損データのチェックと補完
```bash
# 過去30日間の欠損をチェックして自動補完
python scripts\data_management\check_missing_dates.py --days 30 --auto-fill
```

### 4.3 実行ログの確認
```bash
# 最新のログを確認
type logs\bloomberg_ingestion_%date:~0,4%%date:~5,2%%date:~8,2%.log

# エラーログのみ確認
type logs\errors.log
```

### 4.4 データ確認SQL
```sql
-- 最新データの確認
SELECT TOP 10 * FROM T_CommodityPrice 
ORDER BY TradeDate DESC, LastUpdated DESC

-- 本日のデータ件数
SELECT COUNT(*) as TodayRecords 
FROM T_CommodityPrice 
WHERE TradeDate = CAST(GETDATE() as DATE)

-- 過去7日間のデータ取得状況
SELECT TradeDate, COUNT(*) as RecordCount
FROM T_CommodityPrice
WHERE TradeDate >= DATEADD(day, -7, GETDATE())
GROUP BY TradeDate
ORDER BY TradeDate DESC
```

---

## 5. トラブルシューティング

### 5.1 よくある問題と対処法

#### Bloomberg接続エラー
```
Error: Cannot connect to Bloomberg Terminal
```
**対処法**:
1. Bloomberg Terminalが起動しているか確認
2. BBCommサービスが実行中か確認
3. ポート8194が開いているか確認

#### SQL Server接続エラー
```
Error: Cannot connect to SQL Server
```
**対処法**:
1. SQL Serverサービスが起動しているか確認
2. `config\database_config.py`の接続設定を確認
3. ファイアウォール設定を確認

#### データ重複エラー
```
Violation of UNIQUE KEY constraint
```
**対処法**:
- 通常は無視して問題ありません（UPSERT処理により既存データは更新されます）

### 5.2 データ欠損の対処

#### 1週間以上データ取得を忘れた場合
```bash
# 欠損期間を自動検出して補完
python scripts\data_management\check_missing_dates.py --days 30 --auto-fill

# または特定期間を指定
python scripts\data_management\run_with_dates.py 2025-07-01 2025-07-24
```

### 5.3 緊急時の対応

#### タスクの一時停止
```bash
# 日次タスクを無効化
schtasks /change /tn "BloombergDataDaily" /disable

# 再開する場合
schtasks /change /tn "BloombergDataDaily" /enable
```

---

## 6. メンテナンス

### 6.1 定期メンテナンス（月次）

#### ログファイルのクリーンアップ
```bash
# 30日以上前のログを削除
forfiles /p "logs" /s /m *.log /d -30 /c "cmd /c del @path"
```

#### データ整合性チェック
```sql
-- 重複データのチェック
SELECT TradeDate, MetalID, GenericID, COUNT(*) as DupeCount
FROM T_CommodityPrice
GROUP BY TradeDate, MetalID, GenericID
HAVING COUNT(*) > 1

-- 欠損日のチェック
WITH DateRange AS (
    SELECT CAST(DATEADD(day, -30, GETDATE()) as DATE) as Date
    UNION ALL
    SELECT DATEADD(day, 1, Date)
    FROM DateRange
    WHERE Date < CAST(GETDATE() as DATE)
)
SELECT d.Date
FROM DateRange d
LEFT JOIN (
    SELECT DISTINCT TradeDate 
    FROM T_CommodityPrice
) t ON d.Date = t.TradeDate
WHERE t.TradeDate IS NULL
AND DATEPART(dw, d.Date) NOT IN (1, 7)  -- 土日を除く
```

### 6.2 年次メンテナンス

#### 古いデータのアーカイブ（必要に応じて）
```sql
-- 5年以上前のデータをアーカイブテーブルに移動
-- ※実行前に必ずバックアップを取得
```

### 6.3 システム更新

#### コードの更新
```bash
# 最新版を取得
git pull origin main

# 依存パッケージの更新
pip install -r requirements.txt --upgrade
```

---

## 付録A: 主要ファイル一覧

### 実行ファイル
- `src/main.py` - メインプログラム
- `scripts/daily_operations/run_daily.bat` - 日次更新バッチ
- `scripts/daily_operations/weekly_catchup.bat` - 週次補完バッチ
- `src/setup_scheduled_task.py` - タスク自動登録

### 設定ファイル
- `config/bloomberg_config.py` - Bloomberg設定（ティッカー、期間）
- `config/database_config.py` - データベース接続設定
- `config/logging_config.py` - ログ設定

### ユーティリティ
- `scripts/data_management/check_missing_dates.py` - 欠損チェック
- `scripts/data_management/run_with_dates.py` - 期間指定実行

---

## 付録B: 緊急連絡先

- システム管理者: [担当者名]
- Bloomberg サポート: [電話番号]
- データベース管理者: [担当者名]

---

最終更新日: 2025年7月24日