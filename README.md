# MasterDB Bloomberg Data Ingestion System

LME（ロンドン金属取引所）のテナースプレッド分析のために、Bloomberg APIからデータを取得してSQL Serverデータベースに格納するシステムです。市場タイミングを考慮し、データ検証機能を備えた高度なデータ取得システムです。

## システム概要

このシステムは、Bloomberg APIから以下のデータを取得し、Azure SQL Database（JCL）に格納します：
- LME、SHFE、CMXの銅先物価格
- 在庫データ（地域別：AMER、ASIA、EURO）
- 金利、為替レート
- マクロ経済指標
- COTRレポート
- ポジションバンディングレポート
- エネルギー価格、企業株価など

## 主な機能

- **市場タイミング対応**: 各取引所のセトルメント時間を考慮した適切なタイミングでのデータ取得
- **データ検証**: 既存データとの比較による異常値検出
- **地域別在庫**: LME在庫の地域別データ（MEST地域は自動除外）
- **タイムゾーン対応**: 各市場のタイムゾーンを考慮した処理
- **エラーハンドリング**: 包括的なエラー処理とログ記録

## 必要条件

- Python 3.8以上
- Bloomberg Terminal（BBCommが起動している状態）
- Azure SQL Database接続権限
- 必要なPythonパッケージ（requirements.txt参照）

## インストール

```bash
# リポジトリのクローン
git clone https://github.com/telesuhr/MasteDB_Blg.git
cd MasteDB_Blg

# 仮想環境の作成（推奨）
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 依存パッケージのインストール
pip install -r requirements.txt
```

## 設定

1. `config/` ディレクトリ内の設定ファイルを環境に合わせて編集
2. Bloomberg APIの接続設定を確認
3. SQL Serverの接続情報を設定

## 使用方法

### 初回データロード（過去データの一括取得）
```bash
# プロジェクトルートディレクトリから実行
python run.py --mode initial

# または実行スクリプトを使用
# Linux/Mac:
./run_initial.sh
# Windows:
run_initial.bat
```

### 日次更新（推奨）
```bash
# 標準的な日次更新（自動でマッピング更新・価格データ取得）
scripts\daily_operations\run_daily.bat

# Linux/Mac:
./scripts/daily_operations/run_daily.sh
```

### 期間を指定したデータ取得
```bash
# 特定期間のデータを一括取得（全カテゴリ）
python scripts/data_management/run_with_dates.py 2025-01-01 2025-03-01

# または対話形式
scripts\data_management\fetch_period_data.bat

# 特定カテゴリのみ取得
python scripts/data_management/run_with_dates.py 2025-01-01 2025-03-01 --categories LME_INVENTORY INTEREST_RATES
```

### 契約情報の更新
```bash
# Bloomberg APIから実契約の正確な情報を取得
scripts\data_management\update_contract_info.bat

# 特定の契約のみ更新
scripts\data_management\update_contract_info.bat LPF25 LPG25 LPH25
```

## 主な機能と特徴

### 日次更新の特徴
- **自動マッピング更新**: Bloomberg APIから実契約の正確な情報を自動取得
- **市場タイミング**: LME（17:00ロンドン時間+1時間）、SHFE（15:00上海時間+2時間）、CMX（13:30NY時間+1時間）
- **データ検証**: 過去2日分の既存データと新規データを比較
- **変更率監視**: 10%以上の変更率で警告を出力
- **自動除外**: 問題のあるティッカー（MEST地域など）を自動除外

### データ更新方式
- **MERGE文（UPSERT）**: 既存データは更新、新規データは挿入
- **重複実行安全**: 同じ期間を複数回実行しても問題なし
- **Bloomberg API優先**: 常に最新のBloombergデータで更新

## プロジェクト構造

```
MasterDB_Blg/
├── src/                           # メインソースコード
│   ├── main.py                    # メインエントリーポイント
│   ├── enhanced_daily_update.py   # 拡張日次更新（市場タイミング・検証）
│   ├── bloomberg_api.py           # Bloomberg API接続・データ取得
│   ├── database.py                # SQL Server接続・データ格納
│   ├── data_processor.py          # データ変換・処理
│   ├── historical_mapping_updater.py # Generic-Actual契約マッピング更新
│   ├── run_daily_with_mapping.py  # マッピング付き日次更新
│   └── utils.py                   # ユーティリティ関数
├── config/                        # 設定ファイル
│   ├── database_config.py         # データベース設定
│   ├── bloomberg_config.py        # Bloombergティッカー定義
│   └── logging_config.py          # ロギング設定
├── sql/                           # SQL スクリプト
│   ├── checks/                    # データ確認用SQL
│   ├── fixes/                     # データ修正用SQL
│   ├── views/                     # ビュー作成・更新SQL
│   ├── create_tables.sql          # テーブル作成SQL
│   └── insert_master_data.sql     # マスタデータ初期化
├── scripts/                       # 実行スクリプト
│   ├── daily_operations/          # 日次実行用
│   ├── data_management/           # データ管理・一括取得
│   ├── data_loaders/              # 各種データローダー
│   └── testing/                   # テスト・検証用
├── docs/                          # ドキュメント
│   ├── DATABASE_DETAILED_SCHEMA.md  # 詳細データベーススキーマ
│   └── BLOOMBERG_TICKER_MAPPING.md  # Bloombergティッカーマッピング
├── logs/                          # ログファイル
└── CLAUDE.md                      # プロジェクト指示書
```

## 詳細ドキュメント

- **[詳細データベーススキーマ](docs/DATABASE_DETAILED_SCHEMA.md)**: 全テーブル構造、フィールド詳細、インデックス、制約の完全ドキュメント
- **[Bloombergティッカーマッピング](docs/BLOOMBERG_TICKER_MAPPING.md)**: 各Bloombergティッカーとデータベースフィールドの詳細マッピング
- **[既存ドキュメント](DATABASE_SCHEMA_DOCUMENTATION.md)**: 基本的なスキーマ概要

## ライセンス

Private Repository