# Refinitiv Data Ingestion System

Bloomberg システム (../bloomberg/) とは完全に独立したRefinitiv EIKON Data APIを使用するデータ取得システムです。

## システム概要

Refinitiv EIKON Data APIから以下のデータを取得し、ローカルPostgreSQLに格納します：

- LME、SHFE、CMXの銅先物価格 (RIC: CMCU1, CUc1, HGc1等)
- 在庫データ (RIC: LMCASTK, LMCAWRT等)  
- 金利、為替レート (RIC: USSOFR=, JPY=等)
- 株価指数 (RIC: .SPX, .N225等)
- 企業株価 (RIC: GLEN.L, FCX.N等)

## 必要条件

- Python 3.8以上
- Refinitiv EIKON または Workspace (API Key取得用)
- PostgreSQL 12以上
- 必要なPythonパッケージ

## Bloomberg システムとの違い

| 項目 | Bloomberg (../bloomberg/) | Refinitiv (./refinitiv/) |
|------|---------------------------|--------------------------|
| **データソース** | Bloomberg Terminal API | Refinitiv EIKON Data API |
| **データベース** | Azure SQL Server | ローカル PostgreSQL |
| **ティッカー形式** | Bloomberg ティッカー | RIC コード |
| **接続方法** | BBComm | EIKON Workspace |
| **設定ファイル** | config/bloomberg_config.py | config/refinitiv_config.py |

## インストール

```bash
# プロジェクトルートから
cd refinitiv

# 仮想環境の作成（推奨）
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 依存パッケージのインストール
pip install -r requirements.txt
```

## 設定

1. `config/refinitiv_config.py` でAPI Key設定
2. `config/postgresql_config.py` でPostgreSQL接続設定
3. PostgreSQL データベースの準備

## 使用方法

```bash
# 初回データロード
python run_initial.py

# 日次更新
python run_daily.py
```

## プロジェクト構造

```
refinitiv/
├── src/                         # メインソースコード
│   ├── main.py                  # メインエントリーポイント
│   ├── refinitiv_api.py         # EIKON Data API接続
│   ├── data_processor.py        # データ変換・処理
│   ├── postgresql_db.py         # PostgreSQL接続・操作
│   └── utils.py                 # ユーティリティ関数
├── config/                      # 設定ファイル
│   ├── refinitiv_config.py      # RIC設定・マッピング
│   ├── postgresql_config.py     # PostgreSQL設定
│   └── ric_mapping.py           # Bloomberg→RIC変換テーブル
├── sql/                         # SQL スクリプト
├── docs/                        # ドキュメント
├── logs/                        # ログファイル
├── requirements.txt             # 依存パッケージ
├── run_initial.py               # 初回実行スクリプト
└── run_daily.py                 # 日次実行スクリプト
```

## 独立性の保証

このサブプロジェクトは完全に独立しており：
- Bloomberg システムのファイルを一切参照しない
- 独自の設定ファイルを使用
- 独自のデータベース (PostgreSQL) を使用
- 独自の仮想環境・依存関係を持つ

## 関連ドキュメント

- [../docs/REFINITIV_INTEGRATION_FEASIBILITY.md](../docs/REFINITIV_INTEGRATION_FEASIBILITY.md) - 実現可能性検証レポート