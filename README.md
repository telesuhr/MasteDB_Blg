# MasterDB Bloomberg Data Ingestion System

LME（ロンドン金属取引所）のテナースプレッド分析のために、Bloomberg APIからデータを取得してSQL Serverデータベースに格納するシステムです。

## システム概要

このシステムは、Bloomberg APIから以下のデータを取得し、Azure SQL Database（JCL）に格納します：
- LME、SHFE、CMXの銅先物価格
- 在庫データ（地域別含む）
- 金利、為替レート
- マクロ経済指標
- COTRレポート
- ポジションバンディングレポート
- エネルギー価格、企業株価など

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
python src/main.py --mode initial
```

### 日次更新
```bash
python src/main.py --mode daily
```

## プロジェクト構造

```
MasterDB_Blg/
├── src/
│   ├── main.py              # メインエントリーポイント
│   ├── bloomberg_api.py     # Bloomberg API接続・データ取得
│   ├── database.py          # SQL Server接続・データ格納
│   ├── data_processor.py    # データ変換・処理
│   └── utils.py             # ユーティリティ関数
├── config/
│   ├── database_config.py   # データベース設定
│   ├── bloomberg_config.py  # Bloombergティッカー定義
│   └── logging_config.py    # ロギング設定
├── sql/
│   └── create_tables.sql    # テーブル作成SQL
├── logs/                    # ログファイル
├── tests/                   # テストコード
└── docs/                    # ドキュメント
```

## ライセンス

Private Repository