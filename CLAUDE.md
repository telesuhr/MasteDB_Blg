# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 開発で使用する主要コマンド

### データ取得システムの実行
```bash
# 初回実行（過去5-10年のヒストリカルデータ取得）
python run.py --mode initial

# 日次更新実行
python run.py --mode daily

# プラットフォーム別の実行スクリプト
./run_initial.sh     # Linux/Mac 初回実行
./run_daily.sh       # Linux/Mac 日次更新
run_initial.bat      # Windows 初回実行  
run_daily.bat        # Windows 日次更新
```

### テスト実行
```bash
# 全テスト実行
pytest tests/

# 特定のテストファイル実行
pytest tests/test_database.py

# カバレッジ付きテスト実行
pytest tests/ --cov=src
```

### 依存関係管理
```bash
# 全依存関係インストール（Bloomberg Terminal必須）
pip install -r requirements.txt

# Bloomberg API無しでインストール（開発・テスト用）
pip install -r requirements_no_blpapi.txt
```

### データベース操作
```bash
# データベーステーブル初期化（SQL Server Management Studio）
# 実行: sql/create_tables.sql

# マスターデータ挿入
# 実行: sql/insert_master_data.sql

# M_Metal通貨コード修正
# 実行: sql/fix_metal_currency_code.sql
```

## システム全体アーキテクチャ

### システム概要
Bloomberg APIから金融市場データを取得し、Azure SQL Serverに格納するデータ取得システム。LME（ロンドン金属取引所）のテナースプレッド分析を目的とし、銅先物、在庫、関連市場データを処理する。

### 主要コンポーネント

**BloombergSQLIngestor** (`src/main.py`): データパイプライン全体を統制するメインクラス:
- Bloomberg API接続管理
- データ処理ワークフローの調整
- 初回一括ロードと日次増分更新の両方をサポート
- LME/SHFE/CMX銅価格、在庫、マクロ指標など18+のデータカテゴリに対応

**BloombergDataFetcher** (`src/bloomberg_api.py`): Bloomberg APIインターフェース層:
- Bloomberg Terminal(localhost:8194)への接続処理
- バッチ処理実装（1リクエスト最大100銘柄）
- ヒストリカルデータとリファレンスデータの両方に対応
- Bloomberg Terminal未使用時のモック実装を含む
- 各種Bloombergデータタイプに対応した適切な日付処理を含むAPIレスポンス解析

**DatabaseManager** (`src/database.py`): SQL Serverデータベース操作:
- Azure SQLデータベース接続管理（JCLデータベース）
- 適切なNULL処理を含むMERGE文によるUPSERT操作実装
- マスターデータキャッシュと外部キー解決処理
- 自動マスターデータ作成とデフォルト値設定（例：金属のCurrencyCode='USD'）

**DataProcessor** (`src/data_processor.py`): データ変換パイプライン:
- Bloomberg APIレスポンスをデータベーススキーマに変換
- テナーマッピング処理（現物、トムネクスト、汎用先物等）
- 特定テナー日付 vs 汎用契約のNULL処理
- Bloombergフィールド名からデータベースカラムへのマッピング

### データフローアーキテクチャ

1. **設定読み込み**: `config/`ディレクトリからBloombergティッカーとデータベース設定を読み込み
2. **APIデータ取得**: リトライロジック付きBloomberg APIバッチリクエスト
3. **データ変換**: Bloombergレスポンス → 正規化DataFrame → データベーススキーマ
4. **マスターデータ解決**: 金属、テナータイプ、指標の自動作成・検索
5. **データベース格納**: ユニーク制約処理を含むUPSERT操作

### データベーススキーマ設計

マスター・ディテールパターンを採用:
- **マスターテーブル**: M_Metal, M_TenorType, M_Indicator, M_Region, M_COTRCategory, M_Band
- **ファクトテーブル**: T_CommodityPrice, T_LMEInventory, T_MarketIndicator等
- **ユニーク制約**: 複合キーによる重複防止（例：TradeDate+MetalID+TenorTypeID+SpecificTenorDate）

### 設定システム

**Bloomberg設定** (`config/bloomberg_config.py`):
- 特定ティッカーとフィールドマッピングを含む18+データカテゴリ定義
- テナータイプマッピング（汎用1番限月 → LP1 Comdty）
- データタイプ別ヒストリカルロード期間（3-10年）

**データベース設定** (`config/database_config.py`):
- Azure SQL Server接続設定
- テーブル名マッピングとバッチサイズ
- リトライとタイムアウト設定

### 重要な技術パターン

**エラーハンドリング**: APIとデータベース操作両方に対する指数バックオフ付きマルチレベルリトライ

**日付処理**: datetime.dateまたはdatetime.datetimeオブジェクトを返す可能性があるBloomberg日付フィールドの特別処理

**UPSERTロジック**: JOIN条件での適切なNULL値処理を含むMERGE文:
```sql
(target.SpecificTenorDate = source.SpecificTenorDate OR 
 (target.SpecificTenorDate IS NULL AND source.SpecificTenorDate IS NULL))
```

**Bloomberg API互換性**: 実際のBloomberg APIと開発・テスト用モック実装の両方に対応

### 実行モード

**初回モード**: データカテゴリ別設定可能期間（3-10年）でのヒストリカルデータロード
**日次モード**: 自動日付範囲計算と週末処理を含む増分更新

### 開発環境セットアップ

本番環境ではBBComm実行中のBloomberg Terminalが必要。Terminal未使用の開発環境では、現実的なテストデータを生成するモックBloomberg APIに自動的にフォールバック。

重要: Bloomberg API互換性とデータベース制約問題への対応で頻繁に更新される活発な開発システムのため、変更前には必ず`git pull origin main`を実行すること。