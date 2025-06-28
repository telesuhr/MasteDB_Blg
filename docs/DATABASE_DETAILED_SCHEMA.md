# Bloomberg Data Ingestion System - 詳細データベーススキーマ

## 概要

LME（ロンドン金属取引所）のテナースプレッド分析のためのBloomberg APIデータ取得・格納システムです。
Azure SQL Database（JCL）に対して、マスター・トランザクション構造でデータを格納します。

## データベース構造

### マスターテーブル (M_*)

#### M_Metal (金属銘柄マスタ)
取引対象となる金属の基本情報を管理

| カラム名 | データ型 | 制約 | 説明 |
|----------|----------|------|------|
| MetalID | INT IDENTITY(1,1) | PK, NOT NULL | 金属ID（自動採番）|
| MetalCode | NVARCHAR(10) | UNIQUE, NOT NULL | 金属コード（例：COPPER, CU_SHFE）|
| MetalName | NVARCHAR(50) | NOT NULL | 金属名（例：Copper, Copper SHFE）|
| CurrencyCode | NVARCHAR(3) | NOT NULL | 取引通貨（USD, CNY, JPY）|
| ExchangeCode | NVARCHAR(10) | NULL | 取引所コード（LME, SHFE, CMX）|
| Description | NVARCHAR(255) | NULL | 説明文 |

**初期データ例：**
- COPPER: London Metal Exchange Copper (USD/LME)
- CU_SHFE: Shanghai Futures Exchange Copper (CNY/SHFE)
- CU_CMX: COMEX Copper (USD/CMX)

#### M_TenorType (限月タイプマスタ)
先物取引の限月タイプを管理

| カラム名 | データ型 | 制約 | 説明 |
|----------|----------|------|------|
| TenorTypeID | INT IDENTITY(1,1) | PK, NOT NULL | 限月タイプID |
| TenorTypeName | NVARCHAR(50) | UNIQUE, NOT NULL | 限月タイプ名 |
| Description | NVARCHAR(255) | NULL | 説明文 |

**主要データ：**
- Cash: 現物・スポット決済
- Tom-Next: トムネクスト
- 3M Futures: 3ヶ月先物
- Cash/3M Spread: 現物/3ヶ月スプレッド
- Generic 1st Future～Generic 12th Future: ジェネリック先物（1番限月～12番限月）

#### M_Indicator (指標マスタ)
市場指標・マクロ経済指標の定義

| カラム名 | データ型 | 制約 | 説明 |
|----------|----------|------|------|
| IndicatorID | INT IDENTITY(1,1) | PK, NOT NULL | 指標ID |
| IndicatorCode | NVARCHAR(50) | UNIQUE, NOT NULL | 指標コード（Bloombergティッカーベース）|
| IndicatorName | NVARCHAR(100) | NOT NULL | 指標名 |
| Category | NVARCHAR(50) | NULL | カテゴリ（Interest Rate, FX, Commodity Index等）|
| Unit | NVARCHAR(20) | NULL | 単位（%, Index Points, Currency等）|
| Freq | NVARCHAR(10) | NULL | 頻度（Daily, Weekly, Monthly, Yearly）|
| Description | NVARCHAR(255) | NULL | 説明文 |

**カテゴリ別指標例：**
- Interest Rate: SOFRRATE, TSFR1M, TSFR3M, US0001M, US0003M
- FX: USDJPY, EURUSD, USDCNY, USDCLP, USDPEN
- Commodity Index: BCOM, SPGSCI
- Equity Index: SPX, NKY, SHCOMP
- Energy: CP1 (WTI), CO1 (Brent), NG1 (Natural Gas)
- Physical Premium: CECN0001/0002 (Yangshan Copper Premium)
- Macro Economic: NAPMPMI (US PMI), CPMINDX (China PMI), GDP, CPI等

#### M_Region (地域マスタ)
LME在庫の地域別分類

| カラム名 | データ型 | 制約 | 説明 |
|----------|----------|------|------|
| RegionID | INT IDENTITY(1,1) | PK, NOT NULL | 地域ID |
| RegionCode | NVARCHAR(10) | UNIQUE, NOT NULL | 地域コード |
| RegionName | NVARCHAR(50) | NOT NULL | 地域名 |
| Description | NVARCHAR(255) | NULL | 説明文 |

**地域分類：**
- GLOBAL: 世界合計
- ASIA: アジア地域合計
- EURO: ヨーロッパ地域合計
- AMER: アメリカ地域合計
- MEST: 中東地域合計（※Bloomberg APIで認識されないため実運用では除外）

#### M_COTRCategory (COTRカテゴリーマスタ)
LME COTR（Commitments of Traders Report）の参加者カテゴリ

| カラム名 | データ型 | 制約 | 説明 |
|----------|----------|------|------|
| COTRCategoryID | INT IDENTITY(1,1) | PK, NOT NULL | COTRカテゴリID |
| CategoryName | NVARCHAR(50) | UNIQUE, NOT NULL | カテゴリ名 |
| Description | NVARCHAR(255) | NULL | 説明文 |

**カテゴリ：**
- Investment Funds: 投資ファンド・マネーマネージャー
- Commercial Undertakings: 商業事業者（生産者・商社・加工業者・利用者）

#### M_HoldingBand (保有比率バンドマスタ)
LMEバンディングレポートの保有比率区分

| カラム名 | データ型 | 制約 | 説明 |
|----------|----------|------|------|
| BandID | INT IDENTITY(1,1) | PK, NOT NULL | バンドID |
| BandRange | NVARCHAR(20) | UNIQUE, NOT NULL | バンド範囲表記 |
| MinValue | DECIMAL(5,2) | NULL | 下限値（%）|
| MaxValue | DECIMAL(5,2) | NULL | 上限値（%）|
| Description | NVARCHAR(255) | NULL | 説明文 |

**バンド設定例：**
- 5-9%: 5.0～9.0%の保有比率
- 10-19%: 10.0～19.0%の保有比率
- 40+%: 40.0%以上の保有比率
- 90+%: 90.0%以上の保有比率

### トランザクションテーブル (T_*)

#### T_CommodityPrice (商品価格データ)
LME、SHFE、CMXの銅先物価格データ

| カラム名 | データ型 | 制約 | 説明 |
|----------|----------|------|------|
| PriceID | BIGINT IDENTITY(1,1) | PK, NOT NULL | 価格データID |
| TradeDate | DATE | NOT NULL | 取引日 |
| MetalID | INT | FK, NOT NULL | 金属ID（M_Metal） |
| TenorTypeID | INT | FK, NOT NULL | 限月タイプID（M_TenorType） |
| SpecificTenorDate | DATE | NULL | 具体的満期日（ジェネリック先物の場合NULL）|
| SettlementPrice | DECIMAL(18,4) | NULL | 決済価格 |
| OpenPrice | DECIMAL(18,4) | NULL | 始値 |
| HighPrice | DECIMAL(18,4) | NULL | 高値 |
| LowPrice | DECIMAL(18,4) | NULL | 安値 |
| LastPrice | DECIMAL(18,4) | NULL | 終値 |
| Volume | BIGINT | NULL | 出来高 |
| OpenInterest | BIGINT | NULL | 建玉数 |
| MaturityDate | DATE | NULL | Bloomberg取得の満期日 |
| LastUpdated | DATETIME2(0) | NOT NULL | 最終更新日時 |

**ユニーク制約：** (TradeDate, MetalID, TenorTypeID, SpecificTenorDate)
**インデックス：** IX_T_CommodityPrice_MetalDateTenor

**データ例：**
- LME現物: TradeDate=2025-06-28, MetalID=1(COPPER), TenorTypeID=1(Cash), SettlementPrice=9500.00
- LME 3ヶ月先物: TradeDate=2025-06-28, MetalID=1(COPPER), TenorTypeID=4(3M Futures), SettlementPrice=9520.00

#### T_LMEInventory (LME在庫データ)
LME倉庫在庫の地域別データ

| カラム名 | データ型 | 制約 | 説明 |
|----------|----------|------|------|
| InventoryID | BIGINT IDENTITY(1,1) | PK, NOT NULL | 在庫データID |
| ReportDate | DATE | NOT NULL | レポート日 |
| MetalID | INT | FK, NOT NULL | 金属ID（M_Metal） |
| RegionID | INT | FK, NOT NULL | 地域ID（M_Region） |
| TotalStock | DECIMAL(18,0) | NULL | 総在庫量（トン）|
| OnWarrant | DECIMAL(18,0) | NULL | ワラント在庫量（トン）|
| CancelledWarrant | DECIMAL(18,0) | NULL | キャンセルワラント量（トン）|
| Inflow | DECIMAL(18,0) | NULL | 流入量（トン）|
| Outflow | DECIMAL(18,0) | NULL | 流出量（トン）|
| LastUpdated | DATETIME2(0) | NOT NULL | 最終更新日時 |

**ユニーク制約：** (ReportDate, MetalID, RegionID)
**インデックス：** IX_T_LMEInventory_MetalDateRegion

**Bloombergティッカーマッピング：**
- TotalStock: NLSCA Index (Global), NLSCA %ASIA Index (Asia), NLSCA %AMER Index (Americas), NLSCA %EURO Index (Europe)
- OnWarrant: NLECA Index系列
- CancelledWarrant: NLFCA Index系列
- Inflow: NLJCA Index系列
- Outflow: NLKCA Index系列

#### T_OtherExchangeInventory (他取引所在庫データ)
SHFE、CMXの在庫データ

| カラム名 | データ型 | 制約 | 説明 |
|----------|----------|------|------|
| OtherInvID | BIGINT IDENTITY(1,1) | PK, NOT NULL | 在庫データID |
| ReportDate | DATE | NOT NULL | レポート日 |
| MetalID | INT | FK, NOT NULL | 金属ID |
| ExchangeCode | NVARCHAR(10) | NOT NULL | 取引所コード（SHFE, CMX）|
| TotalStock | DECIMAL(18,0) | NULL | 総在庫量 |
| OnWarrant | DECIMAL(18,0) | NULL | ワラント在庫量 |
| LastUpdated | DATETIME2(0) | NOT NULL | 最終更新日時 |

**ユニーク制約：** (ReportDate, MetalID, ExchangeCode)

**SHFE在庫ティッカー：**
- SHFCCOPD Index → OnWarrant (Delivered/On Warrant)
- SHFCCOPO Index → TotalStock (Open Interest/Total)

**CMX在庫ティッカー：**
- 設定次第でマッピング

#### T_MarketIndicator (市場指標データ)
金利、為替、指数、エネルギー価格等の日次データ

| カラム名 | データ型 | 制約 | 説明 |
|----------|----------|------|------|
| MarketIndID | BIGINT IDENTITY(1,1) | PK, NOT NULL | 市場指標データID |
| ReportDate | DATE | NOT NULL | レポート日 |
| IndicatorID | INT | FK, NOT NULL | 指標ID（M_Indicator） |
| MetalID | INT | FK, NULL | 金属ID（金属固有指標の場合）|
| Value | DECIMAL(18,4) | NULL | 指標値 |
| LastUpdated | DATETIME2(0) | NOT NULL | 最終更新日時 |

**ユニーク制約：** (ReportDate, IndicatorID, MetalID)

**データ例：**
- SOFR金利: IndicatorID=1(SOFRRATE), MetalID=NULL, Value=5.25
- USD/JPY: IndicatorID=7(USDJPY), MetalID=NULL, Value=150.25
- 洋山プレミアム: IndicatorID=85(CECN0001), MetalID=1(COPPER), Value=120.00

#### T_MacroEconomicIndicator (マクロ経済指標データ)
PMI、GDP、CPI等の月次・四半期・年次データ

| カラム名 | データ型 | 制約 | 説明 |
|----------|----------|------|------|
| MacroIndID | BIGINT IDENTITY(1,1) | PK, NOT NULL | マクロ指標データID |
| ReportDate | DATE | NOT NULL | レポート日（月次等）|
| IndicatorID | INT | FK, NOT NULL | 指標ID |
| CountryCode | NVARCHAR(3) | NULL | 国コード（US, CN, EU等）|
| Value | DECIMAL(18,4) | NULL | 指標値 |
| LastUpdated | DATETIME2(0) | NOT NULL | 最終更新日時 |

**ユニーク制約：** (ReportDate, IndicatorID, CountryCode)

**データ例：**
- US PMI: ReportDate=2025-06-01, IndicatorID=88(NAPMPMI), CountryCode='US', Value=52.5
- China GDP: ReportDate=2025-03-31, IndicatorID=92(EHGDCNY), CountryCode='CN', Value=6.8

#### T_COTR (LME COTRデータ)
LME Commitments of Traders Report（週次）

| カラム名 | データ型 | 制約 | 説明 |
|----------|----------|------|------|
| COTRID | BIGINT IDENTITY(1,1) | PK, NOT NULL | COTRID |
| ReportDate | DATE | NOT NULL | レポート日（火曜日）|
| MetalID | INT | FK, NOT NULL | 金属ID |
| COTRCategoryID | INT | FK, NOT NULL | COTRカテゴリID |
| LongPosition | BIGINT | NULL | ロングポジション |
| ShortPosition | BIGINT | NULL | ショートポジション |
| SpreadPosition | BIGINT | NULL | スプレッドポジション |
| NetPosition | BIGINT | NULL | ネットポジション（Long-Short）|
| LongChange | BIGINT | NULL | ロング前週比 |
| ShortChange | BIGINT | NULL | ショート前週比 |
| NetChange | BIGINT | NULL | ネット前週比 |
| LongPctOpenInterest | DECIMAL(5,2) | NULL | ロング建玉比率（%）|
| ShortPctOpenInterest | DECIMAL(5,2) | NULL | ショート建玉比率（%）|
| TotalOpenInterest | BIGINT | NULL | 総建玉数 |
| LastUpdated | DATETIME2(0) | NOT NULL | 最終更新日時 |

**ユニーク制約：** (ReportDate, MetalID, COTRCategoryID)

**Bloombergティッカー例：**
- Investment Funds Long: CTCTMHZA Index, CTCTLUZX Index
- Investment Funds Short: CTCTGKLQ Index, CTCTWVTK Index

#### T_BandingReport (保有比率バンディングレポート)
LME Position Banding Report（週次）

| カラム名 | データ型 | 制約 | 説明 |
|----------|----------|------|------|
| BandingID | BIGINT IDENTITY(1,1) | PK, NOT NULL | バンディングID |
| ReportDate | DATE | NOT NULL | レポート日 |
| MetalID | INT | FK, NOT NULL | 金属ID |
| ReportType | NVARCHAR(50) | NOT NULL | レポートタイプ |
| TenorTypeID | INT | FK, NULL | 限月タイプID（先物の場合）|
| BandID | INT | FK, NOT NULL | バンドID |
| Value | DECIMAL(18,0) | NULL | 該当バンドの値 |
| LastUpdated | DATETIME2(0) | NOT NULL | 最終更新日時 |

**ReportType：**
- Futures Long: 先物ロングポジション
- Futures Short: 先物ショートポジション
- Warrant: ワラントポジション
- Cash: 現物ポジション
- Tom: トムネクストポジション

**ユニーク制約：** (ReportDate, MetalID, ReportType, TenorTypeID, BandID)

#### T_CompanyStockPrice (企業株価データ)
銅関連企業の株価データ

| カラム名 | データ型 | 制約 | 説明 |
|----------|----------|------|------|
| CompanyPriceID | BIGINT IDENTITY(1,1) | PK, NOT NULL | 企業株価ID |
| TradeDate | DATE | NOT NULL | 取引日 |
| CompanyTicker | NVARCHAR(20) | NOT NULL | Bloombergティッカー |
| OpenPrice | DECIMAL(18,4) | NULL | 始値 |
| HighPrice | DECIMAL(18,4) | NULL | 高値 |
| LowPrice | DECIMAL(18,4) | NULL | 安値 |
| LastPrice | DECIMAL(18,4) | NULL | 終値 |
| Volume | BIGINT | NULL | 出来高 |
| LastUpdated | DATETIME2(0) | NOT NULL | 最終更新日時 |

**ユニーク制約：** (TradeDate, CompanyTicker)

**対象企業例：**
- GLEN LN Equity: Glencore
- FCX US Equity: Freeport-McMoRan
- 銅関連企業の株価

## データ取得・処理フロー

### 1. Bloomberg APIからのデータ取得
- **Historical Data Request**: 価格データ、在庫データ等の日次・時系列データ
- **Reference Data Request**: COTR、バンディングレポート等の最新値データ

### 2. データ処理・変換
- **地域別マッピング**: LME在庫の地域コード変換（%ASIA → ASIA等）
- **限月マッピング**: LP1 → Generic 1st Future等
- **データクリーニング**: NULL値処理、数値型変換

### 3. データベース格納
- **UPSERT処理**: 既存データの更新または新規挿入
- **参照整合性**: マスターデータの自動作成・紐付け
- **トランザクション制御**: データ整合性の保証

### 4. 市場タイミング考慮
- **LME**: ロンドン時間17:00セトルメント後1時間待機
- **SHFE**: 上海時間15:00セトルメント後2時間待機
- **CMX**: NY時間13:30セトルメント後1時間待機

### 5. データ検証
- **重複データ確認**: 過去2日分との比較
- **変更率監視**: 10%以上の変更で警告
- **エラーハンドリング**: 詳細ログとエラー記録

## インデックス戦略

### パフォーマンス最適化
- **複合インデックス**: 検索条件に応じた複合キー
- **カバリングインデックス**: SELECT項目を含むインデックス
- **統計情報更新**: 定期的な統計情報メンテナンス

### 主要インデックス
- T_CommodityPrice: (MetalID, TradeDate, TenorTypeID)
- T_LMEInventory: (MetalID, ReportDate, RegionID)
- T_MarketIndicator: (ReportDate, IndicatorID)
- T_COTR: (MetalID, ReportDate, COTRCategoryID)

## 容量・メンテナンス

### データ容量見積もり
- **価格データ**: 約50レコード/日 × 365日 × 5年 ≈ 91,250レコード
- **在庫データ**: 約20レコード/日 × 365日 × 5年 ≈ 36,500レコード
- **指標データ**: 約100レコード/日 × 365日 × 5年 ≈ 182,500レコード

### メンテナンス計画
- **日次**: ログファイルのローテーション
- **週次**: インデックス断片化チェック
- **月次**: 統計情報更新
- **年次**: 古いデータのアーカイブ検討

## セキュリティ・アクセス制御

### データアクセス
- **読み取り専用ユーザー**: 分析・レポート用
- **データ更新ユーザー**: Bloomberg取得プロセス専用
- **管理者ユーザー**: スキーマ変更・メンテナンス用

### データ保護
- **暗号化**: 保存時暗号化（TDE）
- **バックアップ**: 日次自動バックアップ
- **監査ログ**: データアクセス・変更履歴の記録