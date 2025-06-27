# Bloomberg データベーススキーマ詳細仕様書

## 概要
LME（ロンドン金属取引所）テナースプレッド分析のためのBloomberg APIデータを格納するAzure SQL Serverデータベース。

**データベース名**: JCL  
**サーバー**: jcz.database.windows.net  
**スキーマパターン**: マスター・ディテール設計  

---

## 1. マスターテーブル (M_*)

### M_Metal (金属銘柄マスタ)
**用途**: 対象金属（銅等）の基本情報管理

| カラム名 | データ型 | 制約 | 説明 | 例 |
|---------|---------|------|------|-----|
| MetalID | INT IDENTITY(1,1) | PK | 金属ID（自動採番） | 770 |
| MetalCode | NVARCHAR(10) | NOT NULL, UNIQUE | 金属コード | 'COPPER' |
| MetalName | NVARCHAR(50) | NOT NULL | 金属名（日本語可） | '銅' |
| CurrencyCode | NVARCHAR(3) | NOT NULL | 取引通貨 | 'USD' |
| ExchangeCode | NVARCHAR(10) | NULL | 主要取引所 | 'LME' |
| Description | NVARCHAR(255) | NULL | 説明 | 'ロンドン金属取引所銅' |

**制約**:
- PK: MetalID
- UQ: MetalCode
- FK: なし

---

### M_TenorType (限月タイプマスタ)
**用途**: 先物の限月・期間タイプ管理

| カラム名 | データ型 | 制約 | 説明 | 例 |
|---------|---------|------|------|-----|
| TenorTypeID | INT IDENTITY(1,1) | PK | テナータイプID | 1 |
| TenorTypeName | NVARCHAR(50) | NOT NULL, UNIQUE | テナータイプ名 | 'Cash', '3M Futures', 'Generic 1st Future' |
| Description | NVARCHAR(255) | NULL | 説明 | '現物価格', '3ヶ月先物' |

**Bloomberg APIマッピング**:
```
'LMCADY Index' → 'Cash' (ID: 1)
'LMCADS03 Comdty' → '3M Futures' (ID: 3)  
'LP1 Comdty' → 'Generic 1st Future' (ID: 5)
'LP2 Comdty' → 'Generic 2nd Future' (ID: 6)
...
'LP12 Comdty' → 'Generic 12th Future' (ID: 16)
```

---

### M_Indicator (指標マスタ)
**用途**: 金利・為替・指数・マクロ指標管理

| カラム名 | データ型 | 制約 | 説明 | 例 |
|---------|---------|------|------|-----|
| IndicatorID | INT IDENTITY(1,1) | PK | 指標ID | 1 |
| IndicatorCode | NVARCHAR(50) | NOT NULL, UNIQUE | 指標コード | 'USDJPY', 'NAPMPMI' |
| IndicatorName | NVARCHAR(100) | NOT NULL | 指標名 | '米ドル円為替レート', '米国PMI' |
| Category | NVARCHAR(50) | NULL | カテゴリ | 'FX', 'Macro Economic' |
| Unit | NVARCHAR(20) | NULL | 単位 | 'JPY/USD', '%' |
| Freq | NVARCHAR(10) | NULL | 更新頻度 | 'Daily', 'Monthly' |
| Description | NVARCHAR(255) | NULL | 詳細説明 | |

**カテゴリ分類**:
- 'Interest Rate': 金利（SOFRRATE, US0003M等）
- 'FX': 為替レート（USDJPY, EURUSD等）
- 'Commodity Index': コモディティ指数（BCOM, SPGSCI等）
- 'Equity Index': 株価指数（SPX, NKY等）
- 'Macro Economic': マクロ指標（PMI, GDP等）
- 'Energy': エネルギー価格

---

### M_Region (地域マスタ)
**用途**: LME在庫データの地域別管理

| カラム名 | データ型 | 制約 | 説明 | 例 |
|---------|---------|------|------|-----|
| RegionID | INT IDENTITY(1,1) | PK | 地域ID | 1 |
| RegionCode | NVARCHAR(10) | NOT NULL, UNIQUE | 地域コード | 'GLOBAL', 'ASIA' |
| RegionName | NVARCHAR(50) | NOT NULL | 地域名 | '全世界', 'アジア' |
| Description | NVARCHAR(255) | NULL | 詳細説明 | |

**Bloomberg APIマッピング**:
```
'NLSCA Index' → 'GLOBAL' (全世界)
'NLSCA %ASIA Index' → 'ASIA' (アジア)
'NLSCA %AMER Index' → 'AMER' (アメリカ)
'NLSCA %EURO Index' → 'EURO' (ヨーロッパ)
'NLSCA %MEST Index' → 'MEST' (中東)
```

---

### M_COTRCategory (COTRカテゴリマスタ)
**用途**: COTR（建玉明細報告）の投資家カテゴリ管理

| カラム名 | データ型 | 制約 | 説明 | 例 |
|---------|---------|------|------|-----|
| COTRCategoryID | INT IDENTITY(1,1) | PK | COTRカテゴリID | 1 |
| CategoryName | NVARCHAR(50) | NOT NULL, UNIQUE | カテゴリ名 | 'Investment Funds' |
| Description | NVARCHAR(255) | NULL | 説明 | '投資ファンド' |

**主要カテゴリ**:
- 'Producer/Merchant/Processor/User': 生産者・加工業者
- 'Investment Funds': 投資ファンド
- 'Other Reportables': その他報告義務者

---

### M_HoldingBand (保有比率バンドマスタ)
**用途**: バンディングレポートの保有比率区分管理

| カラム名 | データ型 | 制約 | 説明 | 例 |
|---------|---------|------|------|-----|
| BandID | INT IDENTITY(1,1) | PK | バンドID | 1 |
| BandRange | NVARCHAR(20) | NOT NULL, UNIQUE | バンド表記 | '5-9%', '40+%' |
| MinValue | DECIMAL(5,2) | NULL | 下限値 | 5.00, 40.00 |
| MaxValue | DECIMAL(5,2) | NULL | 上限値 | 9.00, 100.00 |
| Description | NVARCHAR(255) | NULL | 説明 | '5-9% holding band' |

**Bloomberg APIマッピング**:
```python
band_mapping = {
    'LMFBJAM1 Index': '5-9%',     # Band A
    'LMFBJBM1 Index': '10-19%',   # Band B  
    'LMFBJCM1 Index': '20-29%',   # Band C
    'LMFBJDM1 Index': '30-39%',   # Band D
    'LMFBJEM1 Index': '40+%'      # Band E
}
```

---

## 2. トランザクションテーブル (T_*)

### T_CommodityPrice (商品価格データ)
**用途**: LME/SHFE/CMX銅先物価格の時系列データ

| カラム名 | データ型 | 制約 | 説明 | Bloomberg Field |
|---------|---------|------|------|-----------------|
| PriceID | BIGINT IDENTITY(1,1) | PK | 価格ID（自動採番） | - |
| TradeDate | DATE | NOT NULL | 取引日 | - |
| MetalID | INT | NOT NULL, FK | 金属ID | - |
| TenorTypeID | INT | NOT NULL, FK | テナータイプID | - |
| SpecificTenorDate | DATE | NULL | 具体的満期日 | FUT_DLV_DT |
| SettlementPrice | DECIMAL(18,4) | NULL | 決済価格 | PX_LAST |
| OpenPrice | DECIMAL(18,4) | NULL | 始値 | PX_OPEN |
| HighPrice | DECIMAL(18,4) | NULL | 高値 | PX_HIGH |
| LowPrice | DECIMAL(18,4) | NULL | 安値 | PX_LOW |
| LastPrice | DECIMAL(18,4) | NULL | 最終価格 | PX_LAST |
| Volume | BIGINT | NULL | 取引量 | PX_VOLUME |
| OpenInterest | BIGINT | NULL | 建玉数 | OPEN_INT |
| MaturityDate | DATE | NULL | 満期日 | FUT_DLV_DT |
| LastUpdated | DATETIME2(0) | NOT NULL | 最終更新日時 | GETDATE() |

**制約**:
- PK: PriceID
- UQ: (TradeDate, MetalID, TenorTypeID, SpecificTenorDate)
- FK: MetalID → M_Metal, TenorTypeID → M_TenorType
- IX: (MetalID, TradeDate, TenorTypeID)

**データの特徴**:
- **Generic Futures** (LP1-LP12, CU1-CU12, HG1-HG12): SpecificTenorDate = NULL
- **特定限月契約**: SpecificTenorDate = 実際の満期日
- **スプレッド**: 'LMCADS 0003 Comdty' = Cash/3M価格差

---

### T_LMEInventory (LME在庫データ)
**用途**: LME地域別在庫情報

| カラム名 | データ型 | 制約 | 説明 | Bloomberg Ticker例 |
|---------|---------|------|------|-------------------|
| InventoryID | BIGINT IDENTITY(1,1) | PK | 在庫ID | - |
| ReportDate | DATE | NOT NULL | 報告日 | - |
| MetalID | INT | NOT NULL, FK | 金属ID | - |
| RegionID | INT | NOT NULL, FK | 地域ID | - |
| TotalStock | DECIMAL(18,0) | NULL | 総在庫量（トン） | NLSCA Index |
| OnWarrant | DECIMAL(18,0) | NULL | ワラント在庫（トン） | NLECA Index |
| CancelledWarrant | DECIMAL(18,0) | NULL | キャンセル済ワラント | NLFCA Index |
| Inflow | DECIMAL(18,0) | NULL | 流入量（トン） | NLJCA Index |
| Outflow | DECIMAL(18,0) | NULL | 流出量（トン） | NLKCA Index |
| LastUpdated | DATETIME2(0) | NOT NULL | 最終更新 | GETDATE() |

**制約**:
- PK: InventoryID
- UQ: (ReportDate, MetalID, RegionID)
- FK: MetalID → M_Metal, RegionID → M_Region
- IX: (MetalID, ReportDate, RegionID)

---

### T_OtherExchangeInventory (他取引所在庫データ)
**用途**: SHFE・CMX在庫データ

| カラム名 | データ型 | 制約 | 説明 | SHFE例 | CMX例 |
|---------|---------|------|------|--------|-------|
| OtherInvID | BIGINT IDENTITY(1,1) | PK | 在庫ID | - | - |
| ReportDate | DATE | NOT NULL | 報告日 | - | - |
| MetalID | INT | NOT NULL, FK | 金属ID | - | - |
| ExchangeCode | NVARCHAR(10) | NOT NULL | 取引所コード | 'SHFE' | 'CMX' |
| TotalStock | DECIMAL(18,0) | NULL | 総在庫量 | SHFCCOPD Index | COMXCOPR Index |
| OnWarrant | DECIMAL(18,0) | NULL | ワラント在庫 | SHFCCOPO Index | - |
| LastUpdated | DATETIME2(0) | NOT NULL | 最終更新 | GETDATE() | GETDATE() |

---

### T_MarketIndicator (市場指標データ)
**用途**: 金利・為替・指数・エネルギー価格等

| カラム名 | データ型 | 制約 | 説明 | 例 |
|---------|---------|------|------|-----|
| MarketIndID | BIGINT IDENTITY(1,1) | PK | 指標ID | - |
| ReportDate | DATE | NOT NULL | 報告日 | - |
| IndicatorID | INT | NOT NULL, FK | 指標ID | - |
| MetalID | INT | NULL, FK | 金属ID（金属特化指標の場合） | 770 (洋山プレミアム等) |
| Value | DECIMAL(18,4) | NULL | 指標値 | 150.25 (USDJPY), 3.25 (金利%) |
| LastUpdated | DATETIME2(0) | NOT NULL | 最終更新 | GETDATE() |

**主要指標分類**:
- **金利**: SOFRRATE, US0003M (単位: %)
- **為替**: USDJPY, EURUSD (単位: 通貨)
- **指数**: SPX, BCOM (単位: ポイント)
- **プレミアム**: 洋山プレミアム等 (単位: USD/MT)

---

### T_MacroEconomicIndicator (マクロ経済指標)
**用途**: PMI・GDP・CPI等の国別マクロ指標

| カラム名 | データ型 | 制約 | 説明 | 例 |
|---------|---------|------|------|-----|
| MacroIndID | BIGINT IDENTITY(1,1) | PK | マクロ指標ID | - |
| ReportDate | DATE | NOT NULL | 発表日 | - |
| IndicatorID | INT | NOT NULL, FK | 指標ID | - |
| CountryCode | NVARCHAR(3) | NULL | 国コード | 'US', 'CN', 'EU' |
| Value | DECIMAL(18,4) | NULL | 指標値 | 52.4 (PMI), 3.2 (GDP成長率%) |
| LastUpdated | DATETIME2(0) | NOT NULL | 最終更新 | GETDATE() |

**Bloomberg指標マッピング**:
```python
'US': {
    'NAPMPMI Index': '米国製造業PMI',
    'EHGDUSY Index': '米国GDP成長率',
    'EHIUUSY Index': '米国鉱工業生産指数',
    'EHPIUSY Index': '米国消費者物価指数'
}
```

---

### T_COTR (LME COTRデータ)
**用途**: LME建玉明細報告（週次）

| カラム名 | データ型 | 制約 | 説明 | 例 |
|---------|---------|------|------|-----|
| COTRID | BIGINT IDENTITY(1,1) | PK | COTRID | - |
| ReportDate | DATE | NOT NULL | 報告対象日（火曜日） | - |
| MetalID | INT | NOT NULL, FK | 金属ID | 770 |
| COTRCategoryID | INT | NOT NULL, FK | カテゴリID | - |
| LongPosition | BIGINT | NULL | ロングポジション（ロット） | 125000 |
| ShortPosition | BIGINT | NULL | ショートポジション（ロット） | 98000 |
| SpreadPosition | BIGINT | NULL | スプレッドポジション | 15000 |
| NetPosition | BIGINT | NULL | ネットポジション（計算値） | 27000 |
| LongChange | BIGINT | NULL | ロング変化量（前週比） | 3500 |
| ShortChange | BIGINT | NULL | ショート変化量（前週比） | -1200 |
| NetChange | BIGINT | NULL | ネット変化量（計算値） | 4700 |
| LongPctOpenInterest | DECIMAL(5,2) | NULL | ロング/総建玉比率(%) | 15.25 |
| ShortPctOpenInterest | DECIMAL(5,2) | NULL | ショート/総建玉比率(%) | 12.10 |
| TotalOpenInterest | BIGINT | NULL | 総建玉数 | 820000 |
| LastUpdated | DATETIME2(0) | NOT NULL | 最終更新 | GETDATE() |

---

### T_BandingReport (保有比率バンディングレポート)
**用途**: LME週次ポジション集中度レポート

| カラム名 | データ型 | 制約 | 説明 | 例 |
|---------|---------|------|------|-----|
| BandingID | BIGINT IDENTITY(1,1) | PK | バンディングID | - |
| ReportDate | DATE | NOT NULL | 報告日 | - |
| MetalID | INT | NOT NULL, FK | 金属ID | 770 |
| ReportType | NVARCHAR(50) | NOT NULL | レポートタイプ | 'Futures Long', 'Warrant' |
| TenorTypeID | INT | NULL, FK | テナータイプID | 5 (Generic 1st) |
| BandID | INT | NOT NULL, FK | バンドID | - |
| Value | DECIMAL(18,0) | NULL | 該当ポジション量 | 45000 |
| LastUpdated | DATETIME2(0) | NOT NULL | 最終更新 | GETDATE() |

**ReportType分類**:
- 'Futures Long': 先物ロング集中度
- 'Futures Short': 先物ショート集中度
- 'Warrant': ワラント保有集中度
- 'Cash': 現物取引集中度

**Bloomberg Ticker例**:
```
LMFBJAM1 Index: M1先物 バンドA(5-9%) ロング
LMFBJFM1 Index: M1先物 バンドF(5-9%) ショート
LMWARJAM Index: ワラント バンドA(5-9%)
```

---

### T_CompanyStockPrice (企業株価データ)
**用途**: 銅関連企業株価（任意取得）

| カラム名 | データ型 | 制約 | 説明 | Bloomberg例 |
|---------|---------|------|------|-------------|
| CompanyPriceID | BIGINT IDENTITY(1,1) | PK | 企業株価ID | - |
| TradeDate | DATE | NOT NULL | 取引日 | - |
| CompanyTicker | NVARCHAR(20) | NOT NULL | 企業ティッカー | 'GLEN LN Equity' |
| OpenPrice | DECIMAL(18,4) | NULL | 始値 | 425.50 |
| HighPrice | DECIMAL(18,4) | NULL | 高値 | 432.20 |
| LowPrice | DECIMAL(18,4) | NULL | 安値 | 421.30 |
| LastPrice | DECIMAL(18,4) | NULL | 終値 | 428.75 |
| Volume | BIGINT | NULL | 取引量 | 1250000 |
| LastUpdated | DATETIME2(0) | NOT NULL | 最終更新 | GETDATE() |

---

## 3. データ取得設定

### Bloomberg API設定
```python
BLOOMBERG_HOST = "localhost"
BLOOMBERG_PORT = 8194

# 取得期間設定
INITIAL_LOAD_PERIODS = {
    'prices': 5,      # 年（価格データ）
    'inventory': 5,   # 年（在庫データ）
    'indicators': 5,  # 年（市場指標）
    'macro': 10,      # 年（マクロ指標）
    'cotr': 5,        # 年（COTR）
    'banding': 3,     # 年（バンディング）
    'stocks': 5       # 年（企業株価）
}

# Bloomberg フィールド定義
PRICE_FIELDS = ['PX_LAST', 'PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PX_VOLUME', 'OPEN_INT', 'FUT_DLV_DT']
INVENTORY_FIELDS = ['PX_LAST']
INDICATOR_FIELDS = ['PX_LAST']
STOCK_FIELDS = ['PX_LAST', 'PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PX_VOLUME']
```

### データ更新頻度
- **日次**: 価格、在庫、市場指標、企業株価
- **週次**: COTR、バンディングレポート（金曜日）
- **月次**: マクロ経済指標（月初1週間）

---

## 4. 他プロジェクトでの活用方法

### データ接続設定
```python
# config/database_config.py
DATABASE_CONFIG = {
    'server': 'jcz.database.windows.net',
    'database': 'JCL',
    'username': 'TKJCZ01',
    'password': 'P@ssw0rdmbkazuresql',
    'driver': 'ODBC Driver 17 for SQL Server'
}
```

### 基本的なクエリ例
```sql
-- 銅価格データ取得（過去1ヶ月）
SELECT 
    cp.TradeDate,
    m.MetalName,
    tt.TenorTypeName,
    cp.LastPrice,
    cp.Volume
FROM T_CommodityPrice cp
JOIN M_Metal m ON cp.MetalID = m.MetalID
JOIN M_TenorType tt ON cp.TenorTypeID = tt.TenorTypeID
WHERE m.MetalCode = 'COPPER'
  AND cp.TradeDate >= DATEADD(MONTH, -1, GETDATE())
ORDER BY cp.TradeDate DESC, tt.TenorTypeID;

-- LME在庫推移取得
SELECT 
    li.ReportDate,
    r.RegionName,
    li.TotalStock,
    li.OnWarrant,
    li.Inflow,
    li.Outflow
FROM T_LMEInventory li
JOIN M_Region r ON li.RegionID = r.RegionID
JOIN M_Metal m ON li.MetalID = m.MetalID
WHERE m.MetalCode = 'COPPER'
  AND li.ReportDate >= DATEADD(MONTH, -3, GETDATE())
ORDER BY li.ReportDate DESC, r.RegionCode;

-- カーブ分析用データ（特定日のテナー構造）
SELECT 
    tt.TenorTypeName,
    cp.LastPrice,
    cp.SpecificTenorDate
FROM T_CommodityPrice cp
JOIN M_TenorType tt ON cp.TenorTypeID = tt.TenorTypeID
JOIN M_Metal m ON cp.MetalID = m.MetalID
WHERE m.MetalCode = 'COPPER'
  AND cp.TradeDate = '2024-06-27'
  AND tt.TenorTypeName LIKE 'Generic%'
ORDER BY tt.TenorTypeID;
```

### テーブル関係図
```
M_Metal (1) ←→ (N) T_CommodityPrice (N) ←→ (1) M_TenorType
    ↓
T_LMEInventory (N) ←→ (1) M_Region
    ↓
T_MarketIndicator (N) ←→ (1) M_Indicator
    ↓
T_COTR (N) ←→ (1) M_COTRCategory
    ↓
T_BandingReport (N) ←→ (1) M_HoldingBand
```

このスキーマを基に、銅のスプレッド分析、在庫相関分析、マクロファクター分析等の様々な分析プロジェクトが構築可能です。