# Bloomberg ティッカーマッピング詳細

## 概要

本ドキュメントでは、Bloomberg APIから取得するデータの各ティッカーコードとデータベースフィールドの詳細なマッピングを説明します。

## 価格データ (T_CommodityPrice)

### LME銅価格 (LME_COPPER_PRICES)

| Bloombergティッカー | 説明 | TenorType | 取得フィールド |
|---------------------|------|-----------|----------------|
| LMCADY Index | LME銅現物価格 | Cash | PX_LAST, PX_OPEN, PX_HIGH, PX_LOW, PX_VOLUME |
| CAD TT00 Comdty | LME銅トムネクスト | Tom-Next | PX_LAST, PX_OPEN, PX_HIGH, PX_LOW, PX_VOLUME |
| LMCADS03 Comdty | LME銅3ヶ月先物 | 3M Futures | PX_LAST, PX_OPEN, PX_HIGH, PX_LOW, PX_VOLUME, OPEN_INT, FUT_DLV_DT |
| LMCADS 0003 Comdty | LME銅現物/3ヶ月スプレッド | Cash/3M Spread | PX_LAST |
| LP1 Comdty | LME銅ジェネリック1番限月 | Generic 1st Future | PX_LAST, PX_OPEN, PX_HIGH, PX_LOW, PX_VOLUME, OPEN_INT, FUT_DLV_DT |
| LP2 Comdty | LME銅ジェネリック2番限月 | Generic 2nd Future | PX_LAST, PX_OPEN, PX_HIGH, PX_LOW, PX_VOLUME, OPEN_INT, FUT_DLV_DT |
| ... | ... | ... | ... |
| LP12 Comdty | LME銅ジェネリック12番限月 | Generic 12th Future | PX_LAST, PX_OPEN, PX_HIGH, PX_LOW, PX_VOLUME, OPEN_INT, FUT_DLV_DT |

### SHFE銅価格 (SHFE_COPPER_PRICES)

| Bloombergティッカー | 説明 | TenorType | 取得フィールド |
|---------------------|------|-----------|----------------|
| CU1 Comdty | SHFE銅ジェネリック1番限月 | Generic 1st Future | PX_LAST, PX_OPEN, PX_HIGH, PX_LOW, PX_VOLUME, OPEN_INT, FUT_DLV_DT |
| CU2 Comdty | SHFE銅ジェネリック2番限月 | Generic 2nd Future | PX_LAST, PX_OPEN, PX_HIGH, PX_LOW, PX_VOLUME, OPEN_INT, FUT_DLV_DT |
| ... | ... | ... | ... |
| CU12 Comdty | SHFE銅ジェネリック12番限月 | Generic 12th Future | PX_LAST, PX_OPEN, PX_HIGH, PX_LOW, PX_VOLUME, OPEN_INT, FUT_DLV_DT |

### CMX銅価格 (CMX_COPPER_PRICES)

| Bloombergティッカー | 説明 | TenorType | 取得フィールド |
|---------------------|------|-----------|----------------|
| HG1 Comdty | CMX銅ジェネリック1番限月 | Generic 1st Future | PX_LAST, PX_OPEN, PX_HIGH, PX_LOW, PX_VOLUME, OPEN_INT, FUT_DLV_DT |
| HG2 Comdty | CMX銅ジェネリック2番限月 | Generic 2nd Future | PX_LAST, PX_OPEN, PX_HIGH, PX_LOW, PX_VOLUME, OPEN_INT, FUT_DLV_DT |
| ... | ... | ... | ... |
| HG12 Comdty | CMX銅ジェネリック12番限月 | Generic 12th Future | PX_LAST, PX_OPEN, PX_HIGH, PX_LOW, PX_VOLUME, OPEN_INT, FUT_DLV_DT |

## 在庫データ (T_LMEInventory / T_OtherExchangeInventory)

### LME在庫 (LME_INVENTORY)

| データタイプ | 地域 | Bloombergティッカー | DBフィールド | 説明 |
|-------------|------|---------------------|-------------|------|
| total_stock | GLOBAL | NLSCA Index | TotalStock | 世界合計総在庫 |
| total_stock | AMER | NLSCA %AMER Index | TotalStock | アメリカ地域総在庫 |
| total_stock | ASIA | NLSCA %ASIA Index | TotalStock | アジア地域総在庫 |
| total_stock | EURO | NLSCA %EURO Index | TotalStock | ヨーロッパ地域総在庫 |
| on_warrant | GLOBAL | NLECA Index | OnWarrant | 世界合計ワラント在庫 |
| on_warrant | AMER | NLECA %AMER Index | OnWarrant | アメリカ地域ワラント在庫 |
| on_warrant | ASIA | NLECA %ASIA Index | OnWarrant | アジア地域ワラント在庫 |
| on_warrant | EURO | NLECA %EURO Index | OnWarrant | ヨーロッパ地域ワラント在庫 |
| cancelled_warrant | GLOBAL | NLFCA Index | CancelledWarrant | 世界合計キャンセルワラント |
| cancelled_warrant | AMER | NLFCA %AMER Index | CancelledWarrant | アメリカ地域キャンセルワラント |
| cancelled_warrant | ASIA | NLFCA %ASIA Index | CancelledWarrant | アジア地域キャンセルワラント |
| cancelled_warrant | EURO | NLFCA %EURO Index | CancelledWarrant | ヨーロッパ地域キャンセルワラント |
| inflow | GLOBAL | NLJCA Index | Inflow | 世界合計流入量 |
| inflow | AMER | NLJCA %AMER Index | Inflow | アメリカ地域流入量 |
| inflow | ASIA | NLJCA %ASIA Index | Inflow | アジア地域流入量 |
| inflow | EURO | NLJCA %EURO Index | Inflow | ヨーロッパ地域流入量 |
| outflow | GLOBAL | NLKCA Index | Outflow | 世界合計流出量 |
| outflow | AMER | NLKCA %AMER Index | Outflow | アメリカ地域流出量 |
| outflow | ASIA | NLKCA %ASIA Index | Outflow | アジア地域流出量 |
| outflow | EURO | NLKCA %EURO Index | Outflow | ヨーロッパ地域流出量 |

**注意**: MEST地域（%MEST Index）は Bloomberg APIで認識されないため、システムで自動除外されます。

**取得フィールド**: PX_LAST, LAST_PRICE, CUR_MKT_VALUE（フォールバック）

### SHFE在庫 (SHFE_INVENTORY)

| Bloombergティッカー | データタイプ | DBフィールド | 説明 |
|---------------------|-------------|-------------|------|
| SHFCCOPD Index | on_warrant | OnWarrant | SHFE銅受渡済み在庫（ワラント相当）|
| SHFCCOPO Index | total_stock | TotalStock | SHFE銅建玉残高（総在庫相当）|
| SFCDTOTL Index | total_stock | TotalStock | SHFE銅合計在庫（代替）|

### CMX在庫 (CMX_INVENTORY)

| Bloombergティッカー | データタイプ | DBフィールド | 説明 |
|---------------------|-------------|-------------|------|
| CMXCU Index | total_stock | TotalStock | CMX銅総在庫 |

## 市場指標データ (T_MarketIndicator)

### 金利 (INTEREST_RATES)

| Bloombergティッカー | IndicatorCode | 説明 | 単位 | 頻度 |
|---------------------|---------------|------|------|------|
| SOFRRATE Index | SOFRRATE | 米国担保付翌日物調達金利 | % | Daily |
| TSFR1M Index | TSFR1M | CME Term SOFR 1ヶ月 | % | Daily |
| TSFR3M Index | TSFR3M | CME Term SOFR 3ヶ月 | % | Daily |
| US0001M Index | US0001M | 米ドル1ヶ月LIBOR | % | Daily |
| US0003M Index | US0003M | 米ドル3ヶ月LIBOR | % | Daily |

### 為替レート (FX_RATES)

| Bloombergティッカー | IndicatorCode | 説明 | 単位 | 頻度 |
|---------------------|---------------|------|------|------|
| USDJPY Curncy | USDJPY | 米ドル/日本円 | Currency | Daily |
| EURUSD Curncy | EURUSD | ユーロ/米ドル | Currency | Daily |
| USDCNY Curncy | USDCNY | 米ドル/中国元 | Currency | Daily |
| USDCLP Curncy | USDCLP | 米ドル/チリペソ | Currency | Daily |
| USDPEN Curncy | USDPEN | 米ドル/ペルーソル | Currency | Daily |

### コモディティ指数 (COMMODITY_INDICES)

| Bloombergティッカー | IndicatorCode | 説明 | 単位 | 頻度 |
|---------------------|---------------|------|------|------|
| BCOM Index | BCOM | ブルームバーグコモディティ指数 | Index Points | Daily |
| SPGSCI Index | SPGSCI | S&P GSCIコモディティ指数 | Index Points | Daily |

### 株価指数 (EQUITY_INDICES)

| Bloombergティッカー | IndicatorCode | 説明 | 単位 | 頻度 |
|---------------------|---------------|------|------|------|
| SPX Index | SPX | S&P 500指数 | Index Points | Daily |
| NKY Index | NKY | 日経225指数 | Index Points | Daily |
| SHCOMP Index | SHCOMP | 上海総合指数 | Index Points | Daily |

### エネルギー価格 (ENERGY_PRICES)

| Bloombergティッカー | IndicatorCode | 説明 | 単位 | 頻度 |
|---------------------|---------------|------|------|------|
| CP1 Index | CP1 | WTI原油先物 | USD/Barrel | Daily |
| CO1 Index | CO1 | ブレント原油先物 | USD/Barrel | Daily |
| NG1 Index | NG1 | 天然ガス先物 | USD/MMBtu | Daily |

### 現物プレミアム (PHYSICAL_PREMIUMS)

| Bloombergティッカー | IndicatorCode | 説明 | 単位 | 頻度 | MetalID |
|---------------------|---------------|------|------|------|---------|
| CECN0001 Index | CECN0001 | 中国洋山銅プレミアム | USD/tonne | Daily | COPPER |
| CECN0002 Index | CECN0002 | 中国洋山銅プレミアム（代替）| USD/tonne | Daily | COPPER |

### その他指標 (OTHER_INDICATORS)

| Bloombergティッカー | IndicatorCode | 説明 | 単位 | 頻度 |
|---------------------|---------------|------|------|------|
| BDIY Index | BDIY | バルチック海運指数 | Index Points | Daily |

## マクロ経済指標データ (T_MacroEconomicIndicator)

### PMI指標 (MACRO_PMI)

| Bloombergティッカー | IndicatorCode | 説明 | 国コード | 単位 | 頻度 |
|---------------------|---------------|------|----------|------|------|
| NAPMPMI Index | NAPMPMI | 米国ISM製造業PMI | US | Index | Monthly |
| CPMINDX Index | CPMINDX | 中国製造業PMI | CN | Index | Monthly |
| MPMIEUMA Index | MPMIEUMA | EU製造業PMI | EU | Index | Monthly |

### GDP指標 (MACRO_GDP)

| Bloombergティッカー | IndicatorCode | 説明 | 国コード | 単位 | 頻度 |
|---------------------|---------------|------|----------|------|------|
| EHGDUSY Index | EHGDUSY | 米国実質GDP | US | YoY % | Yearly |
| EHGDCNY Index | EHGDCNY | 中国実質GDP | CN | YoY % | Yearly |

### 鉱工業生産 (MACRO_INDUSTRIAL)

| Bloombergティッカー | IndicatorCode | 説明 | 国コード | 単位 | 頻度 |
|---------------------|---------------|------|----------|------|------|
| EHIUUSY Index | EHIUUSY | 米国鉱工業生産 | US | YoY % | Yearly |
| EHIUCNY Index | EHIUCNY | 中国鉱工業生産 | CN | YoY % | Yearly |

### CPI指標 (MACRO_CPI)

| Bloombergティッカー | IndicatorCode | 説明 | 国コード | 単位 | 頻度 |
|---------------------|---------------|------|----------|------|------|
| EHPIUSY Index | EHPIUSY | 米国消費者物価指数 | US | YoY % | Yearly |
| EHPICNY Index | EHPICNY | 中国消費者物価指数 | CN | YoY % | Yearly |

## COTRデータ (T_COTR)

### Investment Funds（投資ファンド）

| Bloombergティッカー | ポジション | データタイプ | 説明 |
|---------------------|-----------|-------------|------|
| CTCTMHZA Index | Long | Position | 投資ファンドロングポジション |
| CTCTLUZX Index | Long | Percentage | 投資ファンドロング建玉比率 |
| CTCTGKLQ Index | Short | Position | 投資ファンドショートポジション |
| CTCTWVTK Index | Short | Percentage | 投資ファンドショート建玉比率 |

### Commercial Undertakings（商業事業者）

| Bloombergティッカー | ポジション | データタイプ | 説明 |
|---------------------|-----------|-------------|------|
| CTCTVDQG Index | Long | Position | 商業事業者ロングポジション |
| CTCTFSWP Index | Long | Percentage | 商業事業者ロング建玉比率 |
| CTCTFHAX Index | Short | Position | 商業事業者ショートポジション |
| CTCTZTIH Index | Short | Percentage | 商業事業者ショート建玉比率 |

**更新頻度**: 週次（火曜日レポート）
**取得フィールド**: PX_LAST

## バンディングレポート (T_BandingReport)

### 先物バンディング (FUTURES_BANDING)

| Bloombergティッカーパターン | 説明 | ReportType | TenorType |
|----------------------------|------|------------|-----------|
| LMFBJ[A-E]M[1-3] Index | 先物ロングポジション | Futures Long | Generic 1st-3rd Future |
| LMFBJ[F-J]M[1-3] Index | 先物ショートポジション | Futures Short | Generic 1st-3rd Future |

**バンドマッピング**:
- A/F: 5-9%
- B/G: 10-19%
- C/H: 20-29%
- D/I: 30-39%
- E/J: 40+%

### ワラントバンディング (WARRANT_BANDING)

| Bloombergティッカーパターン | 説明 | ReportType |
|----------------------------|------|------------|
| LMWHCAD[A-E] Index | ワラントポジション | Warrant |
| LMWHCAC[A-E] Index | 現物ポジション | Cash |
| LMWHCAT[A-E] Index | トムネクストポジション | Tom |

**更新頻度**: 週次
**取得フィールド**: PX_LAST

## 企業株価データ (T_CompanyStockPrice)

### 銅関連企業 (COMPANY_STOCKS)

| Bloombergティッカー | 企業名 | 取引所 | 取得フィールド |
|---------------------|--------|--------|----------------|
| GLEN LN Equity | Glencore | London | PX_LAST, PX_OPEN, PX_HIGH, PX_LOW, PX_VOLUME |
| FCX US Equity | Freeport-McMoRan | NYSE | PX_LAST, PX_OPEN, PX_HIGH, PX_LOW, PX_VOLUME |
| SCCO US Equity | Southern Copper | NYSE | PX_LAST, PX_OPEN, PX_HIGH, PX_LOW, PX_VOLUME |

## データ取得・処理ロジック

### フィールド優先順位

在庫データの場合、以下の優先順位でフィールドを取得：
1. PX_LAST（メイン）
2. LAST_PRICE（フォールバック1）
3. CUR_MKT_VALUE（フォールバック2）

### 地域コード変換

LME在庫の地域識別：
- `%AMER Index` → `AMER`
- `%ASIA Index` → `ASIA`
- `%EURO Index` → `EURO`
- `%MEST Index` → 除外（Bloomberg APIで認識されない）
- ` Index`（地域指定なし） → `GLOBAL`

### データ型変換

- 価格データ: DECIMAL(18,4)
- 在庫数量: DECIMAL(18,0)（整数トン単位）
- 出来高・建玉: BIGINT
- 比率データ: DECIMAL(5,2)（%）

### エラーハンドリング

- **無効ティッカー**: ログ記録、処理継続
- **データなし**: 警告ログ、空データとして処理
- **フィールドなし**: フォールバックフィールドチェック
- **型変換エラー**: NULL値として格納