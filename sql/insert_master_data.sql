-- ###########################################################
-- マスタデータ初期値挿入スクリプト
-- ###########################################################

USE [JCL];
GO

-- M_Metal (金属銘柄マスタ)
INSERT INTO M_Metal (MetalCode, MetalName, CurrencyCode, ExchangeCode, Description) VALUES
('COPPER', 'Copper', 'USD', 'LME', 'London Metal Exchange Copper'),
('CU_SHFE', 'Copper SHFE', 'CNY', 'SHFE', 'Shanghai Futures Exchange Copper'),
('CU_CMX', 'Copper CMX', 'USD', 'CMX', 'COMEX Copper');

-- M_TenorType (限月タイプマスタ)
INSERT INTO M_TenorType (TenorTypeName, Description) VALUES
('Cash', 'Spot/Cash settlement'),
('Tom-Next', 'Tomorrow Next'),
('3M Futures', '3 Month Futures'),
('Cash/3M Spread', 'Cash vs 3 Month Spread'),
('Generic 1st Future', 'Generic 1st month future'),
('Generic 2nd Future', 'Generic 2nd month future'),
('Generic 3rd Future', 'Generic 3rd month future'),
('Generic 4th Future', 'Generic 4th month future'),
('Generic 5th Future', 'Generic 5th month future'),
('Generic 6th Future', 'Generic 6th month future'),
('Generic 7th Future', 'Generic 7th month future'),
('Generic 8th Future', 'Generic 8th month future'),
('Generic 9th Future', 'Generic 9th month future'),
('Generic 10th Future', 'Generic 10th month future'),
('Generic 11th Future', 'Generic 11th month future'),
('Generic 12th Future', 'Generic 12th month future');

-- M_Region (地域マスタ)
INSERT INTO M_Region (RegionCode, RegionName, Description) VALUES
('GLOBAL', 'Global Total', 'Worldwide total'),
('ASIA', 'Asia', 'Asian region total'),
('EURO', 'Europe', 'European region total'),
('AMER', 'Americas', 'Americas region total'),
('MEST', 'Middle East', 'Middle East region total');

-- M_COTRCategory (COTRカテゴリーマスタ)
INSERT INTO M_COTRCategory (CategoryName, Description) VALUES
('Investment Funds', 'Money manager and other investment funds'),
('Commercial Undertakings', 'Producer/Merchant/Processor/User and commercial entities');

-- M_HoldingBand (保有比率バンドマスタ)
INSERT INTO M_HoldingBand (BandRange, MinValue, MaxValue, Description) VALUES
('5-9%', 5.0, 9.0, '5% to 9% holding band'),
('10-19%', 10.0, 19.0, '10% to 19% holding band'),
('20-29%', 20.0, 29.0, '20% to 29% holding band'),
('30-39%', 30.0, 39.0, '30% to 39% holding band'),
('40-49%', 40.0, 49.0, '40% to 49% holding band'),
('40+%', 40.0, 100.0, '40% and above holding band'),
('50-79%', 50.0, 79.0, '50% to 79% holding band'),
('80-89%', 80.0, 89.0, '80% to 89% holding band'),
('90+%', 90.0, 100.0, '90% and above holding band');

-- M_Indicator (指標マスタ) - 主要な指標を事前登録
INSERT INTO M_Indicator (IndicatorCode, IndicatorName, Category, Unit, Freq, Description) VALUES
-- 金利
('SOFRRATE', 'Secured Overnight Financing Rate', 'Interest Rate', '%', 'Daily', 'US SOFR rate'),
('TSFR1M', 'CME Term SOFR 1 Month', 'Interest Rate', '%', 'Daily', '1 month term SOFR'),
('TSFR3M', 'CME Term SOFR 3 Month', 'Interest Rate', '%', 'Daily', '3 month term SOFR'),
('US0001M', 'USD 1 Month LIBOR', 'Interest Rate', '%', 'Daily', '1 month USD LIBOR'),
('US0003M', 'USD 3 Month LIBOR', 'Interest Rate', '%', 'Daily', '3 month USD LIBOR'),
-- 為替
('USDJPY', 'USD/JPY Spot Rate', 'FX', 'Currency', 'Daily', 'US Dollar vs Japanese Yen'),
('EURUSD', 'EUR/USD Spot Rate', 'FX', 'Currency', 'Daily', 'Euro vs US Dollar'),
('USDCNY', 'USD/CNY Spot Rate', 'FX', 'Currency', 'Daily', 'US Dollar vs Chinese Yuan'),
('USDCLP', 'USD/CLP Spot Rate', 'FX', 'Currency', 'Daily', 'US Dollar vs Chilean Peso'),
('USDPEN', 'USD/PEN Spot Rate', 'FX', 'Currency', 'Daily', 'US Dollar vs Peruvian Sol'),
-- コモディティ指数
('BCOM', 'Bloomberg Commodity Index', 'Commodity Index', 'Index Points', 'Daily', 'Bloomberg commodity index'),
('SPGSCI', 'S&P GSCI Index', 'Commodity Index', 'Index Points', 'Daily', 'S&P Goldman Sachs Commodity Index'),
-- 株価指数
('SPX', 'S&P 500 Index', 'Equity Index', 'Index Points', 'Daily', 'S&P 500 stock index'),
('NKY', 'Nikkei 225', 'Equity Index', 'Index Points', 'Daily', 'Nikkei 225 stock index'),
('SHCOMP', 'Shanghai Composite', 'Equity Index', 'Index Points', 'Daily', 'Shanghai Composite Index'),
-- エネルギー
('CP1', 'WTI Crude Oil', 'Energy', 'USD/Barrel', 'Daily', 'WTI crude oil futures'),
('CO1', 'Brent Crude Oil', 'Energy', 'USD/Barrel', 'Daily', 'Brent crude oil futures'),
('NG1', 'Natural Gas', 'Energy', 'USD/MMBtu', 'Daily', 'Natural gas futures'),
-- その他
('BDIY', 'Baltic Dry Index', 'Shipping', 'Index Points', 'Daily', 'Baltic Dry shipping index'),
('CECN0001', 'China Yangshan Copper Premium', 'Physical Premium', 'USD/tonne', 'Daily', 'Yangshan copper cathode premium'),
('CECN0002', 'China Yangshan Copper Premium', 'Physical Premium', 'USD/tonne', 'Daily', 'Yangshan copper cathode premium'),
-- マクロ経済指標
('NAPMPMI', 'US ISM Manufacturing PMI', 'Macro Economic', 'Index', 'Monthly', 'ISM Manufacturing PMI'),
('CPMINDX', 'China Manufacturing PMI', 'Macro Economic', 'Index', 'Monthly', 'China Manufacturing PMI'),
('MPMIEUMA', 'EU Manufacturing PMI', 'Macro Economic', 'Index', 'Monthly', 'EU Manufacturing PMI'),
('EHGDUSY', 'US Real GDP', 'Macro Economic', 'YoY %', 'Yearly', 'US Real GDP growth'),
('EHGDCNY', 'China Real GDP', 'Macro Economic', 'YoY %', 'Yearly', 'China Real GDP growth'),
('EHIUUSY', 'US Industrial Production', 'Macro Economic', 'YoY %', 'Yearly', 'US Industrial Production'),
('EHIUCNY', 'China Industrial Production', 'Macro Economic', 'YoY %', 'Yearly', 'China Industrial Production'),
('EHPIUSY', 'US Consumer Price Index', 'Macro Economic', 'YoY %', 'Yearly', 'US CPI'),
('EHPICNY', 'China Consumer Price Index', 'Macro Economic', 'YoY %', 'Yearly', 'China CPI');

PRINT 'Master data initialization completed successfully.';
GO