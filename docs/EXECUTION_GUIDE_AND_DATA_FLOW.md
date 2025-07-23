# Bloomberg Data Ingestion System - 実行ガイドとデータフロー詳細

## 1. 実行方法

### 初回実行（過去データの一括取得）
```bash
# Pythonスクリプト直接実行
python src/main.py --mode initial

# Windows用バッチファイル
run_initial.bat

# Linux/Mac用シェルスクリプト
./run_initial.sh
```

### 日次更新実行
```bash
# 基本的な日次更新
python src/main.py --mode daily

# 市場タイミングを考慮した高度な日次更新
python scripts/daily_operations/run_daily.py

# Windows用バッチファイル
run_daily.bat
```

## 2. データ取得範囲の決定ロジック

### 初回実行（--mode initial）
過去の一定期間のデータを一括取得。期間は`config/bloomberg_config.py`の`INITIAL_LOAD_PERIODS`で定義：

```python
INITIAL_LOAD_PERIODS = {
    'prices': 5,      # 価格データ: 5年
    'inventory': 5,   # 在庫データ: 5年
    'indicators': 5,  # 指標データ: 5年
    'macro': 10,      # マクロ経済: 10年
    'cotr': 5,        # COTRレポート: 5年
    'banding': 3,     # バンディング: 3年
    'stocks': 5       # 株価: 5年
}
```

### 日次更新（--mode daily）
- **基本**: 過去3日分のデータを取得（週末・休日対応）
- **週次データ（COTR）**: 金曜日のみ実行
- **月次データ（マクロ指標）**: 月初1週間のみ実行

### 市場タイミング考慮（enhanced_daily_update.py使用時）
```
LME: ロンドン時間17:00セトルメント後1時間待機
SHFE: 上海時間15:00セトルメント後2時間待機
COMEX: NY時間13:30セトルメント後1時間待機
```

## 3. 更新されるテーブル一覧（カテゴリ別）

### 3.1 商品価格関連
| カテゴリ | 更新テーブル | 含まれるデータ |
|----------|--------------|----------------|
| LME_COPPER_PRICES | T_CommodityPrice / T_CommodityPrice_V2 | LME銅価格（現物、TomNext、3M先物、スプレッド、LP1-LP36） |
| SHFE_COPPER_PRICES | T_CommodityPrice / T_CommodityPrice_V2 | 上海銅価格（CU1-CU12） |
| CMX_COPPER_PRICES | T_CommodityPrice / T_CommodityPrice_V2 | COMEX銅価格（HG1-HG26） |

### 3.2 在庫データ
| カテゴリ | 更新テーブル | 含まれるデータ |
|----------|--------------|----------------|
| LME_INVENTORY | T_LMEInventory | LME地域別在庫（Global、Asia、Euro、Amer）<br>- TotalStock: 総在庫<br>- OnWarrant: ワラント在庫<br>- CancelledWarrant: キャンセルワラント<br>- Inflow/Outflow: 流入出量 |
| SHFE_INVENTORY | T_OtherExchangeInventory | 上海先物取引所在庫 |
| CMX_INVENTORY | T_OtherExchangeInventory | COMEX在庫 |

### 3.3 市場指標
| カテゴリ | 更新テーブル | 含まれるデータ |
|----------|--------------|----------------|
| INTEREST_RATES | T_MarketIndicator | SOFR、TSFR1M、TSFR3M、US0001M、US0003M |
| FX_RATES | T_MarketIndicator | USD/JPY、EUR/USD、USD/CNY、USD/CLP、USD/PEN |
| COMMODITY_INDICES | T_MarketIndicator | BCOM指数、SPGSCI指数 |
| EQUITY_INDICES | T_MarketIndicator | S&P500、日経225、上海総合 |
| ENERGY_PRICES | T_MarketIndicator | WTI原油、Brent原油、天然ガス |
| PHYSICAL_PREMIUMS | T_MarketIndicator | 洋山銅プレミアム（CECN0001/0002） |
| OTHER_INDICATORS | T_MarketIndicator | バルチック海運指数（BDIY） |

### 3.4 マクロ経済指標
| カテゴリ | 更新テーブル | 含まれるデータ |
|----------|--------------|----------------|
| MACRO_INDICATORS | T_MacroEconomicIndicator | 米国: PMI、GDP、工業生産、CPI<br>中国: PMI、GDP、工業生産、CPI<br>EU: PMI |

### 3.5 ポジションレポート
| カテゴリ | 更新テーブル | 含まれるデータ | 更新頻度 |
|----------|--------------|----------------|----------|
| COTR_DATA | T_COTR | LME建玉報告<br>- Investment Funds<br>- Commercial Undertakings | 週次（金曜日） |
| FUTURES_BANDING | T_BandingReport | 先物ロング/ショートポジション分布 | 日次 |
| WARRANT_BANDING | T_BandingReport | ワラントポジション分布 | 日次 |
| CASH_BANDING | T_BandingReport | 現物ポジション分布 | 日次 |

### 3.6 その他
| カテゴリ | 更新テーブル | 含まれるデータ |
|----------|--------------|----------------|
| COMPANY_STOCKS | T_CompanyStockPrice | 鉱山会社株価<br>- Glencore (GLEN LN)<br>- Rio Tinto (RIO LN)<br>- BHP (BHP AU)<br>- Freeport (FCX US)<br>- Jiangxi Copper (2600 HK) |

## 4. マスターテーブルの自動更新

実行時に新しいエンティティが見つかると自動的に作成されるマスターテーブル：

### 4.1 基本マスター
- **M_Metal**: 新しい金属商品（デフォルト: CurrencyCode='USD'）
- **M_TenorType**: 新しい限月タイプ
- **M_Indicator**: 新しい市場指標（カテゴリ、単位、頻度を自動設定）
- **M_Region**: 新しい地域コード
- **M_COTRCategory**: 新しいCOTRカテゴリ
- **M_HoldingBand**: 新しい保有比率バンド

### 4.2 先物関連マスター
- **M_GenericFutures**: 新しいジェネリック先物（LP1、CU1等）
- **M_ActualContract**: 新しい実契約（LPN25、CUH25等）

### 4.3 マッピングテーブル
- **T_GenericContractMapping**: ジェネリック先物と実契約の日次マッピング

## 5. 主要ビューとその用途

### 5.1 メインビュー
#### V_CommodityPriceWithMaturityEx
最も重要なビュー。価格データと満期情報を統合し、動的な契約マッピングを提供。

**主な機能**:
- 取引日ベースの動的契約マッピング
- 満期までの日数計算（カレンダー日数と営業日数）
- ロールオーバー推奨フラグ
- 価格、出来高、建玉データ
- LME、CMX、SHFE全取引所対応

### 5.2 分析用ビュー
| ビュー名 | 用途 | 主要機能 |
|----------|------|----------|
| V_CommodityPriceEnhanced | 高度な価格分析 | 前日比較、変動率、ボラティリティ、取引活動分類 |
| V_CommodityPriceWithChange | 価格変動追跡 | 前日比価格変動額・率 |
| V_CommodityPriceWithAttributes | 基本属性付き価格 | 満期日数、データ品質フラグ、契約月情報 |
| V_CommodityPriceSimple | シンプル価格ビュー | 基本価格データ、取引所表示名、データ品質指標 |

### 5.3 ロールオーバー管理ビュー
| ビュー名 | 用途 | 主要機能 |
|----------|------|----------|
| V_RolloverStatus | 現在のロールオーバー状況 | 全ジェネリック先物の現在マッピング状況 |
| V_RolloverAlertsWithTradingDays | ロールオーバー警告 | 20営業日以内の契約を警告（IMMEDIATE/URGENT/SOON/OK） |

### 5.4 サマリービュー
| ビュー名 | 用途 | 主要機能 |
|----------|------|----------|
| V_MaturitySummaryWithTradingDays | 満期統計 | 取引所別の平均/最小/最大満期日数 |
| V_TradingDaysCalculationDetail | 営業日計算詳細 | カレンダー日数vs営業日数の比較 |

### 5.5 マッピングビュー
| ビュー名 | 用途 | 主要機能 |
|----------|------|----------|
| V_CommodityPriceWithMapping | 契約マッピング表示 | ジェネリック先物の実契約マッピング情報 |
| V_CommodityPriceWithTradingDays | 営業日ベース価格 | 営業日数での満期計算、ロールオーバー判定 |

## 6. データ処理フロー

### 6.1 基本フロー
```
1. Bloomberg API接続
   ↓
2. バッチリクエスト（最大100銘柄/リクエスト）
   ↓
3. DataProcessorでデータ変換
   ↓
4. マスターデータ解決（自動作成）
   ↓
5. UPSERT処理（MERGE文使用）
   ↓
6. ビュー自動更新
```

### 6.2 データ品質チェック
- 重複データ防止（ユニーク制約）
- NULL値の適切な処理
- 10%以上の価格変動で警告
- データ完全性チェック

### 6.3 エラーハンドリング
- APIレベル: タイムアウト処理、セッション監視
- DBレベル: リトライ機構、トランザクション管理
- ログレベル: 詳細なエラーログ記録

## 7. 実行時の注意事項

### 7.1 前提条件
- Bloomberg Terminal実行中（BBComm起動）
- SQL Server接続可能
- 必要なPythonパッケージインストール済み

### 7.2 実行タイミング
- 初回実行: 週末や市場クローズ後推奨（大量データ取得のため）
- 日次更新: 各市場のセトルメント後実行
- 週次更新: 金曜日の市場クローズ後

### 7.3 パフォーマンス
- 初回実行: 数時間（データ量による）
- 日次更新: 10-30分程度
- バッチサイズ: 100銘柄（Bloomberg API制限）

## 8. トラブルシューティング

### 8.1 よくある問題
1. **Bloomberg接続エラー**: Terminal起動確認、ポート8194確認
2. **SQL Server接続エラー**: 接続文字列確認、権限確認
3. **データ重複エラー**: ユニーク制約確認、UPSERT処理確認

### 8.2 ログファイル
- 場所: `logs/bloomberg_ingestion_YYYYMMDD.log`
- エラー専用: `logs/errors.log`

### 8.3 データ検証
- 価格データ: V_CommodityPriceSimpleで確認
- 在庫データ: T_LMEInventoryで地域別確認
- マッピング: V_RolloverStatusで現在の状態確認