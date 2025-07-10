# MasteDB_Blg 可視化システム

Bloomberg APIから取得したLME銅市場データの包括的な可視化・分析システムです。

## 📁 フォルダ構成

```
visualization/
├── config/              # 設定ファイル
│   ├── __init__.py
│   ├── database_config.py    # データベース接続設定
│   └── data_utils.py         # データ取得ユーティリティ
├── notebooks/           # Jupyterノートブック
│   ├── 01_copper_price_analysis.ipynb      # 銅価格時系列分析
│   ├── 02_inventory_analysis.ipynb         # 在庫データ分析
│   ├── 03_tenor_spread_analysis.ipynb     # テナースプレッド分析
│   └── 04_comprehensive_dashboard.ipynb   # 総合ダッシュボード
├── outputs/             # 分析結果出力フォルダ
├── requirements.txt     # Python依存関係
└── README.md           # このファイル
```

## 🚀 セットアップ

### 1. 依存関係のインストール

```bash
# 可視化システム専用の仮想環境作成（推奨）
python -m venv venv_viz
source venv_viz/bin/activate  # Linux/Mac
# または
venv_viz\Scripts\activate     # Windows

# 依存関係インストール
pip install -r requirements.txt
```

### 2. データベース設定

`config/database_config.py`でAzure SQL Server接続設定を確認・修正してください：

```python
DATABASE_CONFIG = {
    'driver': '{ODBC Driver 17 for SQL Server}',
    'server': 'your-server.database.windows.net',
    'database': 'your-database',
    'uid': 'your-username',
    'pwd': 'your-password',
    # ...
}
```

### 3. Jupyter Labの起動

```bash
cd visualization
jupyter lab
```

## 📊 ノートブック概要

### 01_copper_price_analysis.ipynb
**銅価格時系列分析**
- LME、SHFE、CMXの銅価格推移分析
- 価格相関分析
- ボラティリティ分析
- 移動平均線分析
- 限月別価格比較

**主要な可視化:**
- 価格推移チャート（静的・動的）
- 相関マトリックス
- ボラティリティ分布
- 移動平均線からの乖離分析

### 02_inventory_analysis.ipynb
**在庫データ分析**
- LME地域別在庫推移
- 取引所間在庫比較
- 在庫フロー分析（入庫・出庫）
- 在庫と価格の関係性分析

**主要な可視化:**
- 地域別在庫推移・構成比
- 在庫フロー分析
- 在庫・価格相関分析
- 地域別在庫分布

### 03_tenor_spread_analysis.ipynb
**テナースプレッド分析**（プロジェクトの中核）
- Cash/3M スプレッド詳細分析
- 連続限月間スプレッド分析
- コンタンゴ・バックワーデーション判定
- スプレッド変動要因分析
- 季節性・周期性分析
- 簡易予測モデル

**主要な可視化:**
- スプレッド推移・分布・Z-Score分析
- スプレッド相関分析
- 在庫・スプレッド関係性分析
- 季節性分析（月別・四半期別）
- 予測モデル結果

### 04_comprehensive_dashboard.ipynb
**総合ダッシュボード**
- 市場概況サマリー
- 価格・スプレッド・在庫の統合ビュー
- 地域別・取引所別比較
- リスク指標・異常値検知
- 総合評価・推奨アクション
- 自動レポート生成

**主要な機能:**
- 9パネル統合ダッシュボード
- リアルタイム市場健全性スコア
- 自動アラート・異常値検知
- エクスポート機能（レポート・CSV）

## 📈 主要な分析機能

### データ取得・処理
- `DataFetcher`クラスによる統一的なデータ取得
- 自動データクリーニング・前処理
- 欠損値補完・外れ値処理

### 統計分析
- 相関分析・回帰分析
- ボラティリティ・リスク指標計算
- Z-Score・パーセンタイル分析
- 移動統計（移動平均・移動標準偏差）

### 時系列分析
- トレンド・季節性分析
- 自己相関・周期性検出
- ドローダウン分析
- 変動要因分析

### 可視化
- **静的グラフ**: matplotlib/seaborn
- **動的グラフ**: Plotly（ズーム・パン・ホバー対応）
- **ダッシュボード**: 複数パネル統合ビュー
- **ヒートマップ**: 相関・分布分析

## 🚨 アラート・監視機能

### 自動異常値検知
- 価格・スプレッド・在庫の3σ異常値
- 急激な変動率（>10%）の検出
- ボラティリティ異常（>40%年率）

### リスク指標
- VaR（Value at Risk）計算
- 最大ドローダウン
- シャープレシオ
- 連続上昇・下落日数

### 市場構造監視
- コンタンゴ・バックワーデーション判定
- スプレッドの歴史的パーセンタイル
- 在庫水準の適正性評価

## 📊 出力ファイル

分析結果は`outputs/`フォルダに自動保存されます：

- **分析レポート**: `copper_market_report_YYYYMMDD.txt`
- **ダッシュボードデータ**: `copper_dashboard_data_YYYYMMDD.csv`
- **統計サマリー**: `market_summary_YYYYMMDD.csv`

## 🔧 カスタマイズ

### 新しい分析指標の追加
1. `config/data_utils.py`にデータ取得メソッドを追加
2. 各ノートブックに分析ロジックを実装
3. 可視化コードを追加

### 新しいアラート条件
`04_comprehensive_dashboard.ipynb`の異常値検知セクションを修正

### データソースの拡張
`config/database_config.py`でテーブル定義を追加し、`data_utils.py`で取得メソッドを実装

## 🎯 使用方法

### 日次分析ワークフロー
1. **01_copper_price_analysis.ipynb**: 価格動向確認
2. **02_inventory_analysis.ipynb**: 在庫状況確認
3. **03_tenor_spread_analysis.ipynb**: スプレッド分析
4. **04_comprehensive_dashboard.ipynb**: 総合評価

### 定期レビュー
- **週次**: 全ノートブック実行・トレンド確認
- **月次**: 季節性分析・モデル精度検証
- **四半期**: パラメータ調整・新機能追加

## 📞 サポート

分析結果の解釈や技術的な質問については、プロジェクトチームまでお問い合わせください。

---

*Last updated: 2025-07-04*