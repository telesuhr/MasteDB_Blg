import pyodbc
from xbbg import blp
from datetime import datetime, timedelta
import pandas as pd

# SQL Serverに接続
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 18 for SQL Server};SERVER=jcz.database.windows.net;DATABASE=JCL;UID=TKJCZ01;PWD=P@ssw0rdmbkazuresql')

cursor = conn.cursor()

# Bloombergからデータを取得して、テーブルに保存する関数


def fetch_and_store_data(ticker, start_date=None, end_date=None):
    # デフォルトで過去半年分のデータを取得
    if not start_date:
        start_date = datetime.now() - timedelta(days=8)  # ない場合は25000日程度で
    if not end_date:
        end_date = datetime.now()

    # Bloombergからデータ取得
    try:
        # 複数のフィールドを取得して指標の属性を設定
        fields = ['PX_LAST', 'ECO_RELEASE_DT']
        data = blp.bdh(tickers=ticker, flds=fields,
                       start_date=start_date, end_date=end_date)
        print(data)  # データの中身を確認

        # フィールド名を小文字に変更して確認
        if data.empty:
            raise ValueError(f"{ticker}のデータが取得できませんでした。")

    except NameError as e:
        print(f"エラー: {e}, 銘柄: {ticker}")
        return
    except ValueError as e:
        print(f"値エラー: {e}, 銘柄: {ticker}")
        return
    except Exception as e:
        print(f"予期しないエラー: {e}, 銘柄: {ticker}")
        return

    # `indicators`テーブルからTickerを取得
    cursor.execute(
        "SELECT Ticker, indicator_name FROM indicators WHERE indicator_name = ?", (ticker,))
    indicator_id = cursor.fetchone()

    # `indicators`テーブルにデータが存在しない場合、新たに追加
    if not indicator_id:
        # 他のフィールドを使用して指標属性を設定
        fields = ['QUOTE_UNITS', 'INDX_FREQ', 'NAME', 'COUNTRY', 'NAME_KANJI']
        meta_data = blp.bdp(tickers=ticker, flds=fields)
        unit = meta_data['quote_units'].iloc[0] if 'quote_units' in meta_data.columns else 'Unknown Unit'
        comparison_type = meta_data['indx_freq'].iloc[0] if 'indx_freq' in meta_data.columns else 'Unknown Frequency'
        display_name = meta_data['name'].iloc[0] if 'name' in meta_data.columns else 'Unknown Name'
        country = meta_data['country'].iloc[0] if 'country' in meta_data.columns else 'Unknown Country'
        japanese_name = meta_data['name_kanji'].iloc[0] if 'name_kanji' in meta_data.columns else 'Unknown Japanese Name'

        cursor.execute("""
            INSERT INTO indicators (indicator_name, category, comparison_type, unit, name, country, japanese_name)
            OUTPUT INSERTED.Ticker
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (ticker, 'Unknown Category', comparison_type, unit, display_name, country, japanese_name))
        indicator_id = cursor.fetchone()[0]
        conn.commit()
        print(f"{ticker}のインジケーターが`indicators`テーブルに追加されました。")
    else:
        indicator_id = indicator_id[0]

    # 取得したデータを日付ごとに処理
    bulk_insert_data = []  # 初期化
    for date, row in data.iterrows():
        # ティッカーとフィールド名を指定して値を取得
        value = row.get((ticker, 'PX_LAST'), None)
        if pd.isna(value):
            continue  # 値がNULLの場合はインサートしない
        value = None if pd.isna(value) else value  # NaNをNULLとして扱う

        # release_dateの取得と変換
        release_date_str = row.get((ticker, 'ECO_RELEASE_DT'), None)
        if not pd.isna(release_date_str) and release_date_str not in [None, '#N/A N/A']:
            try:
                # 小数点を削除し日付形式に変換
                release_date = datetime.strptime(
                    str(int(float(release_date_str))), '%Y%m%d')
            except ValueError as e:
                print(f"release_date の変換に失敗しました: {e}")
                release_date = None
        else:
            release_date = None

        # 重複チェック：既に`economic_indicators`に同じデータが存在しないか確認
        cursor.execute(
            "SELECT * FROM economic_indicators WHERE Ticker = ? AND date = ?", (indicator_id, date))
        existing_entry = cursor.fetchone()
        if existing_entry:
            # 既に存在する場合、データを更新
            cursor.execute("""
                UPDATE economic_indicators
                SET value = ?, release_dt = ?, updated_at = ?
                WHERE Ticker = ? AND date = ?
            """, (value, release_date, datetime.now(), indicator_id, date))
        else:
            # 新しいデータをリストに追加
            bulk_insert_data.append(
                (indicator_id, date, value, release_date, datetime.now()))

    # バルクインサート実行
    if bulk_insert_data:
        cursor.executemany("""
            INSERT INTO economic_indicators (Ticker, date, value, release_dt, updated_at)
            VALUES (?, ?, ?, ?, ?)
        """, bulk_insert_data)
        conn.commit()
        print(f"{ticker}のデータがバルクインサートされました。")


# データベースに既に登録済みの銘柄を取得し、更新
db_tickers = cursor.execute("SELECT indicator_name FROM indicators").fetchall()
for ticker_row in db_tickers:
    ticker = ticker_row[0]
    fetch_and_store_data(ticker)

# 接続を閉じる
cursor.close()
conn.close()
