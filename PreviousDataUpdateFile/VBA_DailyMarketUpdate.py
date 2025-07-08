import pyodbc
from xbbg import blp
from datetime import datetime, timedelta
import pandas as pd

# SQL Serverに接続
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 18 for SQL Server};SERVER=jcz.database.windows.net;DATABASE=JCL;UID=TKJCZ01;PWD=P@ssw0rdmbkazuresql')

cursor = conn.cursor()

# Bloombergからマーケットデータを取得して、テーブルに保存する関数


def fetch_and_store_market_data(ticker, start_date=None, end_date=None):
    # デフォルトで過去30日分の日次データを取得
    if not start_date:
        # 前営業日より前に取得するよう変更(新規の場合は20,000推奨)
        start_date = datetime.now() - timedelta(days=8)
    if not end_date:
        end_date = datetime.now() - timedelta(days=1)  # 前営業日を終了日とする

    # Bloombergからデータ取得
    try:
        fields = ['PX_LAST', 'OPEN', 'HIGH', 'LOW', 'VOLUME']
        data = blp.bdh(tickers=ticker, flds=fields,
                       start_date=start_date, end_date=end_date)
        print(data)  # データの中身を確認

        if data.empty:
            raise ValueError(f"{ticker}のデータが取得できませんでした。")

        # MultiIndexをフラット化する
        data.columns = ['_'.join(col).strip() if isinstance(
            col, tuple) else col for col in data.columns]
        data.reset_index(inplace=True)
        # インデックス名を'index'から'Date'に変更
        data.rename(columns={'index': 'Date'}, inplace=True)
        print(data.head())  # データの構造を確認

    except NameError as e:
        print(f"エラー: {e}")
        return
    except ValueError as e:
        print(e)
        return

    # `market_indicators`テーブルからTickerを取得
    cursor.execute(
        "SELECT Ticker, market_name, name, name_kanji FROM market_indicators WHERE market_name = ?", (ticker,))
    market_info = cursor.fetchone()

    # `market_indicators`テーブルにデータが存在しない場合、新たに追加
    if not market_info:
        # BDP関数を使用してnameとname_kanjiを取得
        name = blp.bdp(ticker, 'NAME')
        name_kanji = blp.bdp(ticker, 'NAME_KANJI')

        name_value = name.values[0][0] if len(name.values) > 0 else 'Unknown'
        name_kanji_value = name_kanji.values[0][0] if len(
            name.values) > 0 else '未定義'

        # market_indicatorsに新しいティッカーを追加
        cursor.execute("""
            INSERT INTO market_indicators (market_name, category, name, name_kanji)
            VALUES (?, ?, ?, ?)
        """, (ticker, 'Unknown Category', name_value, name_kanji_value))
        conn.commit()

        # 再度market_indicatorsテーブルからデータを取得する
        cursor.execute(
            "SELECT Ticker, market_name, name, name_kanji FROM market_indicators WHERE market_name = ?", (ticker,))
        market_info = cursor.fetchone()
        print(f"{ticker}のマーケットインジケーターが`market_indicators`テーブルに追加されました。")

    market_id = market_info[0]

    # 取得したデータをまとめてバルクインサート用に準備
    bulk_insert_data = []
    for index, row in data.iterrows():
        try:
            date = row['Date']  # 'Date'列を使用
            px_last = None if pd.isna(
                row.get(f'{ticker}_PX_LAST')) else row.get(f'{ticker}_PX_LAST')
            open_price = None if pd.isna(
                row.get(f'{ticker}_OPEN')) else row.get(f'{ticker}_OPEN')
            high = None if pd.isna(
                row.get(f'{ticker}_HIGH')) else row.get(f'{ticker}_HIGH')
            low = None if pd.isna(
                row.get(f'{ticker}_LOW')) else row.get(f'{ticker}_LOW')
            volume = None if pd.isna(
                row.get(f'{ticker}_VOLUME')) else row.get(f'{ticker}_VOLUME')

            # 重複チェック：既に`market_data`に同じ日付のデータが存在する場合は削除してから追加
            cursor.execute(
                "DELETE FROM market_data WHERE Ticker = ? AND date = ?", (market_id, date))

            # 新しいデータをバルクインサート用に追加
            bulk_insert_data.append(
                (market_id, date, px_last, open_price, high, low, volume, datetime.now()))
        except KeyError as e:
            print(f"データの列名に誤りがあります: {e}")
            continue
        except ValueError as e:
            print(f"データ型の変換に失敗しました: {e}")
            continue

    # バルクインサート実行
    if bulk_insert_data:
        cursor.executemany("""
            INSERT INTO market_data (Ticker, date, px_last, open_price, high, low, volume, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, bulk_insert_data)
        conn.commit()
        print(f"{ticker}のデータがバルクインサートされました。")


# データベースに既に登録済みのマーケット銘柄を取得し、更新
cursor.execute("SELECT market_name FROM market_indicators")
manual_tickers = [row[0] for row in cursor.fetchall()]

for ticker in manual_tickers:
    fetch_and_store_market_data(ticker)

# 接続を閉じる
cursor.close()
conn.close()
