import pandas as pd
from datetime import datetime, timedelta
from xbbg import blp
import pyodbc

# SQL Serverに接続
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 18 for SQL Server};SERVER=jcz.database.windows.net;DATABASE=JCL;UID=TKJCZ01;PWD=P@ssw0rdmbkazuresql')

cursor = conn.cursor()


# 営業日カレンダー（簡易版として土日を非営業日とする）
def is_business_day(date):
    return np.is_busday(date.strftime('%Y-%m-%d'), holidays=["2024-01-01", "2024-01-02", "2024-02-11"])


def get_previous_business_day(start_date, num_days):
    business_days = pd.bdate_range(end=start_date, periods=num_days)
    return business_days[0]

# Bloombergからデータを取得して、テーブルに保存する関数


def fetch_and_store_futures_data(ticker, start_date=None, end_date=None):
    # デフォルトで過去半年分のデータを取得（営業日ベースに変更）
    if not start_date:
        start_date = get_previous_business_day(
            datetime.now(), 8)  # 初回10,000
    if not end_date:
        end_date = get_previous_business_day(datetime.now(), 1)  # 前営業日を終了日とする

    # 日付を直接指定(必要な時のもコメントアウトを解除して利用)
    # start_date = datetime(2024, 12, 24)
    # end_date = datetime(2024, 12, 24)

    # Bloombergからデータ取得
    try:
        # 複数のフィールドを取得してデータを設定
        fields = ['PX_SETTLE', 'PX_VOLUME', 'OPEN_INT', 'FUT_CUR_GEN_TICKER']
        data = blp.bdh(tickers=ticker, flds=fields,
                       start_date=start_date, end_date=end_date)
        print(data)  # データの中身を確認

        # フィールド名を小文字に変更して確認
        if data.empty:
            raise ValueError(f"{ticker}のデータが取得できませんでした。")

    except NameError as e:
        print(f"エラー: {e}, ティッカー: {ticker}")
        return
    except ValueError as e:
        print(f"値エラー: {e}, ティッカー: {ticker}")
        return
    except Exception as e:
        print(f"予期しないエラー: {e}, ティッカー: {ticker}")
        return

    # 取得したデータを日付ごとに処理
    bulk_insert_data = []  # 初期化
    for date, row in data.iterrows():
        # 値を取得
        closing_price = row.get((ticker, 'PX_SETTLE'), None)
        volume = row.get((ticker, 'PX_VOLUME'), None)
        open_interest = row.get((ticker, 'OPEN_INT'), None)
        fut_ticker = row.get((ticker, 'FUT_CUR_GEN_TICKER'), None)

        # NoneまたはNaNの値はそのままデータベースにNULLとして扱う
        closing_price = None if pd.isna(
            closing_price) else float(closing_price)
        volume = None if pd.isna(volume) else float(volume)
        open_interest = None if pd.isna(
            open_interest) else float(open_interest)

        if closing_price is None and volume is None and open_interest is None:
            continue  # 値がすべてNULLの場合はスキップ

        # `futures_contract_attributes` テーブルを更新（必要に応じて）
        if fut_ticker:
            # `fut_ticker` が属性テーブルに存在しない場合、新たに追加
            cursor.execute(
                "SELECT id FROM futures_contract_attributes WHERE ticker = ?", (fut_ticker,))
            if not cursor.fetchone():
                fields = ['FUT_DLV_DT_LAST', 'LAST_TRADEABLE_DT',
                          'FUT_CONTRACT_DT', 'FUT_CONT_SIZE', 'EXCH_CODE', 'FUTURES_CATEGORY', 'CRNCY', 'NAME']
                meta_data = blp.bdp(tickers=fut_ticker +
                                    " Comdty", flds=fields)
                first_delivery_date = meta_data['fut_dlv_dt_last'].iloc[
                    0] if 'fut_dlv_dt_last' in meta_data.columns else None
                last_trade_date = meta_data['last_tradeable_dt'].iloc[0] if 'last_tradeable_dt' in meta_data.columns else None
                contract_month = meta_data['fut_contract_dt'].iloc[0] if 'fut_contract_dt' in meta_data.columns else None
                contract_size = float(meta_data['fut_cont_size'].iloc[0]) if 'fut_cont_size' in meta_data.columns and pd.notna(
                    meta_data['fut_cont_size'].iloc[0]) else None
                exchange = meta_data['exch_code'].iloc[0] if 'exch_code' in meta_data.columns else None
                product_category = meta_data['futures_category'].iloc[0] if 'futures_category' in meta_data.columns else None
                currency = meta_data['crncy'].iloc[0] if 'crncy' in meta_data.columns else None
                underlying = meta_data['name'].iloc[0] if 'name' in meta_data.columns else None

                cursor.execute("""
                    INSERT INTO futures_contract_attributes (ticker, last_trade_date, expiry_date, contract_month, contract_size, exchange, product_category, currency, underlying_contract)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (fut_ticker, last_trade_date, first_delivery_date, contract_month, contract_size, exchange, product_category, currency, underlying))
                conn.commit()
                print(
                    f"{fut_ticker}のコントラクト属性が`futures_contract_attributes`テーブルに追加されました。")

        # 不正なデータが含まれていないかを確認し、適切な値のみを追加
        if closing_price is not None or volume is not None or open_interest is not None:  # 少なくとも1つの値がNULLでない場合のみ追加
            # 重複チェック：既に`futures_daily_data`に同じティッカーと日付のデータが存在する場合、古いデータを削除
            cursor.execute(
                "SELECT 1 FROM futures_daily_data WHERE ticker = ? AND date = ?", (ticker, date))
            if cursor.fetchone():
                cursor.execute(
                    "DELETE FROM futures_daily_data WHERE ticker = ? AND date = ?", (ticker, date))
                conn.commit()
            bulk_insert_data.append(
                (ticker, fut_ticker, date, closing_price, volume, open_interest, datetime.now(),))

    # バルクインサート実行
    if bulk_insert_data:
        cursor.fast_executemany = True  # バルクインサートの高速化
        try:
            cursor.executemany("""
                INSERT INTO futures_daily_data (ticker, underlying_contract, date, closing_price, volume, open_interest, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, bulk_insert_data)
            conn.commit()
            print(f"{ticker}のデータがバルクインサートされました。")
        except pyodbc.ProgrammingError as e:
            print(f"バルクインサート中にエラーが発生しました: {e}")


# LME Future
# 各商品のコード（CA:LP, NI:LN, SN:LT, ZS:LX, PB:LL, AH:LA）
commodity_codes = ['LP', 'LN', 'LT', 'LX', 'LL', 'LA']
for code in commodity_codes:
    for i in range(1, 37):
        ticker = f'{code}{i} Comdty'
        fetch_and_store_futures_data(ticker)

# SHFE Future
# 各商品のコード（CA:CU, NI:LN, SN:LT, ZS:LX, PB:LL, AH:LA）
commodity_codes = ['CU']
for code in commodity_codes:
    for i in range(1, 13):
        ticker = f'{code}{i} Comdty'
        fetch_and_store_futures_data(ticker)

# Comex
# 各商品のコード（CA:CU, NI:LN, SN:LT, ZS:LX, PB:LL, AH:LA）
commodity_codes = ['HG']
for code in commodity_codes:
    for i in range(1, 27):
        ticker = f'{code}{i} Comdty'
        fetch_and_store_futures_data(ticker)

# 接続を閉じる
cursor.close()
conn.close()
