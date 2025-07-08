"""
LME Cash価格、3Mアウトライト、Cash/3Mスプレッドデータの追加
"""
import sys
import os
from datetime import datetime, timedelta
import pandas as pd

# プロジェクトルートとsrcディレクトリをPythonパスに追加
project_root = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from bloomberg_api import BloombergDataFetcher
from database import DatabaseManager
from config.logging_config import logger
import pyodbc

def check_existing_data():
    """既存データの確認"""
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=jcz.database.windows.net;"
        "DATABASE=JCL;"
        "UID=TKJCZ01;"
        "PWD=P@ssw0rdmbkazuresql;"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    # GenericTickerの確認
    cursor.execute("""
        SELECT GenericID, GenericTicker, Description
        FROM M_GenericFutures
        WHERE GenericTicker IN ('LMCADY Comdty', 'LMCADS03 Comdty', 'LMCADS 0003 Comdty')
        ORDER BY GenericTicker
    """)
    
    print("\n【既存のLME特殊ティッカー】")
    for row in cursor.fetchall():
        print(f"ID: {row[0]}, Ticker: {row[1]}, Description: {row[2]}")
    
    conn.close()

def add_lme_special_tickers():
    """LME特殊ティッカーをM_GenericFuturesに追加"""
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=jcz.database.windows.net;"
        "DATABASE=JCL;"
        "UID=TKJCZ01;"
        "PWD=P@ssw0rdmbkazuresql;"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    try:
        # MetalIDを取得
        cursor.execute("SELECT MetalID FROM M_Metal WHERE MetalCode = 'COPPER'")
        metal_id = cursor.fetchone()[0]
        
        # 追加するティッカー
        special_tickers = [
            ('LMCADY Comdty', 'LME Copper Cash', 0),
            ('LMCADS03 Comdty', 'LME Copper 3M Outright', 3),
            ('LMCADS 0003 Comdty', 'LME Copper Cash/3M Spread', -1)
        ]
        
        for ticker, description, generic_num in special_tickers:
            # 既存チェック
            cursor.execute("""
                SELECT GenericID FROM M_GenericFutures 
                WHERE GenericTicker = ?
            """, ticker)
            
            if cursor.fetchone() is None:
                # 新規追加
                cursor.execute("""
                    INSERT INTO M_GenericFutures (
                        GenericTicker, MetalID, ExchangeCode, GenericNumber, 
                        Description, IsActive, CreatedDate
                    ) VALUES (?, ?, ?, ?, ?, 1, GETDATE())
                """, ticker, metal_id, 'LME', generic_num, description)
                
                logger.info(f"追加: {ticker} - {description}")
            else:
                logger.info(f"既存: {ticker}")
        
        conn.commit()
        logger.info("M_GenericFutures更新完了")
        
    except Exception as e:
        logger.error(f"エラー: {e}")
        conn.rollback()
        
    finally:
        conn.close()

def load_lme_special_data(days_back=3):
    """LME特殊データのロード"""
    logger.info("=== LME Cash/3M/Spreadデータロード開始 ===")
    
    bloomberg = BloombergDataFetcher()
    db_manager = DatabaseManager()
    
    try:
        # Bloomberg API接続
        if not bloomberg.connect():
            logger.error("Bloomberg API接続失敗")
            return
        
        # 対象ティッカー（正しいBloombergフォーマット）
        tickers = {
            'LMCADY Index': 'LME Copper Cash',          # 現物価格
            'LMCADS03 Comdty': 'LME Copper 3M Outright', # 3Mアウトライト
            'LMCADS 0003 Comdty': 'LME Copper Cash/3M Spread'  # Cash/3Mスプレッド
        }
        
        # 日付範囲
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # フィールド
        fields = ['PX_LAST', 'PX_SETTLE', 'PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PX_VOLUME', 'OPEN_INT']
        
        logger.info(f"取得期間: {start_date.strftime('%Y-%m-%d')} ～ {end_date.strftime('%Y-%m-%d')}")
        logger.info(f"対象ティッカー: {list(tickers.keys())}")
        
        # データ取得
        all_data = []
        
        for ticker, description in tickers.items():
            logger.info(f"\n{ticker} ({description}) を取得中...")
            
            try:
                data = bloomberg.get_historical_data(
                    ticker,
                    fields,
                    start_date.strftime('%Y%m%d'),
                    end_date.strftime('%Y%m%d')
                )
                
                if data is not None and not data.empty:
                    # ティッカー名を統一（IndexをComdtyに変換）
                    generic_ticker = ticker.replace(' Index', ' Comdty')
                    
                    # データ整形
                    data_records = []
                    for index, row in data.iterrows():
                        # 日付処理
                        if hasattr(index, 'date'):
                            trade_date = index.date()
                        elif isinstance(index, pd.Timestamp):
                            trade_date = index.date()
                        else:
                            trade_date = pd.to_datetime(str(index)).date()
                        
                        record = {
                            'TradeDate': trade_date,
                            'GenericTicker': generic_ticker,
                            'Description': description,
                            'LastPrice': row.get('PX_LAST'),
                            'SettlementPrice': row.get('PX_SETTLE'),
                            'OpenPrice': row.get('PX_OPEN'),
                            'HighPrice': row.get('PX_HIGH'),
                            'LowPrice': row.get('PX_LOW'),
                            'Volume': row.get('PX_VOLUME', 0) or row.get('VOLUME', 0) or 0,
                            'OpenInterest': row.get('OPEN_INT', 0) or 0
                        }
                        
                        # 価格データがある場合のみ追加
                        if record['LastPrice'] is not None or record['SettlementPrice'] is not None:
                            data_records.append(record)
                            logger.info(f"  {trade_date}: Last={record['LastPrice']}, Settle={record['SettlementPrice']}")
                    
                    if data_records:
                        all_data.extend(data_records)
                        logger.info(f"  {len(data_records)}件のデータを取得")
                    else:
                        logger.warning(f"  価格データなし")
                else:
                    logger.warning(f"  データ取得失敗またはデータなし")
                    
            except Exception as e:
                logger.error(f"  エラー: {e}")
        
        # データベースに保存
        if all_data:
            logger.info(f"\n合計{len(all_data)}件のデータをデータベースに保存中...")
            
            # DataFrameに変換
            df = pd.DataFrame(all_data)
            
            # GenericIDを取得
            conn_str = (
                "DRIVER={ODBC Driver 17 for SQL Server};"
                "SERVER=jcz.database.windows.net;"
                "DATABASE=JCL;"
                "UID=TKJCZ01;"
                "PWD=P@ssw0rdmbkazuresql;"
                "Encrypt=yes;"
                "TrustServerCertificate=no;"
                "Connection Timeout=30;"
            )
            
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            
            # GenericIDマッピング
            generic_mapping = {}
            for ticker in df['GenericTicker'].unique():
                cursor.execute("""
                    SELECT GenericID FROM M_GenericFutures 
                    WHERE GenericTicker = ?
                """, ticker)
                result = cursor.fetchone()
                if result:
                    generic_mapping[ticker] = result[0]
                else:
                    logger.warning(f"GenericID not found for {ticker}")
            
            # GenericIDを追加
            df['GenericID'] = df['GenericTicker'].map(generic_mapping)
            df['DataType'] = 'Generic'
            df['LastUpdated'] = datetime.now()
            
            # MetalIDを追加
            cursor.execute("SELECT MetalID FROM M_Metal WHERE MetalCode = 'COPPER'")
            metal_id = cursor.fetchone()[0]
            df['MetalID'] = metal_id
            
            # データベースに保存
            success_count = 0
            for _, row in df.iterrows():
                if pd.notna(row['GenericID']):
                    try:
                        cursor.execute("""
                            MERGE T_CommodityPrice_V2 AS target
                            USING (
                                SELECT ? as TradeDate, ? as MetalID, ? as DataType, 
                                       ? as GenericID, NULL as ActualContractID
                            ) AS source
                            ON target.TradeDate = source.TradeDate
                                AND target.MetalID = source.MetalID
                                AND target.DataType = source.DataType
                                AND target.GenericID = source.GenericID
                            WHEN MATCHED THEN
                                UPDATE SET 
                                    SettlementPrice = ?,
                                    OpenPrice = ?,
                                    HighPrice = ?,
                                    LowPrice = ?,
                                    LastPrice = ?,
                                    Volume = ?,
                                    OpenInterest = ?,
                                    LastUpdated = ?
                            WHEN NOT MATCHED THEN
                                INSERT (TradeDate, MetalID, DataType, GenericID, 
                                       SettlementPrice, OpenPrice, HighPrice, LowPrice, 
                                       LastPrice, Volume, OpenInterest, LastUpdated)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """, 
                        # MERGE条件
                        row['TradeDate'], row['MetalID'], row['DataType'], 
                        row['GenericID'],
                        # UPDATE値
                        row['SettlementPrice'], row['OpenPrice'], row['HighPrice'], 
                        row['LowPrice'], row['LastPrice'], row['Volume'], 
                        row['OpenInterest'], row['LastUpdated'],
                        # INSERT値
                        row['TradeDate'], row['MetalID'], row['DataType'], 
                        row['GenericID'], row['SettlementPrice'], row['OpenPrice'], 
                        row['HighPrice'], row['LowPrice'], row['LastPrice'], 
                        row['Volume'], row['OpenInterest'], row['LastUpdated'])
                        
                        success_count += 1
                        
                    except Exception as e:
                        logger.error(f"保存エラー ({row['GenericTicker']} {row['TradeDate']}): {e}")
            
            conn.commit()
            conn.close()
            
            logger.info(f"{success_count}件のデータを保存完了")
            
        else:
            logger.warning("保存するデータがありません")
            
    except Exception as e:
        logger.error(f"処理エラー: {e}")
        
    finally:
        bloomberg.disconnect()
        logger.info("=== LME特殊データロード完了 ===")

def verify_loaded_data():
    """ロードされたデータの確認"""
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=jcz.database.windows.net;"
        "DATABASE=JCL;"
        "UID=TKJCZ01;"
        "PWD=P@ssw0rdmbkazuresql;"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    
    conn = pyodbc.connect(conn_str)
    
    query = """
    SELECT 
        g.GenericTicker,
        p.TradeDate,
        p.LastPrice,
        p.SettlementPrice,
        p.Volume,
        p.OpenInterest
    FROM T_CommodityPrice_V2 p
    INNER JOIN M_GenericFutures g ON p.GenericID = g.GenericID
    WHERE g.GenericTicker IN ('LMCADY Comdty', 'LMCADS03 Comdty', 'LMCADS 0003 Comdty')
        AND p.TradeDate >= DATEADD(day, -7, GETDATE())
    ORDER BY g.GenericTicker, p.TradeDate DESC
    """
    
    df = pd.read_sql(query, conn)
    
    print("\n【LME特殊データ確認】")
    print(df.to_string(index=False))
    
    conn.close()

def main():
    """メイン処理"""
    print(f"実行開始: {datetime.now()}")
    
    # 1. 既存データ確認
    check_existing_data()
    
    # 2. M_GenericFuturesに追加
    add_lme_special_tickers()
    
    # 3. データロード
    load_lme_special_data(days_back=7)  # 1週間分
    
    # 4. 結果確認
    verify_loaded_data()
    
    print(f"\n実行完了: {datetime.now()}")

if __name__ == "__main__":
    main()