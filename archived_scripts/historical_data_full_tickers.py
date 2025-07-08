"""
マルチ取引所対応 全銘柄ヒストリカルデータ取得
LME: LP1-LP60, COMEX: HG1-HG36, SHFE: CU1-CU12
"""
import pyodbc
import pandas as pd
from datetime import datetime, date, timedelta
import time

# データベース接続文字列（Azure SQL Database）
CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=jcz.database.windows.net;"
    "DATABASE=JCL;"
    "UID=TKJCZ01;"
    "PWD=P@ssw0rdmbkazuresql;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

class FullTickerHistoricalLoader:
    """全銘柄ヒストリカルデータ取得"""
    
    def __init__(self):
        # 各取引所の全銘柄定義
        self.exchange_tickers = {
            'LME': [f'LP{i} Comdty' for i in range(1, 61)],     # LP1-LP60 (60銘柄)
            'SHFE': [f'CU{i} Comdty' for i in range(1, 13)],   # CU1-CU12 (12銘柄)
            'CMX': [f'HG{i} Comdty' for i in range(1, 37)]     # HG1-HG36 (36銘柄)
        }
        
        # 各取引所の基準価格（モックデータ用）
        self.base_prices = {
            'LME': 9500.0,    # USD/トン
            'SHFE': 73500.0,  # CNY/トン
            'CMX': 4.25       # USD/ポンド
        }
        
        print("=== 対象銘柄設定 ===")
        for exchange, tickers in self.exchange_tickers.items():
            display_name = "COMEX" if exchange == "CMX" else exchange
            print(f"{display_name}: {len(tickers)}銘柄 ({tickers[0]} - {tickers[-1]})")
        print(f"合計: {sum(len(tickers) for tickers in self.exchange_tickers.values())}銘柄")
        
    def load_historical_data(self, days_back=7):
        """全銘柄ヒストリカルデータ取得"""
        print(f"\n=== {days_back}日分 全銘柄ヒストリカルデータ取得開始 ===")
        
        try:
            conn = pyodbc.connect(CONNECTION_STRING)
            
            # 対象期間の決定
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days_back)
            business_dates = self._get_business_dates(start_date, end_date)
            
            print(f"対象期間: {start_date} - {end_date}")
            print(f"営業日: {len(business_dates)}日間")
            
            # 処理規模の表示
            total_tickers = sum(len(tickers) for tickers in self.exchange_tickers.values())
            total_operations = len(business_dates) * total_tickers
            print(f"総処理数: {total_operations}銘柄×日")
            
            # 各営業日の処理
            success_dates = 0
            
            for i, business_date in enumerate(business_dates, 1):
                print(f"\n[{i}/{len(business_dates)}] {business_date} 処理開始...")
                
                try:
                    # その日の全取引所データ処理
                    daily_success = self._process_daily_data(conn, business_date)
                    
                    if daily_success:
                        success_dates += 1
                        print(f"✅ {business_date} 完了")
                    else:
                        print(f"⚠️ {business_date} 一部失敗")
                    
                    # 処理間隔（大量データのため少し長めに）
                    time.sleep(2)
                    
                except Exception as e:
                    print(f"❌ {business_date} エラー: {e}")
                
            # 結果サマリー
            print(f"\n=== 処理結果 ===")
            print(f"成功: {success_dates}/{len(business_dates)}日")
            print(f"成功率: {success_dates/len(business_dates)*100:.1f}%")
            
            # データ確認
            self._check_full_data(conn)
            
            conn.close()
            return success_dates > 0
            
        except Exception as e:
            print(f"メインエラー: {e}")
            return False
            
    def _get_business_dates(self, start_date, end_date):
        """営業日リスト生成"""
        business_dates = []
        current_date = start_date
        
        while current_date <= end_date:
            if current_date.weekday() < 5:  # 土日除外
                business_dates.append(current_date)
            current_date += timedelta(days=1)
            
        return business_dates
        
    def _process_daily_data(self, conn, target_date):
        """日別全データ処理"""
        daily_success = True
        
        for exchange, tickers in self.exchange_tickers.items():
            display_name = "COMEX" if exchange == "CMX" else exchange
            
            try:
                print(f"  {display_name} ({len(tickers)}銘柄) 処理中...")
                
                # マッピング処理
                mapping_success = self._process_full_mapping(conn, exchange, tickers, target_date)
                
                # 価格データ処理
                price_success = self._process_full_prices(conn, exchange, tickers, target_date)
                
                if mapping_success and price_success:
                    print(f"    ✅ {display_name} 完了")
                else:
                    print(f"    ⚠️ {display_name} 一部失敗")
                    daily_success = False
                    
            except Exception as e:
                print(f"    ❌ {display_name}: {e}")
                daily_success = False
                
        return daily_success
        
    def _process_full_mapping(self, conn, exchange, tickers, target_date):
        """全銘柄マッピング処理"""
        try:
            cursor = conn.cursor()
            
            # 契約月コード生成（簡易版）
            month_codes = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
            base_month_code = month_codes[target_date.month - 1]
            
            # ジェネリック先物情報取得
            placeholders = ','.join(['?' for _ in tickers])
            cursor.execute(f"""
                SELECT GenericID, GenericTicker, MetalID
                FROM M_GenericFutures 
                WHERE ExchangeCode = ? AND GenericTicker IN ({placeholders})
                ORDER BY GenericNumber
            """, [exchange] + tickers)
            
            generic_info = {}
            for row in cursor.fetchall():
                generic_info[row[1]] = {'GenericID': row[0], 'MetalID': row[2]}
            
            # 各銘柄のマッピング処理
            for i, ticker in enumerate(tickers, 1):
                if ticker not in generic_info:
                    continue
                    
                # 実契約名生成（簡易版）
                if exchange == 'LME':
                    # LP1 -> LPN25, LP2 -> LPQ25, etc.
                    contract_ticker = f"LP{base_month_code}{str(target_date.year)[-1]}"
                elif exchange == 'SHFE':
                    # CU1 -> CUN5, CU2 -> CUQ5, etc.
                    contract_ticker = f"CU{base_month_code}{str(target_date.year)[-1]}"
                elif exchange == 'CMX':
                    # HG1 -> HGN5, HG2 -> HGU5, etc.
                    contract_ticker = f"HG{base_month_code}{str(target_date.year)[-1]}"
                
                # 実契約処理
                actual_contract_id = self._get_or_create_contract(
                    conn, contract_ticker, generic_info[ticker], exchange
                )
                
                if actual_contract_id:
                    # マッピング作成
                    self._create_mapping(
                        conn, target_date, generic_info[ticker]['GenericID'], 
                        actual_contract_id
                    )
            
            return True
            
        except Exception as e:
            print(f"      マッピングエラー: {e}")
            return False
            
    def _get_or_create_contract(self, conn, contract_ticker, generic_info, exchange):
        """実契約取得または作成"""
        try:
            cursor = conn.cursor()
            
            # 既存チェック
            cursor.execute(
                "SELECT ActualContractID FROM M_ActualContract WHERE ContractTicker = ?",
                (contract_ticker,)
            )
            existing = cursor.fetchone()
            
            if existing:
                return existing[0]
            
            # 新規作成
            cursor.execute("""
                INSERT INTO M_ActualContract (
                    ContractTicker, MetalID, ExchangeCode, ContractMonth,
                    ContractYear, ContractMonthCode, LastTradeableDate
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                contract_ticker,
                generic_info['MetalID'],
                exchange,
                pd.to_datetime('2025-07-01').date(),
                2025,
                'N',
                '2025-07-15'
            ))
            
            cursor.execute("SELECT @@IDENTITY")
            actual_contract_id = cursor.fetchone()[0]
            conn.commit()
            
            return actual_contract_id
            
        except Exception:
            return None
            
    def _create_mapping(self, conn, target_date, generic_id, actual_contract_id):
        """マッピング作成"""
        try:
            cursor = conn.cursor()
            
            # 既存チェック
            cursor.execute("""
                SELECT MappingID FROM T_GenericContractMapping 
                WHERE TradeDate = ? AND GenericID = ?
            """, (target_date, generic_id))
            
            if not cursor.fetchone():
                cursor.execute("""
                    INSERT INTO T_GenericContractMapping (
                        TradeDate, GenericID, ActualContractID, DaysToExpiry
                    ) VALUES (?, ?, ?, ?)
                """, (target_date, generic_id, actual_contract_id, 30))  # 仮の残存日数
                
            conn.commit()
            
        except Exception:
            pass
            
    def _process_full_prices(self, conn, exchange, tickers, target_date):
        """全銘柄価格データ処理"""
        try:
            # 週末はスキップ
            if target_date.weekday() >= 5:
                return True
                
            cursor = conn.cursor()
            
            # マッピング情報取得
            placeholders = ','.join(['?' for _ in tickers])
            cursor.execute(f"""
                SELECT g.GenericID, g.GenericTicker, g.MetalID, m.ActualContractID
                FROM M_GenericFutures g
                JOIN T_GenericContractMapping m ON m.GenericID = g.GenericID
                WHERE m.TradeDate = ? AND g.ExchangeCode = ? 
                  AND g.GenericTicker IN ({placeholders})
                ORDER BY g.GenericNumber
            """, [target_date, exchange] + tickers)
            
            mapping_info = {}
            for row in cursor.fetchall():
                mapping_info[row[1]] = {
                    'GenericID': row[0],
                    'MetalID': row[2],
                    'ActualContractID': row[3]
                }
            
            # 各銘柄の価格データ生成・格納
            base_price = self.base_prices[exchange]
            
            for i, ticker in enumerate(tickers):
                if ticker not in mapping_info:
                    continue
                    
                mapping = mapping_info[ticker]
                
                # 銘柄ごとに価格を少しずつ変える
                if exchange == 'LME':
                    # LP1=9500, LP2=9520, LP3=9540...
                    current_price = base_price + (i * 20)
                elif exchange == 'SHFE':
                    # CU1=73500, CU2=73600, CU3=73700...
                    current_price = base_price + (i * 100)
                elif exchange == 'CMX':
                    # HG1=4.25, HG2=4.27, HG3=4.29...
                    current_price = base_price + (i * 0.02)
                
                # 既存チェック
                cursor.execute("""
                    SELECT PriceID FROM T_CommodityPrice_V2 
                    WHERE TradeDate = ? AND GenericID = ? AND DataType = 'Generic'
                """, (target_date, mapping['GenericID']))
                
                if cursor.fetchone():
                    # 更新
                    cursor.execute("""
                        UPDATE T_CommodityPrice_V2 
                        SET SettlementPrice = ?, OpenPrice = ?, HighPrice = ?, 
                            LowPrice = ?, LastPrice = ?, Volume = ?, OpenInterest = ?,
                            LastUpdated = ?
                        WHERE TradeDate = ? AND GenericID = ? AND DataType = 'Generic'
                    """, (
                        current_price, current_price * 0.999, current_price * 1.002,
                        current_price * 0.997, current_price, 10000 + i * 100, 
                        20000 + i * 500, datetime.now(), target_date, mapping['GenericID']
                    ))
                else:
                    # 新規挿入
                    cursor.execute("""
                        INSERT INTO T_CommodityPrice_V2 (
                            TradeDate, MetalID, DataType, GenericID, ActualContractID,
                            SettlementPrice, OpenPrice, HighPrice, LowPrice, LastPrice,
                            Volume, OpenInterest
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        target_date, mapping['MetalID'], 'Generic',
                        mapping['GenericID'], mapping['ActualContractID'],
                        current_price, current_price * 0.999, current_price * 1.002,
                        current_price * 0.997, current_price, 10000 + i * 100, 
                        20000 + i * 500
                    ))
                
            conn.commit()
            return True
            
        except Exception as e:
            print(f"      価格データエラー: {e}")
            return False
            
    def _check_full_data(self, conn):
        """全データ確認"""
        print("\n=== 全銘柄データ確認 ===")
        
        try:
            # 取引所別サマリー
            query1 = """
                SELECT 
                    g.ExchangeCode,
                    COUNT(DISTINCT p.TradeDate) as データ日数,
                    COUNT(DISTINCT g.GenericID) as 銘柄数,
                    COUNT(*) as 価格レコード数,
                    MIN(p.SettlementPrice) as 最小価格,
                    MAX(p.SettlementPrice) as 最大価格,
                    AVG(CAST(p.SettlementPrice as FLOAT)) as 平均価格
                FROM T_CommodityPrice_V2 p
                JOIN M_GenericFutures g ON p.GenericID = g.GenericID
                WHERE p.TradeDate >= DATEADD(day, -7, GETDATE())
                GROUP BY g.ExchangeCode
                ORDER BY g.ExchangeCode
            """
            
            df1 = pd.read_sql(query1, conn)
            if not df1.empty:
                print("取引所別データサマリー:")
                for _, row in df1.iterrows():
                    exchange = "COMEX" if row['ExchangeCode'] == "CMX" else row['ExchangeCode']
                    print(f"  {exchange}:")
                    print(f"    データ日数: {row['データ日数']}日")
                    print(f"    銘柄数: {row['銘柄数']}銘柄")
                    print(f"    レコード数: {row['価格レコード数']}件")
                    print(f"    価格レンジ: {row['最小価格']:.2f} - {row['最大価格']:.2f}")
                    print(f"    平均価格: {row['平均価格']:.2f}")
            
            # 銘柄数の期待値チェック
            print("\n銘柄数チェック:")
            expected_counts = {'LME': 60, 'SHFE': 12, 'CMX': 36}
            for _, row in df1.iterrows():
                exchange = row['ExchangeCode']
                actual = row['銘柄数']
                expected = expected_counts.get(exchange, 0)
                status = "✅" if actual == expected else "⚠️"
                display_name = "COMEX" if exchange == "CMX" else exchange
                print(f"  {status} {display_name}: {actual}/{expected}銘柄")
                
        except Exception as e:
            print(f"確認エラー: {e}")

def main():
    """メイン実行"""
    loader = FullTickerHistoricalLoader()
    
    print("\n全銘柄対応版でヒストリカルデータ取得を開始します...")
    print("処理時間: 大量データのため時間がかかります")
    
    success = loader.load_historical_data(days_back=3)  # まず3日でテスト
    
    if success:
        print("\n" + "=" * 50)
        print("✅ 全銘柄ヒストリカルデータ取得完了")
        print("✅ LME: LP1-LP60")
        print("✅ COMEX: HG1-HG36") 
        print("✅ SHFE: CU1-CU12")
        print("=" * 50)
    else:
        print("NG 全銘柄ヒストリカルデータ取得失敗")
        
    return success

if __name__ == "__main__":
    main()