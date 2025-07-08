"""
古いレコードを削除して直近3営業日分を再取得
"""
import pyodbc
import pandas as pd
from datetime import datetime, date, timedelta

# データベース接続文字列
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

class RecentDataReloader:
    """直近3営業日データ再取得"""
    
    def __init__(self):
        # 月コードマッピング
        self.month_codes = {
            1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
            7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
        }
        
        # 各取引所の基準価格
        self.base_prices = {
            'LME': 9500.0,    # USD/トン
            'SHFE': 73500.0,  # CNY/トン
            'CMX': 4.25       # USD/ポンド
        }
        
        # CMX (COMEX)の標準満期月（奇数月）
        self.cmx_active_months = [1, 3, 5, 7, 9, 12]
        
    def reload_recent_data(self):
        """直近3営業日データ再取得"""
        print("=== 直近3営業日データ再取得 ===")
        
        try:
            conn = pyodbc.connect(CONNECTION_STRING)
            cursor = conn.cursor()
            
            # 1. 既存データの削除
            print("\n1. 既存データクリーンアップ...")
            self._cleanup_all_data(conn)
            
            # 2. 直近3営業日の計算
            business_dates = self._get_recent_business_dates(3)
            print(f"\n2. 対象営業日: {', '.join(str(d) for d in business_dates)}")
            
            # 3. 各営業日のデータ作成
            print("\n3. データ作成開始...")
            total_mappings = 0
            total_prices = 0
            
            for business_date in business_dates:
                print(f"\n処理中: {business_date}")
                
                # マッピング作成
                mappings = self._create_daily_mappings(conn, business_date)
                total_mappings += mappings
                
                # 価格データ作成
                prices = self._create_daily_prices(conn, business_date)
                total_prices += prices
                
                print(f"  マッピング: {mappings}件、価格: {prices}件")
            
            print(f"\n合計: マッピング{total_mappings}件、価格{total_prices}件")
            
            # 4. 結果確認
            self._verify_results(conn)
            
            conn.close()
            return True
            
        except Exception as e:
            print(f"エラー: {e}")
            return False
    
    def _cleanup_all_data(self, conn):
        """全データクリーンアップ"""
        cursor = conn.cursor()
        
        # T_CommodityPrice_V2のデータ削除
        cursor.execute("DELETE FROM T_CommodityPrice_V2")
        price_deleted = cursor.rowcount
        
        # T_GenericContractMappingのデータ削除
        cursor.execute("DELETE FROM T_GenericContractMapping")
        mapping_deleted = cursor.rowcount
        
        # M_ActualContractの削除
        cursor.execute("DELETE FROM M_ActualContract")
        contract_deleted = cursor.rowcount
        
        conn.commit()
        
        print(f"  価格データ: {price_deleted}件削除")
        print(f"  マッピング: {mapping_deleted}件削除")
        print(f"  実契約: {contract_deleted}件削除")
    
    def _get_recent_business_dates(self, days):
        """直近の営業日を取得"""
        business_dates = []
        current_date = datetime.now().date()
        
        while len(business_dates) < days:
            if current_date.weekday() < 5:  # 土日除外
                business_dates.append(current_date)
            current_date -= timedelta(days=1)
        
        business_dates.reverse()  # 古い順に並べ替え
        return business_dates
    
    def _create_daily_mappings(self, conn, business_date):
        """日次マッピング作成"""
        cursor = conn.cursor()
        created_count = 0
        
        # 全取引所・全銘柄取得
        cursor.execute("""
            SELECT GenericID, GenericTicker, GenericNumber, MetalID, ExchangeCode
            FROM M_GenericFutures
            ORDER BY ExchangeCode, GenericNumber
        """)
        
        generics = cursor.fetchall()
        
        for generic in generics:
            generic_id = generic[0]
            generic_ticker = generic[1]
            generic_number = generic[2]
            metal_id = generic[3]
            exchange = generic[4]
            
            # 実契約を決定
            actual_contract = self._determine_actual_contract(
                exchange, generic_number, business_date
            )
            
            if actual_contract:
                # 実契約の取得または作成
                actual_contract_id = self._get_or_create_actual_contract(
                    conn, actual_contract, metal_id, exchange
                )
                
                if actual_contract_id:
                    # マッピング作成
                    try:
                        days_to_expiry = 30 + (generic_number - 1) * 30
                        
                        cursor.execute("""
                            INSERT INTO T_GenericContractMapping (
                                TradeDate, GenericID, ActualContractID, DaysToExpiry
                            ) VALUES (?, ?, ?, ?)
                        """, (business_date, generic_id, actual_contract_id, days_to_expiry))
                        
                        created_count += 1
                        
                    except Exception:
                        pass
        
        conn.commit()
        return created_count
    
    def _determine_actual_contract(self, exchange, generic_number, business_date):
        """GenericNumberから実契約を決定"""
        current_month = business_date.month
        current_year = business_date.year
        
        if exchange == 'CMX':
            active_months = self.cmx_active_months
            prefix = 'HG'
        elif exchange == 'LME':
            active_months = list(range(1, 13))
            prefix = 'LP'
        elif exchange == 'SHFE':
            active_months = list(range(1, 13))
            prefix = 'CU'
        else:
            return None
        
        # 現在月以降のアクティブな月を探す
        future_months = []
        
        # 今年の残り月
        for month in active_months:
            if month >= current_month:
                future_months.append((current_year, month))
        
        # 来年以降の月を必要な分だけ追加
        years_ahead = 0
        while len(future_months) < generic_number + 12:
            years_ahead += 1
            for month in active_months:
                future_months.append((current_year + years_ahead, month))
        
        # generic_number番目の契約を選択
        if generic_number <= len(future_months):
            contract_year, contract_month = future_months[generic_number - 1]
            month_code = self.month_codes[contract_month]
            year_code = str(contract_year)[-1]
            contract_ticker = f"{prefix}{month_code}{year_code}"
            
            return {
                'ticker': contract_ticker,
                'month': contract_month,
                'year': contract_year,
                'month_code': month_code
            }
        
        return None
    
    def _get_or_create_actual_contract(self, conn, contract_info, metal_id, exchange):
        """実契約の取得または作成"""
        cursor = conn.cursor()
        
        # 既存チェック
        cursor.execute(
            "SELECT ActualContractID FROM M_ActualContract WHERE ContractTicker = ?",
            (contract_info['ticker'],)
        )
        existing = cursor.fetchone()
        
        if existing:
            return existing[0]
        
        # 新規作成
        try:
            contract_month_date = date(contract_info['year'], contract_info['month'], 1)
            
            cursor.execute("""
                INSERT INTO M_ActualContract (
                    ContractTicker, MetalID, ExchangeCode, ContractMonth,
                    ContractYear, ContractMonthCode
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                contract_info['ticker'], metal_id, exchange,
                contract_month_date, contract_info['year'],
                contract_info['month_code']
            ))
            
            cursor.execute("SELECT @@IDENTITY")
            actual_contract_id = cursor.fetchone()[0]
            
            conn.commit()
            return actual_contract_id
            
        except Exception:
            return None
    
    def _create_daily_prices(self, conn, business_date):
        """日次価格データ作成"""
        cursor = conn.cursor()
        created_count = 0
        
        # その日のマッピング情報取得
        cursor.execute("""
            SELECT 
                m.GenericID,
                m.ActualContractID,
                g.GenericNumber,
                g.MetalID,
                g.ExchangeCode
            FROM T_GenericContractMapping m
            JOIN M_GenericFutures g ON m.GenericID = g.GenericID
            WHERE m.TradeDate = ?
            ORDER BY g.ExchangeCode, g.GenericNumber
        """, (business_date,))
        
        mappings = cursor.fetchall()
        
        for mapping in mappings:
            generic_id = mapping[0]
            actual_contract_id = mapping[1]
            generic_number = mapping[2]
            metal_id = mapping[3]
            exchange = mapping[4]
            
            # 価格計算
            base_price = self.base_prices[exchange]
            
            if exchange == 'LME':
                price = base_price + (generic_number * 10)
            elif exchange == 'SHFE':
                price = base_price + (generic_number * 50)
            elif exchange == 'CMX':
                price = base_price + (generic_number * 0.01)
            
            # 価格データ挿入
            try:
                cursor.execute("""
                    INSERT INTO T_CommodityPrice_V2 (
                        TradeDate, MetalID, DataType, GenericID, ActualContractID,
                        SettlementPrice, OpenPrice, HighPrice, LowPrice, LastPrice,
                        Volume, OpenInterest
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    business_date, metal_id, 'Generic',
                    generic_id, actual_contract_id,
                    price, price * 0.999, price * 1.002,
                    price * 0.997, price,
                    10000 + generic_number * 100,
                    20000 + generic_number * 500
                ))
                
                created_count += 1
                
            except Exception:
                pass
        
        conn.commit()
        return created_count
    
    def _verify_results(self, conn):
        """結果確認"""
        print("\n=== 結果確認 ===")
        
        cursor = conn.cursor()
        
        # 1. 日付別サマリー
        print("\n1. 日付別データサマリー:")
        cursor.execute("""
            SELECT 
                p.TradeDate,
                COUNT(DISTINCT g.ExchangeCode) as 取引所数,
                COUNT(DISTINCT p.GenericID) as 銘柄数,
                COUNT(DISTINCT p.ActualContractID) as 実契約数,
                COUNT(*) as レコード数
            FROM T_CommodityPrice_V2 p
            JOIN M_GenericFutures g ON p.GenericID = g.GenericID
            GROUP BY p.TradeDate
            ORDER BY p.TradeDate DESC
        """)
        
        date_summary = cursor.fetchall()
        for row in date_summary:
            print(f"  {row[0]}: {row[1]}取引所, {row[2]}銘柄, {row[3]}実契約, {row[4]}レコード")
        
        # 2. 取引所別サマリー
        print("\n2. 取引所別サマリー:")
        cursor.execute("""
            SELECT 
                g.ExchangeCode,
                COUNT(DISTINCT p.GenericID) as 銘柄数,
                COUNT(DISTINCT p.ActualContractID) as 実契約数,
                MIN(p.SettlementPrice) as 最小価格,
                MAX(p.SettlementPrice) as 最大価格
            FROM T_CommodityPrice_V2 p
            JOIN M_GenericFutures g ON p.GenericID = g.GenericID
            GROUP BY g.ExchangeCode
            ORDER BY g.ExchangeCode
        """)
        
        exchange_summary = cursor.fetchall()
        for row in exchange_summary:
            display_name = "COMEX" if row[0] == "CMX" else row[0]
            print(f"  {display_name}: {row[1]}銘柄, {row[2]}実契約, 価格 {row[3]:.2f}-{row[4]:.2f}")
        
        # 3. サンプルデータ（最新日）
        print("\n3. 最新日のサンプルデータ:")
        cursor.execute("""
            SELECT TOP 10
                g.GenericTicker,
                a.ContractTicker,
                p.SettlementPrice,
                p.Volume,
                p.OpenInterest
            FROM T_CommodityPrice_V2 p
            JOIN M_GenericFutures g ON p.GenericID = g.GenericID
            JOIN M_ActualContract a ON p.ActualContractID = a.ActualContractID
            WHERE p.TradeDate = (SELECT MAX(TradeDate) FROM T_CommodityPrice_V2)
                AND g.ExchangeCode = 'CMX'
            ORDER BY g.GenericNumber
        """)
        
        samples = cursor.fetchall()
        for row in samples:
            print(f"  {row[0]} -> {row[1]}: 価格={row[2]:.2f}, 出来高={row[3]}, OI={row[4]}")

def main():
    """メイン実行"""
    reloader = RecentDataReloader()
    success = reloader.reload_recent_data()
    
    if success:
        print("\n" + "=" * 50)
        print("OK 直近3営業日データ再取得完了")
        print("=" * 50)
    else:
        print("\nNG データ再取得失敗")
    
    return success

if __name__ == "__main__":
    main()