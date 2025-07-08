"""
テーブル再設計とデータ再ロード
GenericIDを主キーから外し、各取引所で1から始まるN番限月として扱う
"""
import pyodbc
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

class TableRedesignAndReload:
    """テーブル再設計とデータ再ロード"""
    
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
        
    def execute_redesign(self):
        """テーブル再設計実行"""
        print("=== テーブル再設計とデータ再ロード ===")
        
        try:
            conn = pyodbc.connect(CONNECTION_STRING)
            cursor = conn.cursor()
            
            # 1. 既存データのクリーンアップ
            print("\n1. 既存データクリーンアップ...")
            self._cleanup_all_data(conn)
            
            # 2. テーブル再作成
            print("\n2. テーブル再作成...")
            self._recreate_tables(conn)
            
            # 3. M_GenericFuturesに新しい構造でデータ挿入
            print("\n3. M_GenericFuturesデータ作成...")
            self._create_generic_futures(conn)
            
            # 4. 3営業日分のデータ作成
            print("\n4. 3営業日分のデータ作成...")
            business_dates = self._get_recent_business_dates(3)
            print(f"対象営業日: {', '.join(str(d) for d in business_dates)}")
            
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
            
            # 5. 結果確認
            self._verify_results(conn)
            
            conn.close()
            return True
            
        except Exception as e:
            print(f"エラー: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _cleanup_all_data(self, conn):
        """全データクリーンアップ"""
        cursor = conn.cursor()
        
        # 外部キー制約があるため順番に削除
        tables = [
            "T_CommodityPrice_V2",
            "T_GenericContractMapping", 
            "M_ActualContract",
            "M_GenericFutures"
        ]
        
        for table in tables:
            try:
                cursor.execute(f"DELETE FROM {table}")
                count = cursor.rowcount
                print(f"  {table}: {count}件削除")
            except Exception as e:
                print(f"  {table}: 削除エラー - {e}")
        
        conn.commit()
    
    def _recreate_tables(self, conn):
        """テーブル再作成（SQLファイルは使わず直接実行）"""
        cursor = conn.cursor()
        
        # M_GenericFuturesのみ再作成（他のテーブルはそのまま）
        try:
            # 既存テーブル削除
            cursor.execute("DROP TABLE IF EXISTS M_GenericFutures")
            conn.commit()
            
            # 新しい構造で作成（GenericIDは主キーではない）
            cursor.execute("""
                CREATE TABLE dbo.M_GenericFutures (
                    ID INT IDENTITY(1,1) NOT NULL,
                    GenericID INT NOT NULL,
                    GenericTicker NVARCHAR(20) NOT NULL,
                    MetalID INT NOT NULL,
                    ExchangeCode NVARCHAR(10) NOT NULL,
                    GenericNumber INT NOT NULL,
                    Description NVARCHAR(100) NULL,
                    IsActive BIT NOT NULL DEFAULT 1,
                    CreatedDate DATETIME2(0) NOT NULL DEFAULT GETDATE(),
                    CONSTRAINT PK_M_GenericFutures PRIMARY KEY CLUSTERED (ID),
                    CONSTRAINT UQ_M_GenericFutures_Ticker UNIQUE (GenericTicker),
                    CONSTRAINT UQ_M_GenericFutures_Exchange_GenericID UNIQUE (ExchangeCode, GenericID),
                    CONSTRAINT FK_M_GenericFutures_MetalID FOREIGN KEY (MetalID) 
                        REFERENCES dbo.M_Metal (MetalID)
                )
            """)
            
            # インデックス作成
            cursor.execute("""
                CREATE NONCLUSTERED INDEX IX_M_GenericFutures_MetalExchange 
                ON dbo.M_GenericFutures (MetalID, ExchangeCode, GenericID)
            """)
            
            conn.commit()
            print("  M_GenericFutures再作成完了")
            
        except Exception as e:
            print(f"  テーブル再作成エラー: {e}")
            raise
    
    def _create_generic_futures(self, conn):
        """M_GenericFuturesデータ作成"""
        cursor = conn.cursor()
        
        # MetalID取得
        cursor.execute("""
            SELECT MetalID, MetalCode
            FROM M_Metal
            WHERE MetalCode IN ('COPPER', 'CU_SHFE', 'CU_CMX')
        """)
        metal_ids = {}
        for row in cursor.fetchall():
            if row[1] == 'COPPER':
                metal_ids['LME'] = row[0]
            elif row[1] == 'CU_SHFE':
                metal_ids['SHFE'] = row[0]
            elif row[1] == 'CU_CMX':
                metal_ids['CMX'] = row[0]
        
        created_count = 0
        
        # LME: LP1-LP60 (GenericID=1-60)
        print("  LME作成中...")
        for i in range(1, 61):
            ticker = f'LP{i} Comdty'
            description = f'LME Copper Generic {i}{"st" if i == 1 else "nd" if i == 2 else "rd" if i == 3 else "th"} Future'
            
            cursor.execute("""
                INSERT INTO M_GenericFutures (
                    GenericID, GenericTicker, MetalID, ExchangeCode, 
                    GenericNumber, Description, IsActive
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (i, ticker, metal_ids['LME'], 'LME', i, description, 1))
            created_count += 1
        
        # SHFE: CU1-CU12 (GenericID=1-12)
        print("  SHFE作成中...")
        for i in range(1, 13):
            ticker = f'CU{i} Comdty'
            description = f'SHFE Copper Generic {i}{"st" if i == 1 else "nd" if i == 2 else "rd" if i == 3 else "th"} Future'
            
            cursor.execute("""
                INSERT INTO M_GenericFutures (
                    GenericID, GenericTicker, MetalID, ExchangeCode, 
                    GenericNumber, Description, IsActive
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (i, ticker, metal_ids['SHFE'], 'SHFE', i, description, 1))
            created_count += 1
        
        # CMX: HG1-HG36 (GenericID=1-36)
        print("  COMEX作成中...")
        for i in range(1, 37):
            ticker = f'HG{i} Comdty'
            description = f'COMEX Copper Generic {i}{"st" if i == 1 else "nd" if i == 2 else "rd" if i == 3 else "th"} Future'
            
            cursor.execute("""
                INSERT INTO M_GenericFutures (
                    GenericID, GenericTicker, MetalID, ExchangeCode, 
                    GenericNumber, Description, IsActive
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (i, ticker, metal_ids['CMX'], 'CMX', i, description, 1))
            created_count += 1
        
        conn.commit()
        print(f"  合計{created_count}銘柄作成完了")
    
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
        """日次マッピング作成（新しいテーブル構造対応）"""
        cursor = conn.cursor()
        created_count = 0
        
        # 全取引所・全銘柄取得（新しいテーブル構造）
        cursor.execute("""
            SELECT ID, GenericID, GenericTicker, GenericNumber, MetalID, ExchangeCode
            FROM M_GenericFutures
            ORDER BY ExchangeCode, GenericNumber
        """)
        
        generics = cursor.fetchall()
        
        for generic in generics:
            id = generic[0]  # 新しい主キー
            generic_id = generic[1]  # N番限月
            generic_ticker = generic[2]
            generic_number = generic[3]
            metal_id = generic[4]
            exchange = generic[5]
            
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
                    # マッピング作成（IDを使用）
                    try:
                        days_to_expiry = 30 + (generic_number - 1) * 30
                        
                        cursor.execute("""
                            INSERT INTO T_GenericContractMapping (
                                TradeDate, GenericID, ActualContractID, DaysToExpiry
                            ) VALUES (?, ?, ?, ?)
                        """, (business_date, id, actual_contract_id, days_to_expiry))
                        
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
        """日次価格データ作成（新しいテーブル構造対応）"""
        cursor = conn.cursor()
        created_count = 0
        
        # その日のマッピング情報取得（新しいテーブル構造）
        cursor.execute("""
            SELECT 
                m.GenericID as MappingGenericID,  -- これはM_GenericFuturesのID
                m.ActualContractID,
                g.GenericNumber,
                g.GenericID as RealGenericID,      -- これがN番限月
                g.MetalID,
                g.ExchangeCode
            FROM T_GenericContractMapping m
            JOIN M_GenericFutures g ON m.GenericID = g.ID
            WHERE m.TradeDate = ?
            ORDER BY g.ExchangeCode, g.GenericNumber
        """, (business_date,))
        
        mappings = cursor.fetchall()
        
        for mapping in mappings:
            mapping_generic_id = mapping[0]  # M_GenericFuturesのID
            actual_contract_id = mapping[1]
            generic_number = mapping[2]
            real_generic_id = mapping[3]     # N番限月
            metal_id = mapping[4]
            exchange = mapping[5]
            
            # 価格計算
            base_price = self.base_prices[exchange]
            
            if exchange == 'LME':
                price = base_price + (generic_number * 10)
            elif exchange == 'SHFE':
                price = base_price + (generic_number * 50)
            elif exchange == 'CMX':
                price = base_price + (generic_number * 0.01)
            
            # 価格データ挿入（GenericIDにはM_GenericFuturesのIDを使用）
            try:
                cursor.execute("""
                    INSERT INTO T_CommodityPrice_V2 (
                        TradeDate, MetalID, DataType, GenericID, ActualContractID,
                        SettlementPrice, OpenPrice, HighPrice, LowPrice, LastPrice,
                        Volume, OpenInterest
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    business_date, metal_id, 'Generic',
                    mapping_generic_id, actual_contract_id,
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
        
        # 1. M_GenericFuturesの新しい構造確認
        print("\n1. M_GenericFuturesの新しい構造:")
        cursor.execute("""
            SELECT 
                ExchangeCode,
                MIN(GenericID) as 最小GenericID,
                MAX(GenericID) as 最大GenericID,
                COUNT(*) as 銘柄数
            FROM M_GenericFutures
            GROUP BY ExchangeCode
            ORDER BY ExchangeCode
        """)
        
        for row in cursor.fetchall():
            display_name = "COMEX" if row[0] == "CMX" else row[0]
            print(f"  {display_name}: GenericID {row[1]}-{row[2]} ({row[3]}銘柄)")
        
        # 2. 各取引所の最初の3銘柄
        print("\n2. 各取引所の最初の3銘柄:")
        for exchange in ['LME', 'CMX', 'SHFE']:
            cursor.execute("""
                SELECT TOP 3 ID, GenericID, GenericTicker
                FROM M_GenericFutures
                WHERE ExchangeCode = ?
                ORDER BY GenericID
            """, (exchange,))
            
            display_name = "COMEX" if exchange == "CMX" else exchange
            print(f"\n{display_name}:")
            for row in cursor.fetchall():
                print(f"  {row[2]}: ID={row[0]}, GenericID={row[1]}")
        
        # 3. 価格データサマリー
        print("\n3. 価格データサマリー:")
        cursor.execute("""
            SELECT 
                g.ExchangeCode,
                COUNT(DISTINCT p.GenericID) as ID数,
                COUNT(DISTINCT p.ActualContractID) as 実契約数,
                COUNT(*) as レコード数
            FROM T_CommodityPrice_V2 p
            JOIN M_GenericFutures g ON p.GenericID = g.ID
            GROUP BY g.ExchangeCode
            ORDER BY g.ExchangeCode
        """)
        
        for row in cursor.fetchall():
            display_name = "COMEX" if row[0] == "CMX" else row[0]
            print(f"  {display_name}: {row[1]}ID, {row[2]}実契約, {row[3]}レコード")

def main():
    """メイン実行"""
    redesigner = TableRedesignAndReload()
    success = redesigner.execute_redesign()
    
    if success:
        print("\n" + "=" * 50)
        print("OK テーブル再設計とデータ再ロード完了")
        print("GenericIDは各取引所で1から始まるN番限月を表すようになりました")
        print("=" * 50)
    else:
        print("\nNG テーブル再設計失敗")
    
    return success

if __name__ == "__main__":
    main()