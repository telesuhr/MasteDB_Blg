"""
テーブル構造確認
"""
import pyodbc

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

conn = pyodbc.connect(CONNECTION_STRING)
cursor = conn.cursor()

# T_GenericContractMappingの構造確認
cursor.execute("""
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'T_GenericContractMapping'
ORDER BY ORDINAL_POSITION
""")

print("T_GenericContractMapping columns:")
for row in cursor.fetchall():
    print(f"  {row[0]}: {row[1]}")

# M_ActualContractの構造確認
cursor.execute("""
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'M_ActualContract'
ORDER BY ORDINAL_POSITION
""")

print("\nM_ActualContract columns:")
for row in cursor.fetchall():
    print(f"  {row[0]}: {row[1]}")

# M_Metalの構造確認
cursor.execute("""
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'M_Metal'
ORDER BY ORDINAL_POSITION
""")

print("\nM_Metal columns:")
for row in cursor.fetchall():
    print(f"  {row[0]}: {row[1]}")

# サンプルデータ確認
cursor.execute("""
SELECT TOP 5 * FROM T_GenericContractMapping
""")

print("\nT_GenericContractMapping sample data:")
rows = cursor.fetchall()
if rows:
    for row in rows:
        print(f"  {row}")
else:
    print("  No data found")

conn.close()