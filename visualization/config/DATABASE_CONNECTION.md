# データベース接続設定ガイド

## 接続情報の設定方法

### 方法1: 直接編集（推奨）

`visualization/config/database_config.py` を編集して接続情報を設定します：

```python
DATABASE_CONFIG = {
    'driver': '{ODBC Driver 17 for SQL Server}',
    'server': 'your-server.database.windows.net',
    'database': 'your-database',
    'uid': 'your-username',
    'pwd': 'your-password',
    'Encrypt': 'yes',
    'TrustServerCertificate': 'no',
    'Connection Timeout': '30'
}
```

### 方法2: 環境変数（セキュリティ重視）

環境変数を設定して接続情報を管理します：

```bash
# Windows (コマンドプロンプト)
set DB_SERVER=your-server.database.windows.net
set DB_DATABASE=your-database
set DB_USERNAME=your-username
set DB_PASSWORD=your-password

# Windows (PowerShell)
$env:DB_SERVER="your-server.database.windows.net"
$env:DB_DATABASE="your-database"
$env:DB_USERNAME="your-username"
$env:DB_PASSWORD="your-password"

# Linux/Mac
export DB_SERVER=your-server.database.windows.net
export DB_DATABASE=your-database
export DB_USERNAME=your-username
export DB_PASSWORD=your-password
```

## 現在の設定値

プロジェクトのデフォルト設定：

- **サーバー**: jcz.database.windows.net
- **データベース**: JCL
- **ユーザー名**: TKJCZ01
- **ドライバー**: ODBC Driver 17 for SQL Server

## トラブルシューティング

### 1. ODBC Driverのインストール

```bash
# Windows
# Microsoft ODBC Driver 17 for SQL Serverをダウンロード・インストール
# https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

# Mac
brew install msodbcsql17

# Ubuntu/Debian
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update
apt-get install msodbcsql17
```

### 2. ファイアウォール設定

Azure SQL Serverのファイアウォール設定で、クライアントIPアドレスを許可してください。

### 3. 接続テスト

```python
# Pythonで接続テスト
import pyodbc

connection_string = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=jcz.database.windows.net;"
    "DATABASE=JCL;"
    "UID=TKJCZ01;"
    "PWD=your-password;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

try:
    conn = pyodbc.connect(connection_string)
    print("✅ 接続成功")
    conn.close()
except Exception as e:
    print(f"❌ 接続失敗: {e}")
```

### 4. よくあるエラー

**エラー**: `[Microsoft][ODBC Driver 17 for SQL Server]Login timeout expired`
- ネットワーク接続を確認
- サーバー名が正しいか確認
- ファイアウォール設定を確認

**エラー**: `[Microsoft][ODBC Driver 17 for SQL Server][SQL Server]Login failed for user`
- ユーザー名とパスワードを確認
- ユーザーに適切な権限があるか確認

**エラー**: `[Microsoft][ODBC Driver Manager] Data source name not found`
- ODBC Driverがインストールされているか確認
- ドライバー名が正しいか確認

## セキュリティ注意事項

1. **パスワードの管理**: 本番環境では環境変数や秘密管理ツールを使用
2. **接続文字列**: ログに出力しない
3. **権限管理**: 必要最小限の権限のみ付与
4. **SSL/TLS**: 必ず暗号化接続を使用（Encrypt=yes）