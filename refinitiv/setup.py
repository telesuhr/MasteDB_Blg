#!/usr/bin/env python3
"""
Refinitiv システムセットアップスクリプト
"""
import os
import sys
import subprocess
import venv

def create_virtual_environment():
    """仮想環境を作成"""
    venv_path = os.path.join(os.path.dirname(__file__), 'venv')
    
    if os.path.exists(venv_path):
        print(f"✅ Virtual environment already exists: {venv_path}")
        return venv_path
    
    print(f"📦 Creating virtual environment: {venv_path}")
    venv.create(venv_path, with_pip=True)
    print(f"✅ Virtual environment created successfully")
    
    return venv_path

def install_requirements(venv_path):
    """依存パッケージをインストール"""
    requirements_file = os.path.join(os.path.dirname(__file__), 'requirements.txt')
    
    if not os.path.exists(requirements_file):
        print(f"❌ Requirements file not found: {requirements_file}")
        return False
    
    # 仮想環境のpipパス
    if sys.platform == "win32":
        pip_path = os.path.join(venv_path, 'Scripts', 'pip')
    else:
        pip_path = os.path.join(venv_path, 'bin', 'pip')
    
    print(f"📦 Installing requirements from {requirements_file}")
    
    try:
        subprocess.check_call([pip_path, 'install', '-r', requirements_file])
        print(f"✅ Requirements installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install requirements: {e}")
        return False

def setup_environment_file():
    """環境設定ファイルのセットアップ"""
    env_example = os.path.join(os.path.dirname(__file__), '.env.example')
    env_file = os.path.join(os.path.dirname(__file__), '.env')
    
    if os.path.exists(env_file):
        print(f"✅ Environment file already exists: {env_file}")
        return
    
    if os.path.exists(env_example):
        print(f"📝 Creating environment file from template")
        with open(env_example, 'r') as src, open(env_file, 'w') as dst:
            dst.write(src.read())
        print(f"✅ Environment file created: {env_file}")
        print(f"⚠️  Please edit {env_file} with your actual configuration")
    else:
        print(f"❌ Environment template not found: {env_example}")

def check_postgresql():
    """PostgreSQLの確認"""
    try:
        import psycopg2
        print(f"✅ PostgreSQL library (psycopg2) is available")
        return True
    except ImportError:
        print(f"❌ PostgreSQL library not found. Install with: pip install psycopg2-binary")
        return False

def main():
    """セットアップメイン処理"""
    print("🚀 Refinitiv Data Ingestion System Setup")
    print("=" * 50)
    
    # 1. 仮想環境作成
    venv_path = create_virtual_environment()
    
    # 2. 依存パッケージインストール
    if not install_requirements(venv_path):
        print("❌ Setup failed at requirements installation")
        return 1
    
    # 3. 環境設定ファイル
    setup_environment_file()
    
    # 4. PostgreSQL確認
    check_postgresql()
    
    print("\n🎉 Setup completed successfully!")
    print("\nNext steps:")
    print("1. Edit .env file with your Refinitiv API Key and PostgreSQL settings")
    print("2. Activate virtual environment:")
    
    if sys.platform == "win32":
        print("   .\\venv\\Scripts\\activate")
    else:
        print("   source venv/bin/activate")
    
    print("3. Test RIC codes:")
    print("   python test_ric_codes.py")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())