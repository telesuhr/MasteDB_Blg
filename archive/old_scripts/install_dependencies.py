#!/usr/bin/env python3
"""
依存関係のインストールスクリプト
"""
import subprocess
import sys
import os

def install_package(package):
    """パッケージをインストール"""
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        return True
    except subprocess.CalledProcessError:
        return False

def main():
    print("Bloomberg Data Ingestor - Dependency Installer")
    print("=" * 50)
    
    # 必須パッケージのリスト
    required_packages = [
        "loguru==0.7.2",
        "pyodbc>=5.1.0",
        "pandas>=2.2.2",
        "numpy>=1.26.4",
        "python-dateutil>=2.9.0",
        "python-dotenv>=1.0.1",
    ]
    
    # オプションパッケージ
    optional_packages = [
        "blpapi>=3.19.1",  # Bloomberg APIは特別な設定が必要な場合がある
        "sqlalchemy>=2.0.30",
        "pytest>=8.2.2",
        "pytest-cov>=5.0.0",
        "typing-extensions>=4.12.2"
    ]
    
    print("Installing required packages...")
    for package in required_packages:
        print(f"Installing {package}...")
        if install_package(package):
            print(f"✓ {package} installed successfully")
        else:
            print(f"✗ Failed to install {package}")
    
    print("\nInstalling optional packages...")
    for package in optional_packages:
        print(f"Installing {package}...")
        if install_package(package):
            print(f"✓ {package} installed successfully")
        else:
            print(f"✗ Failed to install {package} (this may be optional)")
    
    print("\n" + "=" * 50)
    print("Installation completed!")
    print("\nIf Bloomberg API (blpapi) failed to install, please:")
    print("1. Ensure Bloomberg Terminal is installed")
    print("2. Try: pip install --index-url=https://bcms.bloomberg.com/pip/simple blpapi")
    print("3. Or download from Bloomberg Developer Portal")

if __name__ == "__main__":
    main()