"""
MasterDB Bloomberg Data Ingestion System
Setup Script
"""
from setuptools import setup, find_packages

setup(
    name="masterdb-bloomberg",
    version="1.0.0",
    description="Bloomberg API data ingestion system for LME tenor spread analysis",
    author="JCL Team",
    packages=find_packages(),
    install_requires=[
        "blpapi>=3.19.1",
        "pyodbc>=5.1.0",
        "sqlalchemy>=2.0.30",
        "pandas>=2.2.2",
        "numpy>=1.26.4",
        "python-dateutil>=2.9.0",
        "loguru>=0.7.2",
        "python-dotenv>=1.0.1",
        "typing-extensions>=4.12.2"
    ],
    extras_require={
        "test": [
            "pytest>=8.2.2",
            "pytest-cov>=5.0.0"
        ]
    },
    python_requires=">=3.8",
)