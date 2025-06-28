# Archive Directory

This directory contains files that are no longer actively used in the main project but are kept for reference.

## Structure

### dev_tools/
Development and debugging tools:
- `debug_inventory.py` - Debug script for inventory data with detailed logging
- `test_lme_tickers.py` - Script to test various LME ticker patterns
- `check_odbc.py` - ODBC driver check utility

### old_scripts/
Obsolete scripts replaced by improved versions:
- `update_inventory.py` - Original inventory update script
- `update_inventory_safe.py` - Safe inventory update with MEST exclusion
- `run_simple.py` - Simple execution script
- `setup.py` - Old setup script
- `install_dependencies.py` - Old dependency installer
- `requirements_no_blpapi.txt` - Alternative requirements without Bloomberg API
- `logging_config_simple.py` - Simple logging configuration

### testing/
Test files:
- `tests/` - Original test directory with unit tests

## Main Project Structure
The main project now uses:
- `run_daily.py` - Main daily update script (enhanced version)
- `run_daily.bat` - Windows batch file for daily updates
- `run_initial.py` - Initial data load script
- `src/` - Main source code
- `config/` - Configuration files
- `sql/` - Database scripts
- `docs/` - Documentation

## Date Archived
2025-06-28