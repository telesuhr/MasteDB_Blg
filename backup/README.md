# Backup Files

This directory contains legacy versions of scripts that have been replaced with improved implementations.

## Legacy Daily Update (backup/legacy_daily_update/)

- `run_daily.bat` - Original simple daily update script
- `run.py` - Original daily update Python script

These files were replaced with the enhanced daily update system that includes:
- Market timing awareness (LME, SHFE, CMX specific settlement times)
- Data validation and change detection
- Timezone-aware processing
- Improved error handling

The enhanced version is now the main `run_daily.py` and `run_daily.bat` in the project root.

## Date Archived
2025-06-28