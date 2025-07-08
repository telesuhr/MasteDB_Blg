-- T_GenericContractMappingテーブルのデータをリセット
-- 既存のマッピングデータを全て削除して、再取得の準備をする

-- 1. 現在のデータ件数を確認
SELECT 
    COUNT(*) as TotalRecords,
    MIN(TradeDate) as OldestDate,
    MAX(TradeDate) as NewestDate,
    COUNT(DISTINCT GenericID) as UniqueGenericFutures
FROM T_GenericContractMapping;

-- 2. バックアップテーブルを作成（念のため）
IF OBJECT_ID('dbo.T_GenericContractMapping_Backup', 'U') IS NOT NULL
    DROP TABLE dbo.T_GenericContractMapping_Backup;

SELECT * 
INTO T_GenericContractMapping_Backup
FROM T_GenericContractMapping;

PRINT 'バックアップテーブル作成完了: T_GenericContractMapping_Backup';
PRINT '復元が必要な場合は以下を実行:';
PRINT 'INSERT INTO T_GenericContractMapping SELECT * FROM T_GenericContractMapping_Backup;';

-- 3. 全データを削除
DELETE FROM T_GenericContractMapping;

-- 4. 削除結果を確認
SELECT COUNT(*) as RecordsAfterDelete FROM T_GenericContractMapping;

PRINT 'T_GenericContractMappingのデータを全て削除しました。';
PRINT '次のステップ: historical_mapping_updater.pyを実行して直近1か月のデータを再取得してください。';

-- 5. 関連テーブルの状況確認
SELECT 
    'M_GenericFutures' as TableName,
    COUNT(*) as RecordCount
FROM M_GenericFutures
WHERE IsActive = 1
UNION ALL
SELECT 
    'M_ActualContract' as TableName,
    COUNT(*) as RecordCount
FROM M_ActualContract;