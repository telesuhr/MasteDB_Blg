-- LME在庫データを削除（再取得前に実行）
-- 注意: これは既存のLME在庫データを全て削除します

-- 実行前に現在のデータ件数を確認
SELECT COUNT(*) as CurrentRecordCount FROM T_LMEInventory;

-- 地域別の現在のデータを確認
SELECT 
    r.RegionName,
    COUNT(*) as RecordCount,
    MIN(li.ReportDate) as OldestDate,
    MAX(li.ReportDate) as LatestDate
FROM T_LMEInventory li
JOIN M_Region r ON li.RegionID = r.RegionID
GROUP BY r.RegionName
ORDER BY r.RegionName;

-- データを削除する場合はこのコメントを外して実行
-- DELETE FROM T_LMEInventory;

-- 削除後の確認
-- SELECT COUNT(*) as RemainingRecords FROM T_LMEInventory;