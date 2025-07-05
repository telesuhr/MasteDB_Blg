-- LME銅先物の13-36か月分のテナータイプを追加
-- 既存の12か月分はそのまま残す

-- 13-36か月分のGeneric Futuresを追加
DECLARE @i INT = 13;

WHILE @i <= 36
BEGIN
    -- テナータイプ名を生成
    DECLARE @TenorTypeName NVARCHAR(50);
    SET @TenorTypeName = 
        CASE 
            WHEN @i = 21 THEN 'Generic 21st Future'
            WHEN @i = 22 THEN 'Generic 22nd Future'
            WHEN @i = 23 THEN 'Generic 23rd Future'
            WHEN @i = 31 THEN 'Generic 31st Future'
            WHEN @i = 32 THEN 'Generic 32nd Future'
            WHEN @i = 33 THEN 'Generic 33rd Future'
            ELSE CONCAT('Generic ', @i, 'th Future')
        END;

    -- 存在しない場合のみ挿入
    IF NOT EXISTS (SELECT 1 FROM M_TenorType WHERE TenorTypeName = @TenorTypeName)
    BEGIN
        INSERT INTO M_TenorType (TenorTypeName, Description)
        VALUES (@TenorTypeName, 'LME Copper ' + @TenorTypeName);
        
        PRINT 'Added: ' + @TenorTypeName;
    END
    ELSE
    BEGIN
        PRINT 'Already exists: ' + @TenorTypeName;
    END

    SET @i = @i + 1;
END

-- 追加されたテナータイプを確認
SELECT * FROM M_TenorType 
WHERE TenorTypeName LIKE 'Generic%Future'
ORDER BY 
    CASE 
        WHEN TenorTypeName LIKE '%[0-9]st%' THEN CAST(SUBSTRING(TenorTypeName, 8, CHARINDEX('st', TenorTypeName) - 8) AS INT)
        WHEN TenorTypeName LIKE '%[0-9]nd%' THEN CAST(SUBSTRING(TenorTypeName, 8, CHARINDEX('nd', TenorTypeName) - 8) AS INT)
        WHEN TenorTypeName LIKE '%[0-9]rd%' THEN CAST(SUBSTRING(TenorTypeName, 8, CHARINDEX('rd', TenorTypeName) - 8) AS INT)
        WHEN TenorTypeName LIKE '%[0-9]th%' THEN CAST(SUBSTRING(TenorTypeName, 8, CHARINDEX('th', TenorTypeName) - 8) AS INT)
    END;