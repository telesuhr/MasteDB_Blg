-- GetTradingDaysBetween関数が存在するか確認
SELECT 
    name,
    type_desc,
    create_date,
    modify_date
FROM sys.objects
WHERE name = 'GetTradingDaysBetween'
    AND type IN ('FN', 'TF', 'IF'); -- FN: スカラー関数, TF: テーブル値関数, IF: インライン関数

-- または、より詳細な情報を取得
SELECT 
    ROUTINE_NAME,
    ROUTINE_TYPE,
    DATA_TYPE,
    CREATED,
    LAST_ALTERED
FROM INFORMATION_SCHEMA.ROUTINES
WHERE ROUTINE_NAME = 'GetTradingDaysBetween';

-- M_TradingCalendarテーブルが存在するか確認
SELECT 
    TABLE_NAME,
    TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = 'M_TradingCalendar';

-- 全ての関数を一覧表示（確認用）
SELECT 
    name as FunctionName,
    type_desc as FunctionType,
    create_date as CreatedDate
FROM sys.objects
WHERE type IN ('FN', 'TF', 'IF')
ORDER BY name;