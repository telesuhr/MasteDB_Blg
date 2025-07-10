-- M_Metal テーブルの CurrencyCode を修正するスクリプト
-- 既存のNULLレコードをUSDに更新し、将来の制約違反を防ぐ

-- 実行前の状況確認
PRINT '=== 修正前の状況 ===';
SELECT 
    COUNT(*) as 総レコード数,
    COUNT(CurrencyCode) as CurrencyCode非NULL数,
    COUNT(*) - COUNT(CurrencyCode) as CurrencyCodeのNULL数
FROM M_Metal;

-- NULL値のレコードを確認
PRINT '=== NULL値を持つレコード ===';
SELECT MetalCode, MetalName, CurrencyCode 
FROM M_Metal 
WHERE CurrencyCode IS NULL;

-- 更新実行
PRINT '=== CurrencyCodeの更新を実行 ===';
UPDATE M_Metal 
SET CurrencyCode = 'USD' 
WHERE CurrencyCode IS NULL;

PRINT CONCAT('更新されたレコード数: ', @@ROWCOUNT);

-- 修正後の確認
PRINT '=== 修正後の状況 ===';
SELECT 
    COUNT(*) as 総レコード数,
    COUNT(CurrencyCode) as CurrencyCode非NULL数,
    COUNT(*) - COUNT(CurrencyCode) as CurrencyCodeのNULL数
FROM M_Metal;

-- 全レコードの確認
PRINT '=== 全レコードの最終確認 ===';
SELECT MetalCode, MetalName, CurrencyCode 
FROM M_Metal 
ORDER BY MetalCode;

-- NULL値が残っていないことを確認
IF EXISTS (SELECT 1 FROM M_Metal WHERE CurrencyCode IS NULL)
BEGIN
    PRINT 'エラー: まだNULL値のレコードが存在します!';
END
ELSE
BEGIN
    PRINT '成功: すべてのレコードにCurrencyCodeが設定されました!';
END