-- 不要なデータベースオブジェクトの削除
-- 実行前に必ずバックアップを取得してください

-- 1. 外部キー制約の削除
-- M_ActualContractを参照している制約を削除
ALTER TABLE T_GenericContractMapping DROP CONSTRAINT FK_T_GenericContractMapping_Actual;
ALTER TABLE T_CommodityPrice DROP CONSTRAINT FK_T_CommodityPrice_V2_ActualContractID;

-- 2. 不要テーブルの削除
-- 依存関係の順序に注意して削除

-- まず依存しているテーブルから削除
DROP TABLE IF EXISTS T_GenericContractMapping;

-- 次に参照されているマスターテーブルを削除
DROP TABLE IF EXISTS M_ActualContract;

-- 更新が停止しているテーブルを削除
DROP TABLE IF EXISTS T_MacroEconomicIndicator;
DROP TABLE IF EXISTS T_BandingReport;

-- 3. 削除確認
SELECT 'Remaining tables:' as Status;
SELECT TABLE_NAME 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_NAME IN ('M_ActualContract', 'T_GenericContractMapping', 'T_MacroEconomicIndicator', 'T_BandingReport')
ORDER BY TABLE_NAME;