CREATE OR REPLACE VIEW quarter1_summary AS
-- Min
SELECT 'Min' AS metric, MIN(PWC) AS PWC, MIN(EY) AS EY, MIN(Deloitte) AS Deloitte,
       MIN(KPMG) AS KPMG, MIN(BCG) AS BCG, MIN(Bain) AS Bain, MIN(Mckinsey) AS Mckinsey
FROM master_stocks WHERE QUARTER(date) = 1

UNION ALL
-- Max
SELECT 'Max', MAX(PWC), MAX(EY), MAX(Deloitte), MAX(KPMG),
       MAX(BCG), MAX(Bain), MAX(Mckinsey)
FROM master_stocks WHERE QUARTER(date) = 1

UNION ALL
-- Avg
SELECT 'Avg', ROUND(AVG(PWC),2), ROUND(AVG(EY),2), ROUND(AVG(Deloitte),2),
       ROUND(AVG(KPMG),2), ROUND(AVG(BCG),2), ROUND(AVG(Bain),2), ROUND(AVG(Mckinsey),2)
FROM master_stocks WHERE QUARTER(date) = 1

UNION ALL
-- StdDev
SELECT 'StdDev', ROUND(STDDEV(PWC),2), ROUND(STDDEV(EY),2), ROUND(STDDEV(Deloitte),2),
       ROUND(STDDEV(KPMG),2), ROUND(STDDEV(BCG),2), ROUND(STDDEV(Bain),2), ROUND(STDDEV(Mckinsey),2)
FROM master_stocks WHERE QUARTER(date) = 1

UNION ALL
-- Most Stable (pick stock with least StdDev)
SELECT 'Most_Stable',
       CASE 
         WHEN STDDEV(PWC) <= LEAST(STDDEV(EY), STDDEV(Deloitte), STDDEV(KPMG), STDDEV(BCG), STDDEV(Bain), STDDEV(Mckinsey)) THEN 'PWC'
         WHEN STDDEV(EY) <= LEAST(STDDEV(PWC), STDDEV(Deloitte), STDDEV(KPMG), STDDEV(BCG), STDDEV(Bain), STDDEV(Mckinsey)) THEN 'EY'
         WHEN STDDEV(Deloitte) <= LEAST(STDDEV(PWC), STDDEV(EY), STDDEV(KPMG), STDDEV(BCG), STDDEV(Bain), STDDEV(Mckinsey)) THEN 'Deloitte'
         WHEN STDDEV(KPMG) <= LEAST(STDDEV(PWC), STDDEV(EY), STDDEV(Deloitte), STDDEV(BCG), STDDEV(Bain), STDDEV(Mckinsey)) THEN 'KPMG'
         WHEN STDDEV(BCG) <= LEAST(STDDEV(PWC), STDDEV(EY), STDDEV(Deloitte), STDDEV(KPMG), STDDEV(Bain), STDDEV(Mckinsey)) THEN 'BCG'
         WHEN STDDEV(Bain) <= LEAST(STDDEV(PWC), STDDEV(EY), STDDEV(Deloitte), STDDEV(KPMG), STDDEV(BCG), STDDEV(Mckinsey)) THEN 'Bain'
         ELSE 'Mckinsey'
       END AS MostStableStock,
       NULL,NULL,NULL,NULL,NULL,NULL
FROM master_stocks WHERE QUARTER(date) = 1;