-- Creating db
CREATE DATABASE ConsultingStocks;
USE ConsultingStocks;

-- creating tables
CREATE TABLE pwc (date DATE, close_price DECIMAL(10,2));
CREATE TABLE ey (date DATE, close_price DECIMAL(10,2));
CREATE TABLE deloitte (date DATE, close_price DECIMAL(10,2));
CREATE TABLE kpmg (date DATE, close_price DECIMAL(10,2));
CREATE TABLE bcg (date DATE, close_price DECIMAL(10,2));
CREATE TABLE bain (date DATE, close_price DECIMAL(10,2));
CREATE TABLE mckinsey (date DATE, close_price DECIMAL(10,2));

-- Loading CSV files comprising of stock data
LOAD DATA INFILE '/path/to/pwc.csv' INTO TABLE pwc FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS (date, close_price);
LOAD DATA INFILE '/path/to/ey.csv' INTO TABLE ey FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS (date, close_price);
LOAD DATA INFILE '/path/to/kpmg.csv' INTO TABLE kpmg FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS (date, close_price);
LOAD DATA INFILE '/path/to/deloitte.csv' INTO TABLE deloitte FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS (date, close_price);
LOAD DATA INFILE '/path/to/bcg.csv' INTO TABLE bcg FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS (date, close_price);
LOAD DATA INFILE '/path/to/bain.csv' INTO TABLE bain FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS (date, close_price);
LOAD DATA INFILE '/path/to/mckinsey.csv' INTO TABLE mckinsey FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS (date, close_price);

-- master table to ease user understanding 
CREATE OR REPLACE VIEW master_stocks AS
SELECT
    pwc.date AS date,
    pwc.close_price AS PWC,
    ey.close_price AS EY,
    deloitte.close_price AS Deloitte,
    kpmg.close_price AS KPMG,
    bcg.close_price AS BCG,
    bain.close_price AS Bain,
    mckinsey.close_price AS Mckinsey
FROM pwc
LEFT JOIN ey ON pwc.date = ey.date
LEFT JOIN deloitte ON pwc.date = deloitte.date
LEFT JOIN kpmg ON pwc.date = kpmg.date
LEFT JOIN bcg ON pwc.date = bcg.date
LEFT JOIN bain ON pwc.date = bain.date
LEFT JOIN mckinsey ON pwc.date = mckinsey.date;

-- calculating metrics for tables of quarters to furnish in master table
-- first quarter
CREATE VIEW quarter1_summary AS
WITH moving_avg AS (
    SELECT
        date,
        PWC, EY, Deloitte, KPMG, BCG, Bain, Mckinsey,
        ROUND(AVG(PWC) OVER(ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),2) AS PWC_MA20,
        ROUND(AVG(PWC) OVER(ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),2) AS PWC_MA50,
        ROUND(AVG(EY) OVER(ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),2) AS EY_MA20,
        ROUND(AVG(EY) OVER(ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),2) AS EY_MA50,
        ROUND(AVG(Deloitte) OVER(ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),2) AS Deliotte_MA20,
        ROUND(AVG(Deloitte) OVER(ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),2) AS Deloitte_MA50,
        ROUND(AVG(Kpmg) OVER(ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),2) AS Kpmg_MA20,
        ROUND(AVG(Kpmg) OVER(ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),2) AS Kpmg_MA50,
        ROUND(AVG(Bcg) OVER(ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),2) AS Bcg_MA20,
        ROUND(AVG(Bcg) OVER(ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),2) AS Bcg_MA50,
        ROUND(AVG(Bain) OVER(ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),2) AS Bain_MA20,
        ROUND(AVG(Bain) OVER(ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),2) AS Bain_MA50,
        ROUND(AVG(Mckinsey) OVER(ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),2) AS Mckinsey_MA20,
        ROUND(AVG(Mckinsey) OVER(ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),2) AS Mckinsey_MA50
  
    FROM master_stocks
    WHERE QUARTER(date) = 1
),
signals AS (
    SELECT
        date,
        CASE
            WHEN PWC_MA20 > PWC_MA50 AND LAG(PWC_MA20) OVER(ORDER BY date) <= LAG(PWC_MA50) OVER(ORDER BY date) THEN 'Golden Cross'
            WHEN PWC_MA20 < PWC_MA50 AND LAG(PWC_MA20) OVER(ORDER BY date) >= LAG(PWC_MA50) OVER(ORDER BY date) THEN 'Death Cross'
            ELSE 'Hold'
        END AS PWC_signal,
        
        CASE
            WHEN EY_MA20 > EY_MA50 AND LAG(EY_MA20) OVER(ORDER BY date) <= LAG(EY_MA50) OVER(ORDER BY date) THEN 'Golden Cross'
            WHEN EY_MA20 < EY_MA50 AND LAG(EY_MA20) OVER(ORDER BY date) >= LAG(EY_MA50) OVER(ORDER BY date) THEN 'Death Cross'
            ELSE 'Hold'
        END AS EY_signal,

         CASE
            WHEN Deloitte_MA20 > Deloitte_MA50 AND LAG(Deloitte_MA20) OVER(ORDER BY date) <= LAG(Deloitte_MA50) OVER(ORDER BY date) THEN 'Golden Cross'
            WHEN Deloitte_MA20 < Deloitte_MA50 AND LAG(Deloitte_MA20) OVER(ORDER BY date) >= LAG(Deloitte_MA50) OVER(ORDER BY date) THEN 'Death Cross'
            ELSE 'Hold'
        END AS Deloitte_signal,

        CASE
            WHEN Kpmg_MA20 > Kpmg_MA50 AND LAG(Kpmg_MA20) OVER(ORDER BY date) <= LAG(Kpmg_MA50) OVER(ORDER BY date) THEN 'Golden Cross'
            WHEN Kpmg_MA20 < Kpmg_MA50 AND LAG(Kpmg_MA20) OVER(ORDER BY date) >= LAG(Kpmg_MA50) OVER(ORDER BY date) THEN 'Death Cross'
            ELSE 'Hold'
        END AS Kpmg_signal,

       CASE
            WHEN EY_MA20 > EY_MA50 AND LAG(EY_MA20) OVER(ORDER BY date) <= LAG(EY_MA50) OVER(ORDER BY date) THEN 'Golden Cross'
            WHEN EY_MA20 < EY_MA50 AND LAG(EY_MA20) OVER(ORDER BY date) >= LAG(EY_MA50) OVER(ORDER BY date) THEN 'Death Cross'
            ELSE 'Hold'
        END AS EY_signal,

        CASE
            WHEN Bcg_MA20 > Bcg_MA50 AND LAG(Bcg_MA20) OVER(ORDER BY date) <= LAG(Bcg_MA50) OVER(ORDER BY date) THEN 'Golden Cross'
            WHEN Bcg_MA20 < Bcg_MA50 AND LAG(Bcg_MA20) OVER(ORDER BY date) >= LAG(Bcg_MA50) OVER(ORDER BY date) THEN 'Death Cross'
            ELSE 'Hold'
        END AS Bcg_signal,
        
        CASE
            WHEN Bain_MA20 > Bain_MA50 AND LAG(Bain_MA20) OVER(ORDER BY date) <= LAG(Bain_MA50) OVER(ORDER BY date) THEN 'Golden Cross'
            WHEN Bain_MA20 < Bain_MA50 AND LAG(Bain_MA20) OVER(ORDER BY date) >= LAG(Bain_MA50) OVER(ORDER BY date) THEN 'Death Cross'
            ELSE 'Hold'
        END AS Bain_signal,

        CASE
            WHEN Mckinsey_MA20 > Mckinsey_MA50 AND LAG(Mckinsey_MA20) OVER(ORDER BY date) <= LAG(Mckinsey_MA50) OVER(ORDER BY date) THEN 'Golden Cross'
            WHEN Mckinsey_MA20 < Mckinsey_MA50 AND LAG(Mckinsey_MA20) OVER(ORDER BY date) >= LAG(Mckinsey_MA50) OVER(ORDER BY date) THEN 'Death Cross'
            ELSE 'Hold'
        END AS Mckinsey_signal

    FROM moving_avg
),
quarterly_metrics AS (
    SELECT 
        'Min' AS metric,
        MIN(PWC) AS PWC,
        MIN(EY) AS EY,
        MIN(Deloitte) AS Deloitte,
        MIN(KPMG) AS KPMG,
        MIN(BCG) AS BCG,
        MIN(Bain) AS Bain,
        MIN(Mckinsey) AS Mckinsey
    FROM master_stocks
    WHERE QUARTER(date) = 1
    UNION ALL
    SELECT 'Max', MAX(PWC), MAX(EY), MAX(Deloitte), MAX(KPMG), MAX(BCG), MAX(Bain), MAX(Mckinsey)
    FROM master_stocks
    WHERE QUARTER(date) = 1
    UNION ALL
    SELECT 'Avg', ROUND(AVG(PWC),2), ROUND(AVG(EY),2), ROUND(AVG(Deloitte),2), ROUND(AVG(KPMG),2),
           ROUND(AVG(BCG),2), ROUND(AVG(Bain),2), ROUND(AVG(Mckinsey),2)
    FROM master_stocks
    WHERE QUARTER(date) = 1
    UNION ALL
    SELECT 'StdDev', ROUND(STDDEV(PWC),2), ROUND(STDDEV(EY),2), ROUND(STDDEV(Deloitte),2), ROUND(STDDEV(KPMG),2),
           ROUND(STDDEV(BCG),2), ROUND(STDDEV(Bain),2), ROUND(STDDEV(Mckinsey),2)
    FROM master_stocks
    WHERE QUARTER(date) = 1
    UNION ALL
    SELECT 'Signal', 
           (SELECT PWC_signal FROM signals ORDER BY date DESC LIMIT 1),
           (SELECT EY_signal FROM signals ORDER BY date DESC LIMIT 1),
           'Hold','Hold','Hold','Hold','Hold'
)
SELECT *, 
       (CASE 
            WHEN StdDev = LEAST(PWC, EY, Deloitte, KPMG, BCG, Bain, Mckinsey) THEN 'Most Stable'
            ELSE NULL
        END) AS most_stable_stock
FROM quarterly_metrics;

-- table for quarter 2
CREATE VIEW quarter1_summary AS
WITH moving_avg AS (
    SELECT
        date,
        PWC, EY, Deloitte, KPMG, BCG, Bain, Mckinsey,
        ROUND(AVG(PWC) OVER(ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),2) AS PWC_MA20,
        ROUND(AVG(PWC) OVER(ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),2) AS PWC_MA50,
        ROUND(AVG(EY) OVER(ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),2) AS EY_MA20,
        ROUND(AVG(EY) OVER(ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),2) AS EY_MA50,
        ROUND(AVG(Deloitte) OVER(ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),2) AS Deliotte_MA20,
        ROUND(AVG(Deloitte) OVER(ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),2) AS Deloitte_MA50,
        ROUND(AVG(Kpmg) OVER(ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),2) AS Kpmg_MA20,
        ROUND(AVG(Kpmg) OVER(ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),2) AS Kpmg_MA50,
        ROUND(AVG(Bcg) OVER(ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),2) AS Bcg_MA20,
        ROUND(AVG(Bcg) OVER(ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),2) AS Bcg_MA50,
        ROUND(AVG(Bain) OVER(ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),2) AS Bain_MA20,
        ROUND(AVG(Bain) OVER(ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),2) AS Bain_MA50,
        ROUND(AVG(Mckinsey) OVER(ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),2) AS Mckinsey_MA20,
        ROUND(AVG(Mckinsey) OVER(ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),2) AS Mckinsey_MA50
  
    FROM master_stocks
    WHERE QUARTER(date) = 2
),
signals AS (
    SELECT
        date,
        CASE
            WHEN PWC_MA20 > PWC_MA50 AND LAG(PWC_MA20) OVER(ORDER BY date) <= LAG(PWC_MA50) OVER(ORDER BY date) THEN 'Golden Cross'
            WHEN PWC_MA20 < PWC_MA50 AND LAG(PWC_MA20) OVER(ORDER BY date) >= LAG(PWC_MA50) OVER(ORDER BY date) THEN 'Death Cross'
            ELSE 'Hold'
        END AS PWC_signal,
        
        CASE
            WHEN EY_MA20 > EY_MA50 AND LAG(EY_MA20) OVER(ORDER BY date) <= LAG(EY_MA50) OVER(ORDER BY date) THEN 'Golden Cross'
            WHEN EY_MA20 < EY_MA50 AND LAG(EY_MA20) OVER(ORDER BY date) >= LAG(EY_MA50) OVER(ORDER BY date) THEN 'Death Cross'
            ELSE 'Hold'
        END AS EY_signal,

         CASE
            WHEN Deloitte_MA20 > Deloitte_MA50 AND LAG(Deloitte_MA20) OVER(ORDER BY date) <= LAG(Deloitte_MA50) OVER(ORDER BY date) THEN 'Golden Cross'
            WHEN Deloitte_MA20 < Deloitte_MA50 AND LAG(Deloitte_MA20) OVER(ORDER BY date) >= LAG(Deloitte_MA50) OVER(ORDER BY date) THEN 'Death Cross'
            ELSE 'Hold'
        END AS Deloitte_signal,

        CASE
            WHEN Kpmg_MA20 > Kpmg_MA50 AND LAG(Kpmg_MA20) OVER(ORDER BY date) <= LAG(Kpmg_MA50) OVER(ORDER BY date) THEN 'Golden Cross'
            WHEN Kpmg_MA20 < Kpmg_MA50 AND LAG(Kpmg_MA20) OVER(ORDER BY date) >= LAG(Kpmg_MA50) OVER(ORDER BY date) THEN 'Death Cross'
            ELSE 'Hold'
        END AS Kpmg_signal,

       CASE
            WHEN EY_MA20 > EY_MA50 AND LAG(EY_MA20) OVER(ORDER BY date) <= LAG(EY_MA50) OVER(ORDER BY date) THEN 'Golden Cross'
            WHEN EY_MA20 < EY_MA50 AND LAG(EY_MA20) OVER(ORDER BY date) >= LAG(EY_MA50) OVER(ORDER BY date) THEN 'Death Cross'
            ELSE 'Hold'
        END AS EY_signal,

        CASE
            WHEN Bcg_MA20 > Bcg_MA50 AND LAG(Bcg_MA20) OVER(ORDER BY date) <= LAG(Bcg_MA50) OVER(ORDER BY date) THEN 'Golden Cross'
            WHEN Bcg_MA20 < Bcg_MA50 AND LAG(Bcg_MA20) OVER(ORDER BY date) >= LAG(Bcg_MA50) OVER(ORDER BY date) THEN 'Death Cross'
            ELSE 'Hold'
        END AS Bcg_signal,
        
        CASE
            WHEN Bain_MA20 > Bain_MA50 AND LAG(Bain_MA20) OVER(ORDER BY date) <= LAG(Bain_MA50) OVER(ORDER BY date) THEN 'Golden Cross'
            WHEN Bain_MA20 < Bain_MA50 AND LAG(Bain_MA20) OVER(ORDER BY date) >= LAG(Bain_MA50) OVER(ORDER BY date) THEN 'Death Cross'
            ELSE 'Hold'
        END AS Bain_signal,

        CASE
            WHEN Mckinsey_MA20 > Mckinsey_MA50 AND LAG(Mckinsey_MA20) OVER(ORDER BY date) <= LAG(Mckinsey_MA50) OVER(ORDER BY date) THEN 'Golden Cross'
            WHEN Mckinsey_MA20 < Mckinsey_MA50 AND LAG(Mckinsey_MA20) OVER(ORDER BY date) >= LAG(Mckinsey_MA50) OVER(ORDER BY date) THEN 'Death Cross'
            ELSE 'Hold'
        END AS Mckinsey_signal

    FROM moving_avg
),
quarterly_metrics AS (
    SELECT 
        'Min' AS metric,
        MIN(PWC) AS PWC,
        MIN(EY) AS EY,
        MIN(Deloitte) AS Deloitte,
        MIN(KPMG) AS KPMG,
        MIN(BCG) AS BCG,
        MIN(Bain) AS Bain,
        MIN(Mckinsey) AS Mckinsey
    FROM master_stocks
    WHERE QUARTER(date) = 2
    UNION ALL
    SELECT 'Max', MAX(PWC), MAX(EY), MAX(Deloitte), MAX(KPMG), MAX(BCG), MAX(Bain), MAX(Mckinsey)
    FROM master_stocks
    WHERE QUARTER(date) = 2
    UNION ALL
    SELECT 'Avg', ROUND(AVG(PWC),2), ROUND(AVG(EY),2), ROUND(AVG(Deloitte),2), ROUND(AVG(KPMG),2),
           ROUND(AVG(BCG),2), ROUND(AVG(Bain),2), ROUND(AVG(Mckinsey),2)
    FROM master_stocks
    WHERE QUARTER(date) = 2
    UNION ALL
    SELECT 'StdDev', ROUND(STDDEV(PWC),2), ROUND(STDDEV(EY),2), ROUND(STDDEV(Deloitte),2), ROUND(STDDEV(KPMG),2),
           ROUND(STDDEV(BCG),2), ROUND(STDDEV(Bain),2), ROUND(STDDEV(Mckinsey),2)
    FROM master_stocks
    WHERE QUARTER(date) = 2
    UNION ALL
    SELECT 'Signal', 
           (SELECT PWC_signal FROM signals ORDER BY date DESC LIMIT 1),
           (SELECT EY_signal FROM signals ORDER BY date DESC LIMIT 1),
           'Hold','Hold','Hold','Hold','Hold'
)
SELECT *, 
       (CASE 
            WHEN StdDev = LEAST(PWC, EY, Deloitte, KPMG, BCG, Bain, Mckinsey) THEN 'Most Stable'
            ELSE NULL
        END) AS most_stable_stock
FROM quarterly_metrics;

--quarter 3 table
CREATE VIEW quarter1_summary AS
WITH moving_avg AS (
    SELECT
        date,
        PWC, EY, Deloitte, KPMG, BCG, Bain, Mckinsey,
        ROUND(AVG(PWC) OVER(ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),2) AS PWC_MA20,
        ROUND(AVG(PWC) OVER(ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),2) AS PWC_MA50,
        ROUND(AVG(EY) OVER(ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),2) AS EY_MA20,
        ROUND(AVG(EY) OVER(ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),2) AS EY_MA50,
        ROUND(AVG(Deloitte) OVER(ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),2) AS Deliotte_MA20,
        ROUND(AVG(Deloitte) OVER(ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),2) AS Deloitte_MA50,
        ROUND(AVG(Kpmg) OVER(ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),2) AS Kpmg_MA20,
        ROUND(AVG(Kpmg) OVER(ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),2) AS Kpmg_MA50,
        ROUND(AVG(Bcg) OVER(ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),2) AS Bcg_MA20,
        ROUND(AVG(Bcg) OVER(ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),2) AS Bcg_MA50,
        ROUND(AVG(Bain) OVER(ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),2) AS Bain_MA20,
        ROUND(AVG(Bain) OVER(ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),2) AS Bain_MA50,
        ROUND(AVG(Mckinsey) OVER(ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),2) AS Mckinsey_MA20,
        ROUND(AVG(Mckinsey) OVER(ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),2) AS Mckinsey_MA50
  
    FROM master_stocks
    WHERE QUARTER(date) = 3
),
signals AS (
    SELECT
        date,
        CASE
            WHEN PWC_MA20 > PWC_MA50 AND LAG(PWC_MA20) OVER(ORDER BY date) <= LAG(PWC_MA50) OVER(ORDER BY date) THEN 'Golden Cross'
            WHEN PWC_MA20 < PWC_MA50 AND LAG(PWC_MA20) OVER(ORDER BY date) >= LAG(PWC_MA50) OVER(ORDER BY date) THEN 'Death Cross'
            ELSE 'Hold'
        END AS PWC_signal,
        
        CASE
            WHEN EY_MA20 > EY_MA50 AND LAG(EY_MA20) OVER(ORDER BY date) <= LAG(EY_MA50) OVER(ORDER BY date) THEN 'Golden Cross'
            WHEN EY_MA20 < EY_MA50 AND LAG(EY_MA20) OVER(ORDER BY date) >= LAG(EY_MA50) OVER(ORDER BY date) THEN 'Death Cross'
            ELSE 'Hold'
        END AS EY_signal,

         CASE
            WHEN Deloitte_MA20 > Deloitte_MA50 AND LAG(Deloitte_MA20) OVER(ORDER BY date) <= LAG(Deloitte_MA50) OVER(ORDER BY date) THEN 'Golden Cross'
            WHEN Deloitte_MA20 < Deloitte_MA50 AND LAG(Deloitte_MA20) OVER(ORDER BY date) >= LAG(Deloitte_MA50) OVER(ORDER BY date) THEN 'Death Cross'
            ELSE 'Hold'
        END AS Deloitte_signal,

        CASE
            WHEN Kpmg_MA20 > Kpmg_MA50 AND LAG(Kpmg_MA20) OVER(ORDER BY date) <= LAG(Kpmg_MA50) OVER(ORDER BY date) THEN 'Golden Cross'
            WHEN Kpmg_MA20 < Kpmg_MA50 AND LAG(Kpmg_MA20) OVER(ORDER BY date) >= LAG(Kpmg_MA50) OVER(ORDER BY date) THEN 'Death Cross'
            ELSE 'Hold'
        END AS Kpmg_signal,

       CASE
            WHEN EY_MA20 > EY_MA50 AND LAG(EY_MA20) OVER(ORDER BY date) <= LAG(EY_MA50) OVER(ORDER BY date) THEN 'Golden Cross'
            WHEN EY_MA20 < EY_MA50 AND LAG(EY_MA20) OVER(ORDER BY date) >= LAG(EY_MA50) OVER(ORDER BY date) THEN 'Death Cross'
            ELSE 'Hold'
        END AS EY_signal,

        CASE
            WHEN Bcg_MA20 > Bcg_MA50 AND LAG(Bcg_MA20) OVER(ORDER BY date) <= LAG(Bcg_MA50) OVER(ORDER BY date) THEN 'Golden Cross'
            WHEN Bcg_MA20 < Bcg_MA50 AND LAG(Bcg_MA20) OVER(ORDER BY date) >= LAG(Bcg_MA50) OVER(ORDER BY date) THEN 'Death Cross'
            ELSE 'Hold'
        END AS Bcg_signal,
        
        CASE
            WHEN Bain_MA20 > Bain_MA50 AND LAG(Bain_MA20) OVER(ORDER BY date) <= LAG(Bain_MA50) OVER(ORDER BY date) THEN 'Golden Cross'
            WHEN Bain_MA20 < Bain_MA50 AND LAG(Bain_MA20) OVER(ORDER BY date) >= LAG(Bain_MA50) OVER(ORDER BY date) THEN 'Death Cross'
            ELSE 'Hold'
        END AS Bain_signal,

        CASE
            WHEN Mckinsey_MA20 > Mckinsey_MA50 AND LAG(Mckinsey_MA20) OVER(ORDER BY date) <= LAG(Mckinsey_MA50) OVER(ORDER BY date) THEN 'Golden Cross'
            WHEN Mckinsey_MA20 < Mckinsey_MA50 AND LAG(Mckinsey_MA20) OVER(ORDER BY date) >= LAG(Mckinsey_MA50) OVER(ORDER BY date) THEN 'Death Cross'
            ELSE 'Hold'
        END AS Mckinsey_signal

    FROM moving_avg
),
quarterly_metrics AS (
    SELECT 
        'Min' AS metric,
        MIN(PWC) AS PWC,
        MIN(EY) AS EY,
        MIN(Deloitte) AS Deloitte,
        MIN(KPMG) AS KPMG,
        MIN(BCG) AS BCG,
        MIN(Bain) AS Bain,
        MIN(Mckinsey) AS Mckinsey
    FROM master_stocks
    WHERE QUARTER(date) = 3
    UNION ALL
    SELECT 'Max', MAX(PWC), MAX(EY), MAX(Deloitte), MAX(KPMG), MAX(BCG), MAX(Bain), MAX(Mckinsey)
    FROM master_stocks
    WHERE QUARTER(date) = 3
    UNION ALL
    SELECT 'Avg', ROUND(AVG(PWC),2), ROUND(AVG(EY),2), ROUND(AVG(Deloitte),2), ROUND(AVG(KPMG),2),
           ROUND(AVG(BCG),2), ROUND(AVG(Bain),2), ROUND(AVG(Mckinsey),2)
    FROM master_stocks
    WHERE QUARTER(date) = 3
    UNION ALL
    SELECT 'StdDev', ROUND(STDDEV(PWC),2), ROUND(STDDEV(EY),2), ROUND(STDDEV(Deloitte),2), ROUND(STDDEV(KPMG),2),
           ROUND(STDDEV(BCG),2), ROUND(STDDEV(Bain),2), ROUND(STDDEV(Mckinsey),2)
    FROM master_stocks
    WHERE QUARTER(date) = 3
    UNION ALL
    SELECT 'Signal', 
           (SELECT PWC_signal FROM signals ORDER BY date DESC LIMIT 1),
           (SELECT EY_signal FROM signals ORDER BY date DESC LIMIT 1),
           'Hold','Hold','Hold','Hold','Hold'
)
SELECT *, 
       (CASE 
            WHEN StdDev = LEAST(PWC, EY, Deloitte, KPMG, BCG, Bain, Mckinsey) THEN 'Most Stable'
            ELSE NULL
        END) AS most_stable_stock
FROM quarterly_metrics;
