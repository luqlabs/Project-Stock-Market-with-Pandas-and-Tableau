-- Created by GitHub Copilot in SSMS - review carefully before executing
/*
  Unified stock dataset and analytical queries for Power BI / Tableau.
  Combines company tables into a single view and provides KPI, comparison,
  trend, distribution, and moving-average queries.
*/

--------------------------------------------------------------------------------
-- A. UNIFIED STOCK DATASET
--------------------------------------------------------------------------------
CREATE OR ALTER VIEW dbo.vw_Stock_Combined
AS
SELECT
    CAST([Date] AS date)                              AS trade_date,
    CAST([Open] AS float)                             AS open_price,
    CAST([Close] AS float)                            AS close_price,
    CAST([High] AS float)                             AS high_price,
    CAST([Low] AS float)                              AS low_price,
    CAST([Volume] AS bigint)                          AS volume,
    N'Apple'                                          AS company
FROM dbo.Apple
UNION ALL
SELECT
    CAST([Date] AS date),
    CAST([Open] AS float),
    CAST([Close] AS float),
    CAST([High] AS float),
    CAST([Low] AS float),
    CAST([Volume] AS bigint),
    N'Facebook'
FROM dbo.Facebook
UNION ALL
SELECT
    CAST([Date] AS date),
    CAST([Open] AS float),
    CAST([Close] AS float),
    CAST([High] AS float),
    CAST([Low] AS float),
    CAST([Volume] AS bigint),
    N'Google'
FROM dbo.Google
UNION ALL
SELECT
    CAST([Date] AS date),
    CAST([Open] AS float),
    CAST([Close] AS float),
    CAST([High] AS float),
    CAST([Low] AS float),
    CAST([Volume] AS bigint),
    N'Nvidia'
FROM dbo.Nvidia
UNION ALL
SELECT
    CAST([Date] AS date),
    CAST([Open] AS float),
    CAST([Close] AS float),
    CAST([High] AS float),
    CAST([Low] AS float),
    CAST([Volume] AS bigint),
    N'Tesla'
FROM dbo.Tesla
UNION ALL
SELECT
    CAST([Date] AS date),
    CAST([Open] AS float),
    CAST([Close] AS float),
    CAST([High] AS float),
    CAST([Low] AS float),
    CAST([Volume] AS bigint),
    N'Twitter'
FROM dbo.Twitter;
GO

--------------------------------------------------------------------------------
-- B. KPI CARDS
-- Parameters: @end_date = last available trade_date, @start_date = rolling window (default 30 days)
--------------------------------------------------------------------------------
DECLARE @end_date date = (SELECT MAX(trade_date) FROM dbo.vw_Stock_Combined);
DECLARE @start_date date = DATEADD(day, -30, @end_date);

-- 1) Last available trading date
SELECT @end_date AS last_trade_date;

-- 2) Last day total trading volume (all companies)
SELECT SUM(volume) AS last_day_total_volume
FROM dbo.vw_Stock_Combined
WHERE trade_date = @end_date;

-- 3) Lowest price in selected period (rolling window)
SELECT MIN(low_price) AS lowest_price_in_period
FROM dbo.vw_Stock_Combined
WHERE trade_date BETWEEN @start_date AND @end_date;

-- 4) Highest price in selected period (rolling window)
SELECT MAX(high_price) AS highest_price_in_period
FROM dbo.vw_Stock_Combined
WHERE trade_date BETWEEN @start_date AND @end_date;

-- 5) Total trading volume in selected period (rolling window)
SELECT SUM(volume) AS total_volume_in_period
FROM dbo.vw_Stock_Combined
WHERE trade_date BETWEEN @start_date AND @end_date;
GO

--------------------------------------------------------------------------------
-- C. DAILY COMPARISON TABLE (last trading day per company)
-- Columns: close today, close prev day, change, % change, volume today, prev volume, change, % change
--------------------------------------------------------------------------------
;WITH Ranked AS (
    SELECT
        company,
        trade_date,
        close_price,
        volume,
        LAG(close_price) OVER (PARTITION BY company ORDER BY trade_date)     AS prev_close_price,
        LAG(volume) OVER (PARTITION BY company ORDER BY trade_date)          AS prev_volume,
        ROW_NUMBER() OVER (PARTITION BY company ORDER BY trade_date DESC)    AS rn
    FROM dbo.vw_Stock_Combined
)
SELECT
    company,
    trade_date,
    close_price                                          AS close_price_today,
    prev_close_price                                     AS close_price_prev_day,
    ROUND(CAST(close_price - prev_close_price AS float), 4)           AS price_change,
    CASE
        WHEN prev_close_price IS NULL OR prev_close_price = 0 THEN NULL
        ELSE ROUND(CAST((close_price - prev_close_price) / prev_close_price * 100.0 AS numeric(9,4)), 2)
    END                                                   AS price_change_pct,
    volume                                               AS volume_today,
    prev_volume                                          AS volume_prev_day,
    (volume - prev_volume)                               AS volume_change,
    CASE
        WHEN prev_volume IS NULL OR prev_volume = 0 THEN NULL
        ELSE ROUND(CAST((volume - prev_volume) / CAST(prev_volume AS float) * 100.0 AS numeric(9,4)), 2)
    END                                                   AS volume_change_pct
FROM Ranked
WHERE rn = 1
ORDER BY company;
GO

--------------------------------------------------------------------------------
-- D. VOLUME TREND
-- Daily trading volume per company (Power BI / Tableau ready)
--------------------------------------------------------------------------------
SELECT
    trade_date,
    company,
    volume
FROM dbo.vw_Stock_Combined
ORDER BY trade_date, company;
GO

--------------------------------------------------------------------------------
-- E. PRICE CHANGE DISTRIBUTION
-- Daily % change = (close - prev_close) / prev_close * 100
--------------------------------------------------------------------------------
SELECT
    trade_date,
    company,
    close_price,
    prev_close,
    ROUND(pct_change, 2) AS pct_change
FROM (
    SELECT
        trade_date,
        company,
        close_price,
        LAG(close_price) OVER (PARTITION BY company ORDER BY trade_date) AS prev_close,
        CASE
            WHEN LAG(close_price) OVER (PARTITION BY company ORDER BY trade_date) IS NULL THEN NULL
            WHEN LAG(close_price) OVER (PARTITION BY company ORDER BY trade_date) = 0 THEN NULL
            ELSE (close_price - LAG(close_price) OVER (PARTITION BY company ORDER BY trade_date))
                 / LAG(close_price) OVER (PARTITION BY company ORDER BY trade_date) * 100.0
        END AS pct_change
    FROM dbo.vw_Stock_Combined
) t
ORDER BY company, trade_date;
GO

--------------------------------------------------------------------------------
-- F. MOVING AVERAGE ANALYSIS
-- Open price with 50-day and 200-day moving averages (per company)
--------------------------------------------------------------------------------
SELECT
    trade_date,
    company,
    open_price,
    ROUND(AVG(open_price) OVER (PARTITION BY company ORDER BY trade_date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW), 4)  AS ma50_open,
    ROUND(AVG(open_price) OVER (PARTITION BY company ORDER BY trade_date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW), 4) AS ma200_open
FROM dbo.vw_Stock_Combined
ORDER BY company, trade_date;
GO