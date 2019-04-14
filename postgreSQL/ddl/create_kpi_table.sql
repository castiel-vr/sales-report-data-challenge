CREATE DATABASE main_db;
\connect main_db;
CREATE TABLE sales_kpi (
 period_type CHAR(1) NOT NULL,
 period_id VARCHAR(8) NOT NULL,
 sales_sum INTEGER,
 sales_by_region TEXT,
 sales_by_area TEXT,
 sales_by_market TEXT,
 PRIMARY KEY(period_type, period_id)
);
