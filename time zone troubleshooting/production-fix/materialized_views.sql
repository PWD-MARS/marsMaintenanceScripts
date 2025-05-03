CREATE MATERIALIZED VIEW IF NOT EXISTS data.mat_expl_qa_ow_leveldata
TABLESPACE pg_default
AS
 SELECT tbl_ow_leveldata_raw.ow_leveldata_raw_uid,
    tbl_ow_leveldata_raw.dtime,
    tbl_ow_leveldata_raw.level_ft,
    tbl_ow_leveldata_raw.ow_uid,
    tbl_ow_leveldata_raw.date_added
   FROM data.tbl_ow_leveldata_raw
  WHERE data.fun_date_to_fiscal_quarter(tbl_ow_leveldata_raw.date_added) = data.fun_date_to_fiscal_quarter(CURRENT_DATE)
WITH DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS data.mat_gw_data_day
TABLESPACE pg_default
AS
 SELECT DISTINCT tbl_gw_depthdata_raw.ow_uid,
    tbl_gw_depthdata_raw.dtime::date AS gw_data_day
   FROM data.tbl_gw_depthdata_raw
  ORDER BY tbl_gw_depthdata_raw.ow_uid, (tbl_gw_depthdata_raw.dtime::date)
WITH DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS data.mat_gw_data_quarter
TABLESPACE pg_default
AS
 SELECT DISTINCT tbl_gw_depthdata_raw.ow_uid,
    data.fun_date_to_fiscal_quarter(tbl_gw_depthdata_raw.dtime::date) AS gw_data_quarter
   FROM data.tbl_gw_depthdata_raw
WITH DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS data.mat_level_data_day
TABLESPACE pg_default
AS
 SELECT DISTINCT tbl_ow_leveldata_raw.ow_uid,
    tbl_ow_leveldata_raw.dtime::date AS level_data_day
   FROM data.tbl_ow_leveldata_raw
  ORDER BY tbl_ow_leveldata_raw.ow_uid, (tbl_ow_leveldata_raw.dtime::date)
WITH DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS data.mat_level_data_quarter
TABLESPACE pg_default
AS
 SELECT DISTINCT tbl_ow_leveldata_raw.ow_uid,
    data.fun_date_to_fiscal_quarter(tbl_ow_leveldata_raw.dtime::date) AS level_data_quarter
   FROM data.tbl_ow_leveldata_raw
WITH DATA;

