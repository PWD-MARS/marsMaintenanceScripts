drop materialized view if exists data.mat_level_data_quarter;
drop materialized view if exists data.mat_level_data_day;
drop materialized view if exists data.mat_gw_data_quarter;
drop materialized view if exists data.mat_gw_data_day;
drop materialized view if exists fieldwork.mat_expl_qa_deployment;
drop materialized view if exists data.mat_expl_qa_ow_leveldata;

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

CREATE MATERIALIZED VIEW IF NOT EXISTS fieldwork.mat_expl_qa_deployment
TABLESPACE pg_default
AS
 WITH recent_qa AS (
         SELECT min(mat_expl_qa_ow_leveldata.dtime) AS min_date,
            max(mat_expl_qa_ow_leveldata.dtime) AS max_date,
            mat_expl_qa_ow_leveldata.ow_uid,
            min(mat_expl_qa_ow_leveldata.date_added) AS date_added
           FROM data.mat_expl_qa_ow_leveldata
          GROUP BY mat_expl_qa_ow_leveldata.ow_uid
        )
 SELECT d.deployment_uid,
    d.deployment_dtime,
    d.ow_uid,
    o.smp_id,
    o.ow_suffix,
    d.inventory_sensors_uid,
    d.sensor_purpose,
    d.interval_min,
    d.collection_dtime,
    d.long_term_lookup_uid,
    d.research_lookup_uid,
    d.notes,
    d.download_error,
    d.deployment_dtw_or_depth_ft,
    d.collection_dtw_or_depth_ft,
    d.premonitoring_inspection_date,
    d.ready,
    q.min_date,
    q.max_date,
    q.date_added
   FROM fieldwork.tbl_deployment d
     JOIN recent_qa q ON d.ow_uid = q.ow_uid
     JOIN fieldwork.tbl_ow o ON d.ow_uid = o.ow_uid
  WHERE d.collection_dtime >= q.min_date AND d.collection_dtime <= q.max_date
WITH DATA;