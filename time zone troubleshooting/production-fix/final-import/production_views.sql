-- drop VIEW data.test_viw_gage_event_latestdates;
-- drop VIEW data.test_viw_gage_rain_latestdates;
-- drop VIEW data.test_viw_gage_rainfall;
-- drop VIEW data.test_viw_gwdata_latestdates;
-- drop VIEW data.test_viw_ow_leveldata_sumpcorrected;
-- drop VIEW data.test_viw_owdata_earliestdates;
-- drop VIEW data.test_viw_owdata_latestdates;
-- drop VIEW data.test_viw_radar_event_latestdates;
-- drop VIEW data.test_viw_radar_rain_latestdates;
-- drop VIEW data.test_viw_radar_rainfall;
-- drop VIEW data.test_viw_barodata_neighbors;
-- drop VIEW data.test_viw_barodata_smp;

-- Gage rainfall

CREATE OR REPLACE VIEW data.test_viw_gage_event_latestdates
AS
SELECT rge.gage_uid,
max(rge.eventdataend_local) AS dtime_local,
max(rge.eventdataend_local) AS dtime_edt -- Shim field to transparently serve updated data to end user without updating app first. To be removed when app is updated to target _local field
FROM data.test_tbl_gage_event rge
GROUP BY rge.gage_uid
ORDER BY rge.gage_uid;

CREATE OR REPLACE VIEW data.test_viw_gage_rain_latestdates
AS
SELECT max(rg.dtime_local) AS maxtime_local,
max(rg.dtime_local) AS maxtime_edt, -- Shim field to transparently serve updated data to end user without updating app first. To be removed when app is updated to target _local field
rg.gage_uid
FROM data.test_tbl_gage_rain rg
GROUP BY rg.gage_uid
ORDER BY rg.gage_uid;

CREATE OR REPLACE VIEW data.test_viw_gage_rainfall
AS
SELECT rg.gage_rain_uid,
rg.dtime_local,
rg.gage_uid,
rg.rainfall_in,
rg.dtime_local as dtime_edt, -- Shim field to transparently serve updated data to end user without updating app first. To be removed when app is updated to target _local field
rge.gage_event_uid
FROM data.test_tbl_gage_rain rg
LEFT JOIN data.test_tbl_gage_event rge ON rg.gage_uid = rge.gage_uid AND rg.dtime_local >= rge.eventdatastart_local AND rg.dtime_local <= rge.eventdataend_local;

-- GW and OW data

CREATE OR REPLACE VIEW data.test_viw_gwdata_latestdates
AS
SELECT k.dtime_local,
ow.ow_uid,
ow.smp_id,
ow.ow_suffix, 
k.dtime_local as dtime_est -- Shim field to transparently serve updated data to end user without updating app first. To be removed when app is updated to target _local field
FROM ( SELECT max(od.dtime_local) AS dtime_local,
       od.ow_uid
       FROM data.test_tbl_gw_depthdata_raw od
       GROUP BY od.ow_uid) k
FULL JOIN fieldwork.tbl_ow ow ON k.ow_uid = ow.ow_uid
WHERE ow.ow_suffix ~~ 'GW%'::text OR ow.ow_suffix ~~ 'CW%'::text
ORDER BY ow.smp_id, ow.ow_suffix;

CREATE OR REPLACE VIEW data.test_viw_ow_leveldata_sumpcorrected
AS
SELECT od.ow_leveldata_raw_uid,
od.dtime_local,
GREATEST(round(od.level_ft - osd.sumpdepth_ft, 4), 0::numeric) AS level_ft,
ow.smp_id,
ow.ow_suffix,
ow.ow_uid,
od.dtime_local as dtime_est -- Shim field to transparently serve updated data to end user without updating app first. To be removed when app is updated to target _local field
FROM data.test_tbl_ow_leveldata_raw od
LEFT JOIN fieldwork.tbl_ow ow ON od.ow_uid = ow.ow_uid
LEFT JOIN fieldwork.viw_ow_sumpdepth osd ON od.ow_uid = osd.ow_uid;

CREATE OR REPLACE VIEW data.test_viw_owdata_earliestdates
AS
WITH ow_uid_earliestdate AS (
  SELECT min(od.dtime_local) AS dtime_local,
  od.ow_uid,
  data.fun_date_to_fiscal_quarter(min(od.dtime_local)::date) AS fiscal_quarter
  FROM data.test_tbl_ow_leveldata_raw od
  WHERE od.dtime_local IS NOT NULL
  GROUP BY od.ow_uid
), non_groundwater_wells AS (
  SELECT tbl_ow.ow_uid,
  tbl_ow.smp_id,
  tbl_ow.ow_suffix,
  tbl_ow.facility_id,
  tbl_ow.site_name_lookup_uid
  FROM fieldwork.tbl_ow
  WHERE tbl_ow.ow_suffix !~~ 'GW%'::text AND tbl_ow.ow_suffix !~~ 'CW%'::text AND tbl_ow.smp_id IS NOT NULL
  ORDER BY tbl_ow.smp_id, tbl_ow.ow_suffix
)
SELECT ngw.ow_uid,
ngw.smp_id,
ngw.ow_suffix,
admin.fun_smp_to_system(ngw.smp_id) AS system_id,
k.dtime_local::date AS earliest_data_date,
k.fiscal_quarter,
fq.fiscal_quarter_lookup_uid
FROM ow_uid_earliestdate k
LEFT JOIN non_groundwater_wells ngw ON k.ow_uid = ngw.ow_uid
LEFT JOIN admin.tbl_fiscal_quarter_lookup fq ON fq.fiscal_quarter = k.fiscal_quarter;

CREATE OR REPLACE VIEW data.test_viw_owdata_latestdates
AS
SELECT k.dtime_local,
ow.ow_uid,
ow.smp_id,
ow.ow_suffix,
k.dtime_local as dtime_est -- Shim field to transparently serve updated data to end user without updating app first. To be removed when app is updated to target _local field
FROM ( SELECT max(od.dtime_local) AS dtime_local,
       od.ow_uid
       FROM data.test_tbl_ow_leveldata_raw od
       GROUP BY od.ow_uid) k
FULL JOIN fieldwork.tbl_ow ow ON k.ow_uid = ow.ow_uid
WHERE ow.ow_suffix !~~ 'GW%'::text AND ow.ow_suffix !~~ 'CW%'::text
ORDER BY ow.smp_id, ow.ow_suffix;

-- Radar Rainfall

CREATE OR REPLACE VIEW data.test_viw_radar_event_latestdates
AS
SELECT rce.radar_uid,
max(rce.eventdataend_local) AS dtime_local,
max(rce.eventdataend_local) AS dtime_edt -- Shim field to transparently serve updated data to end user without updating app first. To be removed when app is updated to target _local field
FROM data.test_tbl_radar_event rce
GROUP BY rce.radar_uid
ORDER BY rce.radar_uid;

CREATE OR REPLACE VIEW data.test_viw_radar_rain_latestdates
AS
SELECT max(rg.dtime_local) AS maxtime_local,
max(rg.dtime_local) AS maxtime_edt,-- Shim field to transparently serve updated data to end user without updating app first. To be removed when app is updated to target _local field
rg.radar_uid
FROM data.test_tbl_radar_rain rg
GROUP BY rg.radar_uid
ORDER BY rg.radar_uid;

CREATE OR REPLACE VIEW data.test_viw_radar_rainfall
AS
SELECT rc.radar_rain_uid,
rc.dtime_local,
rc.radar_uid,
rc.rainfall_in,
rc.dtime_local as dtime_edt, -- Shim field to transparently serve updated data to end user without updating app first. To be removed when app is updated to target _local field
rce.radar_event_uid
FROM data.test_tbl_radar_rain rc
LEFT JOIN data.test_tbl_radar_event rce ON rc.radar_uid = rce.radar_uid AND rc.dtime_local >= rce.eventdatastart_local AND rc.dtime_local <= rce.eventdataend_local;

-- Baro data

CREATE OR REPLACE VIEW data.test_viw_barodata_smp
 AS
 SELECT b.dtime_local,
    r.smp_id,
    b.baro_psi,
    b.temp_f,
    b.dtime_est -- Shim field to transparently serve updated data to end user without updating app first. To be removed when app is updated to target _local field
   FROM data.test_tbl_baro b
     LEFT JOIN admin.tbl_baro_rawfile r ON b.baro_rawfile_uid = r.baro_rawfile_uid;

CREATE OR REPLACE VIEW data.test_viw_barodata_neighbors
 AS
 SELECT b.dtime_local,
    b.dtime_local as dtime_est, -- Shim field to transparently serve updated data to end user without updating app first. To be removed when app is updated to target _local field
    count(*) AS neighbors
   FROM data.test_viw_barodata_smp b
  GROUP BY b.dtime_local;
