drop VIEW fieldwork.viw_previous_deployments;
drop VIEW fieldwork.viw_inventory_sensors_full;
drop VIEW fieldwork.viw_unmonitored_postcon_on;
drop VIEW fieldwork.viw_unmonitored_both_on;
drop VIEW fieldwork.viw_unmonitored_both_off;
drop VIEW fieldwork.viw_unmonitored_future_on;
drop VIEW fieldwork.viw_postcon_cwl_smp;
drop VIEW fieldwork.viw_previous_cwl_sites;
drop VIEW fieldwork.viw_active_cwl_sites;
drop VIEW fieldwork.viw_deployment_full_cwl_with_baro;
drop VIEW fieldwork.viw_active_deployments;
drop VIEW fieldwork.viw_first_deployment_cwl;
drop VIEW fieldwork.viw_unmonitored_active_smps;
drop VIEW fieldwork.viw_qaqc_deployments;
drop VIEW fieldwork.viw_deployment_full_cwl;
drop VIEW fieldwork.viw_deployment_full;

alter table fieldwork.tbl_deployment rename dtime_est to dtime;

CREATE OR REPLACE VIEW fieldwork.viw_deployment_full
 AS
 SELECT fd.deployment_uid,
    fd.deployment_dtime,
    fow.smp_id,
    fow.ow_suffix,
    pn.project_name AS greenit_name,
    fd.ow_uid,
    fd.sensor_purpose,
    de.type,
    fd.long_term_lookup_uid,
    lt.type AS term,
    fd.research_lookup_uid,
    rs.type AS research,
    fd.interval_min,
    own.public,
    pr."Designation" AS designation,
    pr."OOWProgramType" AS oow_program_type,
    pr."SMIP" AS smip,
    pr."GARP" AS garp,
    fd.inventory_sensors_uid,
    inv.sensor_serial,
    fd.collection_dtime,
    fd.notes,
    fd.download_error,
    lag(fd.download_error, 1) OVER (PARTITION BY fd.ow_uid, inv.sensor_serial ORDER BY fd.deployment_dtime) AS previous_download_error,
    fow.site_name_lookup_uid,
    snl.site_name,
    COALESCE(pn.project_name, snl.site_name) AS project_name,
    sfc.component_id,
    fd.deployment_dtw_or_depth_ft,
    fd.collection_dtw_or_depth_ft,
    fd.premonitoring_inspection_date,
    fd.ready
   FROM fieldwork.tbl_deployment fd
     LEFT JOIN fieldwork.tbl_ow fow ON fd.ow_uid = fow.ow_uid
     LEFT JOIN fieldwork.tbl_inventory_sensors inv ON fd.inventory_sensors_uid = inv.inventory_sensors_uid
     LEFT JOIN fieldwork.tbl_sensor_purpose_lookup de ON fd.sensor_purpose = de.sensor_purpose_lookup_uid
     LEFT JOIN fieldwork.tbl_long_term_lookup lt ON lt.long_term_lookup_uid = fd.long_term_lookup_uid
     LEFT JOIN fieldwork.tbl_research_lookup rs ON rs.research_lookup_uid = fd.research_lookup_uid
     LEFT JOIN fieldwork.viw_ow_ownership own ON fd.ow_uid = own.ow_uid
     LEFT JOIN external.tbl_planreview_private pr ON fow.smp_id = pr."SMPID"
     LEFT JOIN fieldwork.viw_project_names pn ON fow.smp_id = pn.smp_id
     LEFT JOIN fieldwork.tbl_site_name_lookup snl ON fow.site_name_lookup_uid = snl.site_name_lookup_uid
     LEFT JOIN external.mat_assets sfc ON fow.facility_id = sfc.facility_id;

CREATE OR REPLACE VIEW fieldwork.viw_deployment_full_cwl
 AS
 SELECT viw_deployment_full.deployment_uid,
    viw_deployment_full.deployment_dtime,
    viw_deployment_full.smp_id,
    viw_deployment_full.ow_suffix,
    viw_deployment_full.greenit_name,
    viw_deployment_full.ow_uid,
    viw_deployment_full.sensor_purpose,
    viw_deployment_full.type,
    viw_deployment_full.long_term_lookup_uid,
    viw_deployment_full.term,
    viw_deployment_full.research_lookup_uid,
    viw_deployment_full.research,
    viw_deployment_full.interval_min,
    viw_deployment_full.public,
    viw_deployment_full.designation,
    viw_deployment_full.oow_program_type,
    viw_deployment_full.smip,
    viw_deployment_full.garp,
    viw_deployment_full.inventory_sensors_uid,
    viw_deployment_full.sensor_serial,
    viw_deployment_full.collection_dtime,
    viw_deployment_full.notes,
    viw_deployment_full.download_error,
    viw_deployment_full.previous_download_error,
    viw_deployment_full.site_name_lookup_uid,
    viw_deployment_full.site_name,
    viw_deployment_full.project_name,
    viw_deployment_full.component_id,
    viw_deployment_full.deployment_dtw_or_depth_ft,
    viw_deployment_full.collection_dtw_or_depth_ft,
    viw_deployment_full.premonitoring_inspection_date,
    viw_deployment_full.ready
   FROM fieldwork.viw_deployment_full
  WHERE ((viw_deployment_full.term = ANY (ARRAY['Short'::text, 'Long'::text, 'NA'::text])) OR viw_deployment_full.term IS NULL) AND viw_deployment_full.type = 'LEVEL'::text;

CREATE OR REPLACE VIEW fieldwork.viw_first_deployment_cwl
 AS
 SELECT DISTINCT admin.fun_smp_to_system(d.smp_id) AS system_id,
    min(d.deployment_dtime) AS first_deployment,
    d.public
   FROM fieldwork.viw_deployment_full_cwl d
  GROUP BY (admin.fun_smp_to_system(d.smp_id)), d.public
  ORDER BY (admin.fun_smp_to_system(d.smp_id));

CREATE OR REPLACE VIEW fieldwork.viw_active_deployments
 AS
 SELECT viw_deployment_full.deployment_uid,
    viw_deployment_full.deployment_dtime,
    viw_deployment_full.smp_id,
    viw_deployment_full.ow_suffix,
    viw_deployment_full.project_name,
    viw_deployment_full.ow_uid,
    viw_deployment_full.sensor_purpose,
    viw_deployment_full.type,
    viw_deployment_full.long_term_lookup_uid,
    viw_deployment_full.term,
    viw_deployment_full.research_lookup_uid,
    viw_deployment_full.research,
    viw_deployment_full.interval_min,
    viw_deployment_full.public,
    viw_deployment_full.designation,
    viw_deployment_full.oow_program_type,
    viw_deployment_full.smip,
    viw_deployment_full.garp,
        CASE
            WHEN viw_deployment_full.term = 'SRT'::text THEN viw_deployment_full.deployment_dtime + '5 days'::interval
            ELSE viw_deployment_full.deployment_dtime + viw_deployment_full.interval_min::double precision * '12 days'::interval
        END AS date_80percent,
        CASE
            WHEN viw_deployment_full.term = 'SRT'::text THEN viw_deployment_full.deployment_dtime + '7 days'::interval
            ELSE viw_deployment_full.deployment_dtime + viw_deployment_full.interval_min::double precision * '15 days'::interval
        END AS date_100percent,
    viw_deployment_full.inventory_sensors_uid,
    viw_deployment_full.sensor_serial,
    date_part('month'::text, viw_deployment_full.deployment_dtime) AS collection_month,
    date_part('year'::text, viw_deployment_full.deployment_dtime) AS collection_year,
    viw_deployment_full.notes,
    viw_deployment_full.download_error,
    viw_deployment_full.previous_download_error,
    viw_deployment_full.site_name_lookup_uid,
    viw_deployment_full.site_name,
    viw_deployment_full.component_id,
    viw_deployment_full.deployment_dtw_or_depth_ft,
    viw_deployment_full.premonitoring_inspection_date,
    viw_deployment_full.ready
   FROM fieldwork.viw_deployment_full
  WHERE viw_deployment_full.collection_dtime IS NULL AND (viw_deployment_full.smp_id IS NOT NULL OR viw_deployment_full.site_name_lookup_uid IS NOT NULL)
  ORDER BY viw_deployment_full.smp_id, viw_deployment_full.ow_suffix, viw_deployment_full.sensor_purpose, viw_deployment_full.deployment_dtime;

CREATE OR REPLACE VIEW fieldwork.viw_deployment_full_cwl_with_baro
 AS
 SELECT viw_deployment_full.deployment_uid,
    viw_deployment_full.deployment_dtime,
    viw_deployment_full.smp_id,
    viw_deployment_full.ow_suffix,
    viw_deployment_full.greenit_name,
    viw_deployment_full.ow_uid,
    viw_deployment_full.sensor_purpose,
    viw_deployment_full.type,
    viw_deployment_full.long_term_lookup_uid,
    viw_deployment_full.term,
    viw_deployment_full.research_lookup_uid,
    viw_deployment_full.research,
    viw_deployment_full.interval_min,
    viw_deployment_full.public,
    viw_deployment_full.designation,
    viw_deployment_full.oow_program_type,
    viw_deployment_full.smip,
    viw_deployment_full.garp,
    viw_deployment_full.inventory_sensors_uid,
    viw_deployment_full.sensor_serial,
    viw_deployment_full.collection_dtime,
    viw_deployment_full.notes,
    viw_deployment_full.download_error,
    viw_deployment_full.previous_download_error,
    viw_deployment_full.site_name_lookup_uid,
    viw_deployment_full.site_name,
    viw_deployment_full.project_name,
    viw_deployment_full.component_id,
    viw_deployment_full.deployment_dtw_or_depth_ft,
    viw_deployment_full.collection_dtw_or_depth_ft
   FROM fieldwork.viw_deployment_full
  WHERE ((viw_deployment_full.term = ANY (ARRAY['Short'::text, 'Long'::text, 'NA'::text])) OR viw_deployment_full.term IS NULL) AND (viw_deployment_full.type = ANY (ARRAY['LEVEL'::text, 'BARO'::text]));


CREATE OR REPLACE VIEW fieldwork.viw_active_cwl_sites
 AS
 SELECT DISTINCT dfc.smp_id,
    dfc.ow_suffix,
    dfc.site_name,
    dfc.component_id,
    dfc.project_name,
    dfc.public,
    dfc.type,
    min(df.deployment_dtime) AS first_deployment_date,
    "left"(dfc.ow_suffix, 2) AS location_type
   FROM fieldwork.viw_deployment_full_cwl_with_baro dfc
     LEFT JOIN fieldwork.viw_deployment_full df ON dfc.ow_uid = df.ow_uid
  WHERE dfc.collection_dtime IS NULL
  GROUP BY dfc.smp_id, dfc.ow_suffix, dfc.site_name, dfc.component_id, dfc.project_name, dfc.public, dfc.type;

CREATE OR REPLACE VIEW fieldwork.viw_previous_cwl_sites
 AS
 SELECT DISTINCT dfc.smp_id,
    dfc.ow_suffix,
    dfc.site_name,
    dfc.component_id,
    dfc.project_name,
    dfc.public,
    dfc.type,
    "left"(dfc.ow_suffix, 2) AS location_type,
    min(dfc.deployment_dtime) AS first_deployment_date,
    max(dfc.collection_dtime) AS last_collection_date
   FROM fieldwork.viw_deployment_full_cwl_with_baro dfc
     LEFT JOIN fieldwork.viw_active_cwl_sites ac ON dfc.smp_id = ac.smp_id
  WHERE ac.smp_id IS NULL
  GROUP BY dfc.smp_id, dfc.ow_suffix, dfc.site_name, dfc.component_id, dfc.project_name, dfc.public, dfc.type;


CREATE OR REPLACE VIEW fieldwork.viw_postcon_cwl_smp
 AS
 WITH srt_systems AS (
         SELECT DISTINCT viw_srt_full.srt_uid,
            viw_srt_full.system_id,
            viw_srt_full.test_date,
            viw_srt_full.phase,
            viw_srt_full.type,
            viw_srt_full.srt_volume_ft3,
            viw_srt_full.dcia_ft2,
            viw_srt_full.srt_stormsize_in,
            viw_srt_full.flow_data_recorded,
            viw_srt_full.water_level_recorded,
            viw_srt_full.photos_uploaded,
            viw_srt_full.sensor_collection_date,
            viw_srt_full.qaqc_complete,
            viw_srt_full.srt_summary_date,
            viw_srt_full.turnaround_days,
            viw_srt_full.srt_summary,
            viw_srt_full.project_name,
            viw_srt_full.sensor_deployed,
            viw_srt_full.public
           FROM fieldwork.viw_srt_full
          WHERE viw_srt_full.phase <> 'Post-Construction'::text
        ), fieldwork_deployment_full AS (
         SELECT viw_deployment_full.deployment_uid,
            viw_deployment_full.deployment_dtime,
            viw_deployment_full.smp_id,
            viw_deployment_full.ow_suffix,
            viw_deployment_full.greenit_name,
            viw_deployment_full.ow_uid,
            viw_deployment_full.sensor_purpose,
            viw_deployment_full.type,
            viw_deployment_full.long_term_lookup_uid,
            viw_deployment_full.term,
            viw_deployment_full.research_lookup_uid,
            viw_deployment_full.research,
            viw_deployment_full.interval_min,
            viw_deployment_full.public,
            viw_deployment_full.designation,
            viw_deployment_full.oow_program_type,
            viw_deployment_full.smip,
            viw_deployment_full.garp,
            viw_deployment_full.inventory_sensors_uid,
            viw_deployment_full.sensor_serial,
            viw_deployment_full.collection_dtime,
            viw_deployment_full.notes,
            viw_deployment_full.download_error,
            viw_deployment_full.previous_download_error,
            viw_deployment_full.site_name_lookup_uid,
            viw_deployment_full.site_name,
            viw_deployment_full.project_name,
            viw_deployment_full.component_id,
            viw_deployment_full.deployment_dtw_or_depth_ft,
            viw_deployment_full.collection_dtw_or_depth_ft,
            viw_deployment_full.premonitoring_inspection_date,
            viw_deployment_full.ready,
            admin.fun_smp_to_system(viw_deployment_full.smp_id) AS system_id
           FROM fieldwork.viw_deployment_full
        ), cwl_smp AS (
         SELECT DISTINCT viw_deployment_full_cwl.deployment_uid,
            viw_deployment_full_cwl.deployment_dtime,
            viw_deployment_full_cwl.smp_id,
            viw_deployment_full_cwl.ow_suffix,
            viw_deployment_full_cwl.greenit_name,
            viw_deployment_full_cwl.ow_uid,
            viw_deployment_full_cwl.sensor_purpose,
            viw_deployment_full_cwl.type,
            viw_deployment_full_cwl.long_term_lookup_uid,
            viw_deployment_full_cwl.term,
            viw_deployment_full_cwl.research_lookup_uid,
            viw_deployment_full_cwl.research,
            viw_deployment_full_cwl.interval_min,
            viw_deployment_full_cwl.public,
            viw_deployment_full_cwl.designation,
            viw_deployment_full_cwl.oow_program_type,
            viw_deployment_full_cwl.smip,
            viw_deployment_full_cwl.garp,
            viw_deployment_full_cwl.inventory_sensors_uid,
            viw_deployment_full_cwl.sensor_serial,
            viw_deployment_full_cwl.collection_dtime,
            viw_deployment_full_cwl.notes,
            viw_deployment_full_cwl.download_error,
            viw_deployment_full_cwl.previous_download_error,
            viw_deployment_full_cwl.site_name_lookup_uid,
            viw_deployment_full_cwl.site_name,
            viw_deployment_full_cwl.project_name,
            viw_deployment_full_cwl.component_id,
            viw_deployment_full_cwl.deployment_dtw_or_depth_ft,
            viw_deployment_full_cwl.collection_dtw_or_depth_ft,
            viw_deployment_full_cwl.premonitoring_inspection_date,
            viw_deployment_full_cwl.ready
           FROM fieldwork.viw_deployment_full_cwl
        ), special_cases AS (
         SELECT fieldwork_deployment_full.deployment_uid,
            fieldwork_deployment_full.deployment_dtime,
            fieldwork_deployment_full.smp_id,
            fieldwork_deployment_full.ow_suffix,
            fieldwork_deployment_full.greenit_name,
            fieldwork_deployment_full.ow_uid,
            fieldwork_deployment_full.sensor_purpose,
            fieldwork_deployment_full.type,
            fieldwork_deployment_full.long_term_lookup_uid,
            fieldwork_deployment_full.term,
            fieldwork_deployment_full.research_lookup_uid,
            fieldwork_deployment_full.research,
            fieldwork_deployment_full.interval_min,
            fieldwork_deployment_full.public,
            fieldwork_deployment_full.designation,
            fieldwork_deployment_full.oow_program_type,
            fieldwork_deployment_full.smip,
            fieldwork_deployment_full.garp,
            fieldwork_deployment_full.inventory_sensors_uid,
            fieldwork_deployment_full.sensor_serial,
            fieldwork_deployment_full.collection_dtime,
            fieldwork_deployment_full.notes,
            fieldwork_deployment_full.download_error,
            fieldwork_deployment_full.previous_download_error,
            fieldwork_deployment_full.site_name_lookup_uid,
            fieldwork_deployment_full.site_name,
            fieldwork_deployment_full.project_name,
            fieldwork_deployment_full.component_id,
            fieldwork_deployment_full.deployment_dtw_or_depth_ft,
            fieldwork_deployment_full.collection_dtw_or_depth_ft,
            fieldwork_deployment_full.premonitoring_inspection_date,
            fieldwork_deployment_full.ready,
            fieldwork_deployment_full.system_id,
            srt_systems.srt_uid,
            srt_systems.system_id,
            srt_systems.test_date,
            srt_systems.phase,
            srt_systems.type,
            srt_systems.srt_volume_ft3,
            srt_systems.dcia_ft2,
            srt_systems.srt_stormsize_in,
            srt_systems.flow_data_recorded,
            srt_systems.water_level_recorded,
            srt_systems.photos_uploaded,
            srt_systems.sensor_collection_date,
            srt_systems.qaqc_complete,
            srt_systems.srt_summary_date,
            srt_systems.turnaround_days,
            srt_systems.srt_summary,
            srt_systems.project_name,
            srt_systems.sensor_deployed,
            srt_systems.public
           FROM fieldwork_deployment_full
             JOIN srt_systems ON fieldwork_deployment_full.system_id = srt_systems.system_id AND fieldwork_deployment_full.deployment_dtime = srt_systems.test_date
        )
 SELECT cwl_smp.smp_id
   FROM cwl_smp
     LEFT JOIN special_cases special_cases(deployment_uid, deployment_dtime, smp_id, ow_suffix, greenit_name, ow_uid, sensor_purpose, type, long_term_lookup_uid, term, research_lookup_uid, research, interval_min, public, designation, oow_program_type, smip, garp, inventory_sensors_uid, sensor_serial, collection_dtime, notes, download_error, previous_download_error, site_name_lookup_uid, site_name, project_name, component_id, deployment_dtw_or_depth_ft, collection_dtw_or_depth_ft, premonitoring_inspection_date, ready, system_id, srt_uid, system_id_1, test_date, phase, type_1, srt_volume_ft3, dcia_ft2, srt_stormsize_in, flow_data_recorded, water_level_recorded, photos_uploaded, sensor_collection_date, qaqc_complete, srt_summary_date, turnaround_days, srt_summary, project_name_1, sensor_deployed, public_1) ON cwl_smp.deployment_uid = special_cases.deployment_uid
  WHERE special_cases.deployment_uid IS NULL;

CREATE OR REPLACE VIEW fieldwork.viw_unmonitored_postcon_on
 AS
 WITH cwl_smp_postcon AS (
         SELECT viw_postcon_cwl_smp.smp_id
           FROM fieldwork.viw_postcon_cwl_smp
        ), srt_systems_postcon AS (
         SELECT DISTINCT viw_srt_full.srt_uid,
            viw_srt_full.system_id,
            viw_srt_full.test_date,
            viw_srt_full.phase,
            viw_srt_full.type,
            viw_srt_full.srt_volume_ft3,
            viw_srt_full.dcia_ft2,
            viw_srt_full.srt_stormsize_in,
            viw_srt_full.flow_data_recorded,
            viw_srt_full.water_level_recorded,
            viw_srt_full.photos_uploaded,
            viw_srt_full.sensor_collection_date,
            viw_srt_full.qaqc_complete,
            viw_srt_full.srt_summary_date,
            viw_srt_full.turnaround_days,
            viw_srt_full.srt_summary,
            viw_srt_full.project_name,
            viw_srt_full.sensor_deployed,
            viw_srt_full.public
           FROM fieldwork.viw_srt_full
          WHERE viw_srt_full.phase = 'Post-Construction'::text
        ), greenit_built_info AS (
         SELECT tbl_smpbdv.smp_id,
            tbl_smpbdv.system_id,
            tbl_smpbdv.smp_notbuiltretired,
            tbl_smpbdv.smp_smptype,
            tbl_smpbdv.cipit_status AS capit_status
           FROM external.tbl_smpbdv
          WHERE tbl_smpbdv.smp_notbuiltretired IS NULL AND tbl_smpbdv.smp_smptype <> 'Depaving'::text
        ), stromwatertree_with_cet AS (
         SELECT tbl_smpbdv.smp_id,
            tbl_smpbdv.system_id,
            tbl_smpbdv.smp_notbuiltretired,
            tbl_smpbdv.smp_smptype,
            tbl_smpbdv.cipit_status AS capit_status
           FROM external.tbl_smpbdv
             JOIN fieldwork.tbl_capture_efficiency ON tbl_smpbdv.system_id = tbl_capture_efficiency.system_id
          WHERE tbl_smpbdv.smp_notbuiltretired IS NULL AND tbl_smpbdv.smp_smptype <> 'Depaving'::text AND tbl_smpbdv.smp_smptype = 'Stormwater Tree'::text
        ), monitoring_deny_list AS (
         SELECT tbl_monitoring_deny_list.monitoring_deny_list_uid,
            tbl_monitoring_deny_list.smp_id,
            tbl_monitoring_deny_list.reason
           FROM fieldwork.tbl_monitoring_deny_list
        ), porous_pavement_last_two_yrs AS (
         SELECT tbl_porous_pavement.porous_pavement_uid,
            tbl_porous_pavement.test_date,
            tbl_porous_pavement.smp_id,
            tbl_porous_pavement.surface_type_lookup_uid,
            tbl_porous_pavement.con_phase_lookup_uid,
            tbl_porous_pavement.test_location,
            tbl_porous_pavement.data_in_spreadsheet,
            tbl_porous_pavement.map_in_site_folder,
            tbl_porous_pavement.ring_diameter_in,
            tbl_porous_pavement.prewet_time_s,
            tbl_porous_pavement.prewet_rate_inhr
           FROM fieldwork.tbl_porous_pavement
          WHERE tbl_porous_pavement.test_date > (now() - '2 years'::interval)
        ), pwd_maintained AS (
         SELECT viw_gso_maintenance.smp_id
           FROM external.viw_gso_maintenance
          WHERE viw_gso_maintenance.maintained = true
        )
 SELECT greenit_built_info.smp_id,
    greenit_built_info.system_id,
    greenit_built_info.smp_smptype AS smp_type,
    greenit_built_info.capit_status
   FROM greenit_built_info
     LEFT JOIN cwl_smp_postcon ON greenit_built_info.smp_id = cwl_smp_postcon.smp_id
     LEFT JOIN srt_systems_postcon ON greenit_built_info.system_id = srt_systems_postcon.system_id
     LEFT JOIN monitoring_deny_list ON greenit_built_info.smp_id = monitoring_deny_list.smp_id
     LEFT JOIN porous_pavement_last_two_yrs ON greenit_built_info.smp_id = porous_pavement_last_two_yrs.smp_id
     LEFT JOIN stromwatertree_with_cet ON greenit_built_info.system_id = stromwatertree_with_cet.system_id
  WHERE (greenit_built_info.smp_id IN ( SELECT DISTINCT pwd_maintained.smp_id
           FROM pwd_maintained)) AND cwl_smp_postcon.smp_id IS NULL AND srt_systems_postcon.system_id IS NULL AND monitoring_deny_list.smp_id IS NULL AND porous_pavement_last_two_yrs.smp_id IS NULL AND stromwatertree_with_cet.system_id IS NULL;

CREATE OR REPLACE VIEW fieldwork.viw_unmonitored_both_off
 AS
 WITH cwl_smp AS (
         SELECT DISTINCT viw_deployment_full_cwl.deployment_uid,
            viw_deployment_full_cwl.deployment_dtime,
            viw_deployment_full_cwl.smp_id,
            viw_deployment_full_cwl.ow_suffix,
            viw_deployment_full_cwl.greenit_name,
            viw_deployment_full_cwl.ow_uid,
            viw_deployment_full_cwl.sensor_purpose,
            viw_deployment_full_cwl.type,
            viw_deployment_full_cwl.long_term_lookup_uid,
            viw_deployment_full_cwl.term,
            viw_deployment_full_cwl.research_lookup_uid,
            viw_deployment_full_cwl.research,
            viw_deployment_full_cwl.interval_min,
            viw_deployment_full_cwl.public,
            viw_deployment_full_cwl.designation,
            viw_deployment_full_cwl.oow_program_type,
            viw_deployment_full_cwl.smip,
            viw_deployment_full_cwl.garp,
            viw_deployment_full_cwl.inventory_sensors_uid,
            viw_deployment_full_cwl.sensor_serial,
            viw_deployment_full_cwl.collection_dtime,
            viw_deployment_full_cwl.notes,
            viw_deployment_full_cwl.download_error,
            viw_deployment_full_cwl.previous_download_error,
            viw_deployment_full_cwl.site_name_lookup_uid,
            viw_deployment_full_cwl.site_name,
            viw_deployment_full_cwl.project_name,
            viw_deployment_full_cwl.component_id,
            viw_deployment_full_cwl.deployment_dtw_or_depth_ft,
            viw_deployment_full_cwl.collection_dtw_or_depth_ft,
            viw_deployment_full_cwl.premonitoring_inspection_date,
            viw_deployment_full_cwl.ready
           FROM fieldwork.viw_deployment_full_cwl
        ), srt_systems AS (
         SELECT DISTINCT viw_srt_full.srt_uid,
            viw_srt_full.system_id,
            viw_srt_full.test_date,
            viw_srt_full.phase,
            viw_srt_full.type,
            viw_srt_full.srt_volume_ft3,
            viw_srt_full.dcia_ft2,
            viw_srt_full.srt_stormsize_in,
            viw_srt_full.flow_data_recorded,
            viw_srt_full.water_level_recorded,
            viw_srt_full.photos_uploaded,
            viw_srt_full.sensor_collection_date,
            viw_srt_full.qaqc_complete,
            viw_srt_full.srt_summary_date,
            viw_srt_full.turnaround_days,
            viw_srt_full.srt_summary,
            viw_srt_full.project_name,
            viw_srt_full.sensor_deployed,
            viw_srt_full.public
           FROM fieldwork.viw_srt_full
        ), greenit_built_info AS (
         SELECT tbl_smpbdv.smp_id,
            tbl_smpbdv.system_id,
            tbl_smpbdv.smp_notbuiltretired,
            tbl_smpbdv.smp_smptype,
            tbl_smpbdv.cipit_status AS capit_status
           FROM external.tbl_smpbdv
          WHERE tbl_smpbdv.smp_notbuiltretired IS NULL AND tbl_smpbdv.smp_smptype <> 'Depaving'::text
        ), stromwatertree_with_cet AS (
         SELECT tbl_smpbdv.smp_id,
            tbl_smpbdv.system_id,
            tbl_smpbdv.smp_notbuiltretired,
            tbl_smpbdv.smp_smptype,
            tbl_smpbdv.cipit_status AS capit_status
           FROM external.tbl_smpbdv
             JOIN fieldwork.tbl_capture_efficiency ON tbl_smpbdv.system_id = tbl_capture_efficiency.system_id
          WHERE tbl_smpbdv.smp_notbuiltretired IS NULL AND tbl_smpbdv.smp_smptype <> 'Depaving'::text AND tbl_smpbdv.smp_smptype = 'Stormwater Tree'::text
        ), monitoring_deny_list AS (
         SELECT tbl_monitoring_deny_list.monitoring_deny_list_uid,
            tbl_monitoring_deny_list.smp_id,
            tbl_monitoring_deny_list.reason
           FROM fieldwork.tbl_monitoring_deny_list
        ), porous_pavement_last_two_yrs AS (
         SELECT tbl_porous_pavement.porous_pavement_uid,
            tbl_porous_pavement.test_date,
            tbl_porous_pavement.smp_id,
            tbl_porous_pavement.surface_type_lookup_uid,
            tbl_porous_pavement.con_phase_lookup_uid,
            tbl_porous_pavement.test_location,
            tbl_porous_pavement.data_in_spreadsheet,
            tbl_porous_pavement.map_in_site_folder,
            tbl_porous_pavement.ring_diameter_in,
            tbl_porous_pavement.prewet_time_s,
            tbl_porous_pavement.prewet_rate_inhr
           FROM fieldwork.tbl_porous_pavement
          WHERE tbl_porous_pavement.test_date > (now() - '2 years'::interval)
        ), pwd_maintained AS (
         SELECT viw_gso_maintenance.smp_id
           FROM external.viw_gso_maintenance
          WHERE viw_gso_maintenance.maintained = true
        )
 SELECT greenit_built_info.smp_id,
    greenit_built_info.system_id,
    greenit_built_info.smp_smptype AS smp_type,
    greenit_built_info.capit_status
   FROM greenit_built_info
     LEFT JOIN cwl_smp ON greenit_built_info.smp_id = cwl_smp.smp_id
     LEFT JOIN srt_systems ON greenit_built_info.system_id = srt_systems.system_id
     LEFT JOIN monitoring_deny_list ON greenit_built_info.smp_id = monitoring_deny_list.smp_id
     LEFT JOIN porous_pavement_last_two_yrs ON greenit_built_info.smp_id = porous_pavement_last_two_yrs.smp_id
     LEFT JOIN stromwatertree_with_cet ON greenit_built_info.system_id = stromwatertree_with_cet.system_id
  WHERE (greenit_built_info.smp_id IN ( SELECT DISTINCT pwd_maintained.smp_id
           FROM pwd_maintained)) AND cwl_smp.smp_id IS NULL AND srt_systems.system_id IS NULL AND monitoring_deny_list.smp_id IS NULL AND porous_pavement_last_two_yrs.smp_id IS NULL AND stromwatertree_with_cet.system_id IS NULL;


CREATE OR REPLACE VIEW fieldwork.viw_unmonitored_both_on
 AS
 WITH cwl_smp_postcon AS (
         SELECT viw_postcon_cwl_smp.smp_id
           FROM fieldwork.viw_postcon_cwl_smp
        ), srt_systems_postcon AS (
         SELECT DISTINCT viw_srt_full.srt_uid,
            viw_srt_full.system_id,
            viw_srt_full.test_date,
            viw_srt_full.phase,
            viw_srt_full.type,
            viw_srt_full.srt_volume_ft3,
            viw_srt_full.dcia_ft2,
            viw_srt_full.srt_stormsize_in,
            viw_srt_full.flow_data_recorded,
            viw_srt_full.water_level_recorded,
            viw_srt_full.photos_uploaded,
            viw_srt_full.sensor_collection_date,
            viw_srt_full.qaqc_complete,
            viw_srt_full.srt_summary_date,
            viw_srt_full.turnaround_days,
            viw_srt_full.srt_summary,
            viw_srt_full.project_name,
            viw_srt_full.sensor_deployed,
            viw_srt_full.public
           FROM fieldwork.viw_srt_full
          WHERE viw_srt_full.phase = 'Post-Construction'::text
        ), greenit_built_info AS (
         SELECT tbl_smpbdv.smp_id,
            tbl_smpbdv.system_id,
            tbl_smpbdv.smp_notbuiltretired,
            tbl_smpbdv.smp_smptype,
            tbl_smpbdv.cipit_status AS capit_status
           FROM external.tbl_smpbdv
          WHERE tbl_smpbdv.smp_notbuiltretired IS NULL AND tbl_smpbdv.smp_smptype <> 'Depaving'::text
        ), stromwatertree_with_cet AS (
         SELECT tbl_smpbdv.smp_id,
            tbl_smpbdv.system_id,
            tbl_smpbdv.smp_notbuiltretired,
            tbl_smpbdv.smp_smptype,
            tbl_smpbdv.cipit_status AS capit_status
           FROM external.tbl_smpbdv
             JOIN fieldwork.tbl_capture_efficiency ON tbl_smpbdv.system_id = tbl_capture_efficiency.system_id
          WHERE tbl_smpbdv.smp_notbuiltretired IS NULL AND tbl_smpbdv.smp_smptype <> 'Depaving'::text AND tbl_smpbdv.smp_smptype = 'Stormwater Tree'::text
        ), monitoring_deny_list AS (
         SELECT tbl_monitoring_deny_list.monitoring_deny_list_uid,
            tbl_monitoring_deny_list.smp_id,
            tbl_monitoring_deny_list.reason
           FROM fieldwork.tbl_monitoring_deny_list
        ), porous_pavement_last_two_yrs AS (
         SELECT tbl_porous_pavement.porous_pavement_uid,
            tbl_porous_pavement.test_date,
            tbl_porous_pavement.smp_id,
            tbl_porous_pavement.surface_type_lookup_uid,
            tbl_porous_pavement.con_phase_lookup_uid,
            tbl_porous_pavement.test_location,
            tbl_porous_pavement.data_in_spreadsheet,
            tbl_porous_pavement.map_in_site_folder,
            tbl_porous_pavement.ring_diameter_in,
            tbl_porous_pavement.prewet_time_s,
            tbl_porous_pavement.prewet_rate_inhr
           FROM fieldwork.tbl_porous_pavement
          WHERE tbl_porous_pavement.test_date > (now() - '2 years'::interval)
        ), pwd_maintained AS (
         SELECT viw_gso_maintenance.smp_id
           FROM external.viw_gso_maintenance
          WHERE viw_gso_maintenance.maintained = true
        ), future_deployments AS (
         SELECT viw_future_deployments_full.future_deployment_uid,
            viw_future_deployments_full.smp_id,
            viw_future_deployments_full.ow_suffix,
            viw_future_deployments_full.greenit_name,
            viw_future_deployments_full.ow_uid,
            viw_future_deployments_full.sensor_purpose,
            viw_future_deployments_full.type,
            viw_future_deployments_full.long_term_lookup_uid,
            viw_future_deployments_full.term,
            viw_future_deployments_full.research_lookup_uid,
            viw_future_deployments_full.research,
            viw_future_deployments_full.interval_min,
            viw_future_deployments_full.public,
            viw_future_deployments_full.designation,
            viw_future_deployments_full.oow_program_type,
            viw_future_deployments_full.smip,
            viw_future_deployments_full.garp,
            viw_future_deployments_full.inventory_sensors_uid,
            viw_future_deployments_full.sensor_serial,
            viw_future_deployments_full.field_test_priority_lookup_uid,
            viw_future_deployments_full.field_test_priority,
            viw_future_deployments_full.notes,
            viw_future_deployments_full.site_name_lookup_uid,
            viw_future_deployments_full.site_name,
            viw_future_deployments_full.project_name,
            viw_future_deployments_full.premonitoring_inspection,
            viw_future_deployments_full.ready
           FROM fieldwork.viw_future_deployments_full
        )
 SELECT greenit_built_info.smp_id,
    greenit_built_info.system_id,
    greenit_built_info.smp_smptype AS smp_type,
    greenit_built_info.capit_status
   FROM greenit_built_info
     LEFT JOIN cwl_smp_postcon ON greenit_built_info.smp_id = cwl_smp_postcon.smp_id
     LEFT JOIN srt_systems_postcon ON greenit_built_info.system_id = srt_systems_postcon.system_id
     LEFT JOIN monitoring_deny_list ON greenit_built_info.smp_id = monitoring_deny_list.smp_id
     LEFT JOIN porous_pavement_last_two_yrs ON greenit_built_info.smp_id = porous_pavement_last_two_yrs.smp_id
     LEFT JOIN stromwatertree_with_cet ON greenit_built_info.system_id = stromwatertree_with_cet.system_id
     LEFT JOIN future_deployments ON greenit_built_info.smp_id = future_deployments.smp_id
  WHERE (greenit_built_info.smp_id IN ( SELECT DISTINCT pwd_maintained.smp_id
           FROM pwd_maintained)) AND cwl_smp_postcon.smp_id IS NULL AND srt_systems_postcon.system_id IS NULL AND monitoring_deny_list.smp_id IS NULL AND future_deployments.smp_id IS NULL AND porous_pavement_last_two_yrs.smp_id IS NULL AND stromwatertree_with_cet.system_id IS NULL;


CREATE OR REPLACE VIEW fieldwork.viw_inventory_sensors_full
 AS
 SELECT inv.sensor_serial,
    sml.sensor_model,
    inv.date_purchased,
    ow.smp_id,
    snl.site_name,
    ow.ow_suffix,
    ssl.sensor_status,
    inv.sensor_issue_lookup_uid_one,
    silo.sensor_issue AS issue_one,
    inv.sensor_issue_lookup_uid_two,
    silt.sensor_issue AS issue_two,
    inv.request_data,
    inv.sensor_model_lookup_uid,
    inv.inventory_sensors_uid
   FROM fieldwork.tbl_inventory_sensors inv
     LEFT JOIN fieldwork.tbl_deployment d ON d.inventory_sensors_uid = inv.inventory_sensors_uid AND d.collection_dtime IS NULL
     LEFT JOIN fieldwork.tbl_ow ow ON ow.ow_uid = d.ow_uid
     LEFT JOIN fieldwork.tbl_sensor_status_lookup ssl ON ssl.sensor_status_lookup_uid = inv.sensor_status_lookup_uid
     LEFT JOIN fieldwork.tbl_site_name_lookup snl ON snl.site_name_lookup_uid = ow.site_name_lookup_uid
     LEFT JOIN fieldwork.tbl_sensor_issue_lookup silo ON inv.sensor_issue_lookup_uid_one = silo.sensor_issue_lookup_uid
     LEFT JOIN fieldwork.tbl_sensor_issue_lookup silt ON inv.sensor_issue_lookup_uid_two = silt.sensor_issue_lookup_uid
     LEFT JOIN fieldwork.tbl_sensor_model_lookup sml ON inv.sensor_model_lookup_uid = sml.sensor_model_lookup_uid;

CREATE OR REPLACE VIEW fieldwork.viw_previous_deployments
 AS
 SELECT fd.deployment_uid,
    fd.deployment_dtime,
    fow.smp_id,
    fow.ow_suffix,
    fd.ow_uid,
    fd.sensor_purpose,
    de.type,
    lt.type AS term,
    rs.type AS research,
    fd.interval_min,
    fd.inventory_sensors_uid,
    inv.sensor_serial,
    fd.collection_dtime,
    fd.notes,
    fd.download_error,
    fow.site_name_lookup_uid,
    snl.site_name,
    fd.deployment_dtw_or_depth_ft,
    fd.collection_dtw_or_depth_ft,
    fd.premonitoring_inspection_date,
    fd.ready
   FROM fieldwork.tbl_deployment fd
     LEFT JOIN fieldwork.tbl_ow fow ON fd.ow_uid = fow.ow_uid
     LEFT JOIN fieldwork.tbl_inventory_sensors inv ON fd.inventory_sensors_uid = inv.inventory_sensors_uid
     LEFT JOIN fieldwork.tbl_sensor_purpose_lookup de ON fd.sensor_purpose = de.sensor_purpose_lookup_uid
     LEFT JOIN fieldwork.tbl_long_term_lookup lt ON lt.long_term_lookup_uid = fd.long_term_lookup_uid
     LEFT JOIN fieldwork.tbl_research_lookup rs ON rs.research_lookup_uid = fd.research_lookup_uid
     LEFT JOIN fieldwork.tbl_site_name_lookup snl ON fow.site_name_lookup_uid = snl.site_name_lookup_uid
  WHERE fd.collection_dtime IS NOT NULL AND (fow.smp_id IS NOT NULL OR fow.site_name_lookup_uid IS NOT NULL);

  CREATE OR REPLACE VIEW fieldwork.viw_unmonitored_active_smps
 AS
 WITH inactive_inlets AS (
         SELECT tbl_gswiinlet.lifecycle_status,
            tbl_gswiinlet.facility_id,
            admin.fun_component_to_smp(tbl_gswiinlet.component_id::character varying::text) AS smp_id,
            admin.fun_smp_to_system(admin.fun_component_to_smp(tbl_gswiinlet.component_id::character varying::text)) AS system_id,
            tbl_gswiinlet.component_id
           FROM external.tbl_gswiinlet
          WHERE (tbl_gswiinlet.lifecycle_status <> 'ACT'::text OR tbl_gswiinlet.plug_status <> 'ONLINE'::text) AND tbl_gswiinlet.component_id IS NOT NULL
        UNION
         SELECT c.lifecycle_status,
            c.facility_id,
            admin.fun_component_to_smp(btrim(c.component_id, ' '::text)::character varying::text) AS smp_id,
            admin.fun_smp_to_system(admin.fun_component_to_smp(btrim(c.component_id, ' '::text)::character varying::text)) AS system_id,
            c.component_id
           FROM external.tbl_gswiconveyance c
             LEFT JOIN fieldwork.tbl_gswi_conveyance_subtype_lookup lo ON c.subtype = lo.code
          WHERE c.component_id IS NOT NULL AND c.lifecycle_status <> 'ACT'::text
        UNION
         SELECT s.lifecycle_status,
            s.facility_id,
            admin.fun_component_to_smp(btrim(s.component_id, ' '::text)::character varying::text) AS smp_id,
            admin.fun_smp_to_system(admin.fun_component_to_smp(btrim(s.component_id, ' '::text)::character varying::text)) AS system_id,
            s.component_id
           FROM external.tbl_gswistructure s
          WHERE s.component_id IS NOT NULL AND s.lifecycle_status <> 'ACT'::text
        ), greenit_built_info AS (
         SELECT tbl_smpbdv.smp_id,
            tbl_smpbdv.system_id,
            tbl_smpbdv.smp_notbuiltretired,
            tbl_smpbdv.smp_smptype,
            tbl_smpbdv.cipit_status AS capit_status
           FROM external.tbl_smpbdv
          WHERE tbl_smpbdv.smp_notbuiltretired IS NULL AND tbl_smpbdv.smp_smptype <> 'Depaving'::text
        ), cwl_smp AS (
         SELECT DISTINCT viw_deployment_full_cwl.smp_id
           FROM fieldwork.viw_deployment_full_cwl
        ), cwl_system AS (
         SELECT DISTINCT admin.fun_smp_to_system(viw_deployment_full_cwl.smp_id::character varying::text) AS system_id
           FROM fieldwork.viw_deployment_full_cwl
        ), srt_systems AS (
         SELECT DISTINCT viw_srt_full.system_id
           FROM fieldwork.viw_srt_full
        )
 SELECT DISTINCT gbi.smp_id,
    gbi.smp_smptype AS smp_type,
    gbi.capit_status,
        CASE
            WHEN sys.system_id IS NULL THEN false
            WHEN sys.system_id IS NOT NULL THEN true
            ELSE NULL::boolean
        END AS other_cwl_at_this_system
   FROM greenit_built_info gbi
     LEFT JOIN cwl_system sys ON sys.system_id = gbi.system_id
  WHERE (gbi.capit_status = ANY (ARRAY['Construction-Substantially Complete'::text, 'Closed'::text])) AND NOT (EXISTS ( SELECT cs.smp_id
           FROM cwl_smp cs
          WHERE cs.smp_id = gbi.smp_id)) AND NOT (EXISTS ( SELECT ss.system_id
           FROM srt_systems ss
          WHERE ss.system_id = gbi.system_id)) AND NOT (EXISTS ( SELECT ii.smp_id
           FROM inactive_inlets ii
          WHERE ii.system_id = gbi.system_id)) AND NOT (EXISTS ( SELECT deny.smp_id
           FROM fieldwork.tbl_monitoring_deny_list deny
          WHERE deny.smp_id = gbi.smp_id)) AND NOT (EXISTS ( SELECT cet.system_id
           FROM fieldwork.tbl_capture_efficiency cet
          WHERE cet.system_id = gbi.system_id AND gbi.smp_smptype = 'Stormwater Tree'::text)) AND NOT (EXISTS ( SELECT ppt.smp_id
           FROM fieldwork.tbl_porous_pavement ppt
          WHERE ppt.smp_id = gbi.smp_id AND ppt.test_date > (now() - '2 years'::interval)))
  ORDER BY gbi.smp_id;

CREATE OR REPLACE VIEW fieldwork.viw_unmonitored_future_on
 AS
 WITH cwl_smp AS (
         SELECT DISTINCT viw_deployment_full_cwl.deployment_uid,
            viw_deployment_full_cwl.deployment_dtime,
            viw_deployment_full_cwl.smp_id,
            viw_deployment_full_cwl.ow_suffix,
            viw_deployment_full_cwl.greenit_name,
            viw_deployment_full_cwl.ow_uid,
            viw_deployment_full_cwl.sensor_purpose,
            viw_deployment_full_cwl.type,
            viw_deployment_full_cwl.long_term_lookup_uid,
            viw_deployment_full_cwl.term,
            viw_deployment_full_cwl.research_lookup_uid,
            viw_deployment_full_cwl.research,
            viw_deployment_full_cwl.interval_min,
            viw_deployment_full_cwl.public,
            viw_deployment_full_cwl.designation,
            viw_deployment_full_cwl.oow_program_type,
            viw_deployment_full_cwl.smip,
            viw_deployment_full_cwl.garp,
            viw_deployment_full_cwl.inventory_sensors_uid,
            viw_deployment_full_cwl.sensor_serial,
            viw_deployment_full_cwl.collection_dtime,
            viw_deployment_full_cwl.notes,
            viw_deployment_full_cwl.download_error,
            viw_deployment_full_cwl.previous_download_error,
            viw_deployment_full_cwl.site_name_lookup_uid,
            viw_deployment_full_cwl.site_name,
            viw_deployment_full_cwl.project_name,
            viw_deployment_full_cwl.component_id,
            viw_deployment_full_cwl.deployment_dtw_or_depth_ft,
            viw_deployment_full_cwl.collection_dtw_or_depth_ft,
            viw_deployment_full_cwl.premonitoring_inspection_date,
            viw_deployment_full_cwl.ready
           FROM fieldwork.viw_deployment_full_cwl
        ), srt_systems AS (
         SELECT DISTINCT viw_srt_full.srt_uid,
            viw_srt_full.system_id,
            viw_srt_full.test_date,
            viw_srt_full.phase,
            viw_srt_full.type,
            viw_srt_full.srt_volume_ft3,
            viw_srt_full.dcia_ft2,
            viw_srt_full.srt_stormsize_in,
            viw_srt_full.flow_data_recorded,
            viw_srt_full.water_level_recorded,
            viw_srt_full.photos_uploaded,
            viw_srt_full.sensor_collection_date,
            viw_srt_full.qaqc_complete,
            viw_srt_full.srt_summary_date,
            viw_srt_full.turnaround_days,
            viw_srt_full.srt_summary,
            viw_srt_full.project_name,
            viw_srt_full.sensor_deployed,
            viw_srt_full.public
           FROM fieldwork.viw_srt_full
        ), greenit_built_info AS (
         SELECT tbl_smpbdv.smp_id,
            tbl_smpbdv.system_id,
            tbl_smpbdv.smp_notbuiltretired,
            tbl_smpbdv.smp_smptype,
            tbl_smpbdv.cipit_status AS capit_status
           FROM external.tbl_smpbdv
          WHERE tbl_smpbdv.smp_notbuiltretired IS NULL AND tbl_smpbdv.smp_smptype <> 'Depaving'::text
        ), stromwatertree_with_cet AS (
         SELECT tbl_smpbdv.smp_id,
            tbl_smpbdv.system_id,
            tbl_smpbdv.smp_notbuiltretired,
            tbl_smpbdv.smp_smptype,
            tbl_smpbdv.cipit_status AS capit_status
           FROM external.tbl_smpbdv
             JOIN fieldwork.tbl_capture_efficiency ON tbl_smpbdv.system_id = tbl_capture_efficiency.system_id
          WHERE tbl_smpbdv.smp_notbuiltretired IS NULL AND tbl_smpbdv.smp_smptype <> 'Depaving'::text AND tbl_smpbdv.smp_smptype = 'Stormwater Tree'::text
        ), monitoring_deny_list AS (
         SELECT tbl_monitoring_deny_list.monitoring_deny_list_uid,
            tbl_monitoring_deny_list.smp_id,
            tbl_monitoring_deny_list.reason
           FROM fieldwork.tbl_monitoring_deny_list
        ), porous_pavement_last_two_yrs AS (
         SELECT tbl_porous_pavement.porous_pavement_uid,
            tbl_porous_pavement.test_date,
            tbl_porous_pavement.smp_id,
            tbl_porous_pavement.surface_type_lookup_uid,
            tbl_porous_pavement.con_phase_lookup_uid,
            tbl_porous_pavement.test_location,
            tbl_porous_pavement.data_in_spreadsheet,
            tbl_porous_pavement.map_in_site_folder,
            tbl_porous_pavement.ring_diameter_in,
            tbl_porous_pavement.prewet_time_s,
            tbl_porous_pavement.prewet_rate_inhr
           FROM fieldwork.tbl_porous_pavement
          WHERE tbl_porous_pavement.test_date > (now() - '2 years'::interval)
        ), pwd_maintained AS (
         SELECT viw_gso_maintenance.smp_id
           FROM external.viw_gso_maintenance
          WHERE viw_gso_maintenance.maintained = true
        ), future_deployments AS (
         SELECT DISTINCT viw_future_deployments_full.future_deployment_uid,
            viw_future_deployments_full.smp_id,
            viw_future_deployments_full.ow_suffix,
            viw_future_deployments_full.greenit_name,
            viw_future_deployments_full.ow_uid,
            viw_future_deployments_full.sensor_purpose,
            viw_future_deployments_full.type,
            viw_future_deployments_full.long_term_lookup_uid,
            viw_future_deployments_full.term,
            viw_future_deployments_full.research_lookup_uid,
            viw_future_deployments_full.research,
            viw_future_deployments_full.interval_min,
            viw_future_deployments_full.public,
            viw_future_deployments_full.designation,
            viw_future_deployments_full.oow_program_type,
            viw_future_deployments_full.smip,
            viw_future_deployments_full.garp,
            viw_future_deployments_full.inventory_sensors_uid,
            viw_future_deployments_full.sensor_serial,
            viw_future_deployments_full.field_test_priority_lookup_uid,
            viw_future_deployments_full.field_test_priority,
            viw_future_deployments_full.notes,
            viw_future_deployments_full.site_name_lookup_uid,
            viw_future_deployments_full.site_name,
            viw_future_deployments_full.project_name,
            viw_future_deployments_full.premonitoring_inspection,
            viw_future_deployments_full.ready
           FROM fieldwork.viw_future_deployments_full
        )
 SELECT greenit_built_info.smp_id,
    greenit_built_info.system_id,
    greenit_built_info.smp_smptype AS smp_type,
    greenit_built_info.capit_status
   FROM greenit_built_info
     LEFT JOIN cwl_smp ON greenit_built_info.smp_id = cwl_smp.smp_id
     LEFT JOIN srt_systems ON greenit_built_info.system_id = srt_systems.system_id
     LEFT JOIN monitoring_deny_list ON greenit_built_info.smp_id = monitoring_deny_list.smp_id
     LEFT JOIN porous_pavement_last_two_yrs ON greenit_built_info.smp_id = porous_pavement_last_two_yrs.smp_id
     LEFT JOIN stromwatertree_with_cet ON greenit_built_info.system_id = stromwatertree_with_cet.system_id
     LEFT JOIN future_deployments ON greenit_built_info.smp_id = future_deployments.smp_id
  WHERE (greenit_built_info.smp_id IN ( SELECT DISTINCT pwd_maintained.smp_id
           FROM pwd_maintained)) AND cwl_smp.smp_id IS NULL AND future_deployments.smp_id IS NULL AND srt_systems.system_id IS NULL AND monitoring_deny_list.smp_id IS NULL AND porous_pavement_last_two_yrs.smp_id IS NULL AND stromwatertree_with_cet.system_id IS NULL;

CREATE OR REPLACE VIEW fieldwork.viw_qaqc_deployments
 AS
 SELECT viw_deployment_full.deployment_uid,
    viw_deployment_full.deployment_dtime,
    viw_deployment_full.smp_id,
    viw_deployment_full.ow_suffix,
    viw_deployment_full.project_name,
    viw_deployment_full.ow_uid,
    viw_deployment_full.sensor_purpose,
    viw_deployment_full.type,
    viw_deployment_full.long_term_lookup_uid,
    viw_deployment_full.term,
    viw_deployment_full.research_lookup_uid,
    viw_deployment_full.research,
    viw_deployment_full.interval_min,
    viw_deployment_full.public,
    viw_deployment_full.designation,
    viw_deployment_full.oow_program_type,
    viw_deployment_full.smip,
    viw_deployment_full.garp,
    viw_deployment_full.deployment_dtime + viw_deployment_full.interval_min::double precision * '15 days'::interval AS date_100percent,
    viw_deployment_full.inventory_sensors_uid,
    viw_deployment_full.sensor_serial,
    date_part('month'::text, viw_deployment_full.deployment_dtime) AS collection_month,
    date_part('year'::text, viw_deployment_full.deployment_dtime) AS collection_year,
    viw_deployment_full.notes,
    viw_deployment_full.download_error,
    viw_deployment_full.previous_download_error,
    viw_deployment_full.site_name_lookup_uid,
    viw_deployment_full.site_name,
    viw_deployment_full.component_id,
    viw_deployment_full.deployment_dtw_or_depth_ft,
    viw_deployment_full.premonitoring_inspection_date,
    viw_deployment_full.ready,
    viw_deployment_full.collection_dtime
   FROM fieldwork.viw_deployment_full
  WHERE viw_deployment_full.smp_id IS NOT NULL OR viw_deployment_full.site_name_lookup_uid IS NOT NULL
  ORDER BY viw_deployment_full.collection_dtime DESC;
