alter view data.viw_gage_event_latestdates rename to old_viw_gage_event_latestdates;
alter view data.viw_gage_rain_latestdates rename to old_viw_gage_rain_latestdates;
alter view data.viw_gage_rainfall rename to old_viw_gage_rainfall;
alter view data.viw_gwdata_latestdates rename to old_viw_gwdata_latestdates;
alter view data.viw_ow_leveldata_sumpcorrected rename to old_viw_ow_leveldata_sumpcorrected;
alter view data.viw_owdata_earliestdates rename to old_viw_owdata_earliestdates;
alter view data.viw_owdata_latestdates rename to old_viw_owdata_latestdates;
alter view data.viw_barodata_smp rename to old_viw_barodata_smp;
alter view data.viw_barodata_neighbors rename to old_viw_barodata_neighbors;

alter table data.tbl_gage_rain rename to old_tbl_gage_rain;
alter table data.tbl_gage_event rename to old_tbl_gage_event;
alter table data.tbl_gw_depthdata_raw rename to old_tbl_gw_depthdata_raw;
alter table data.tbl_ow_leveldata_raw rename to old_tbl_ow_leveldata_raw;
alter table data.tbl_baro rename to old_tbl_baro;

alter table data.test_tbl_gage_rain rename to tbl_gage_rain;
alter table data.test_tbl_gage_event rename to tbl_gage_event;
alter table data.test_tbl_gw_depthdata_raw rename to tbl_gw_depthdata_raw;
alter table data.test_tbl_ow_leveldata_raw rename to tbl_ow_leveldata_raw;
alter table data.test_tbl_baro rename to tbl_baro;

alter view data.test_viw_gage_event_latestdates rename to viw_gage_event_latestdates;
alter view data.test_viw_gage_rain_latestdates rename to viw_gage_rain_latestdates;
alter view data.test_viw_gage_rainfall rename to viw_gage_rainfall;
alter view data.test_viw_gwdata_latestdates rename to viw_gwdata_latestdates;
alter view data.test_viw_ow_leveldata_sumpcorrected rename to viw_ow_leveldata_sumpcorrected;
alter view data.test_viw_owdata_earliestdates rename to viw_owdata_earliestdates;
alter view data.test_viw_owdata_latestdates rename to viw_owdata_latestdates;
alter view data.test_viw_barodata_smp rename to viw_barodata_smp;
alter view data.test_viw_barodata_neighbors rename to viw_barodata_neighbors;