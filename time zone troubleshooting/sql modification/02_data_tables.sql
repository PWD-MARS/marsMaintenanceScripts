drop VIEW if exists data.test_viw_gage_event_latestdates;
drop VIEW if exists data.test_viw_gage_rain_latestdates;
drop VIEW if exists data.test_viw_gage_rainfall;
drop VIEW if exists data.test_viw_gwdata_latestdates;
drop VIEW if exists data.test_viw_ow_leveldata_sumpcorrected;
drop VIEW if exists data.test_viw_owdata_earliestdates;
drop VIEW if exists data.test_viw_owdata_latestdates;
drop VIEW if exists data.test_viw_radar_event_latestdates;
drop VIEW if exists data.test_viw_radar_rain_latestdates;
drop VIEW if exists data.test_viw_radar_rainfall;
drop VIEW if exists data.test_viw_barodata_neighbors;
drop VIEW if exists data.test_viw_barodata_smp;

drop table if exists data.test_tbl_gage_rain;
drop table if exists data.test_tbl_gage_event;
drop table if exists data.test_tbl_gw_depthdata_raw;
drop table if exists data.test_tbl_ow_leveldata_raw;
drop table if exists data.test_tbl_radar_event;
drop table if exists data.test_tbl_radar_rain;
drop table if exists data.test_tbl_baro;

-- Gage rainfall

create table if not exists data.test_tbl_gage_rain
(
    gage_rain_uid integer not null generated always as identity,
    gage_uid integer not null,
    dtime timestamp with time zone not null,
    rainfall_in numeric(8,4) not null,
    constraint sandbox_gage_rain_pkey primary key (gage_rain_uid),
    constraint sandbox_gage_rain_uniqueness unique (gage_uid, dtime),
    constraint sandbox_gage_uid_fkey foreign key (gage_uid)
        references admin.tbl_gage (gage_uid) match simple
        on update cascade
        on delete no action
);

create table if not exists data.test_tbl_gage_event
(
    gage_event_uid integer not null generated always as identity,
    gage_uid integer not null,
    eventdatastart timestamp with time zone not null,
    eventdataend timestamp with time zone not null,
    eventduration_hr numeric(8,4) not null,
    eventpeakintensity_inhr numeric(8,4) not null,
    eventavgintensity_inhr numeric(8,4) not null,
    eventdepth_in numeric(8,4) not null,
    constraint sandbox_gage_event_pkey primary key (gage_event_uid),
    constraint sandbox_gage_uid_fkey foreign key (gage_uid)
        references admin.tbl_gage (gage_uid) match simple
        on update cascade
        on delete no action
);

-- GW and OW

create table if not exists data.test_tbl_gw_depthdata_raw
(
    gw_depthdata_raw_uid integer not null generated always as identity,
    dtime timestamp with time zone not null,
    depth_ft numeric(8,6) not null,
    ow_uid integer not null,
    constraint sandbox_gw_depthdata_pkey primary key (gw_depthdata_raw_uid),
    constraint sandbox_gw_depthdata_uniqueness unique (ow_uid, dtime),
    constraint sandbox_ow_uid_fkey foreign key (ow_uid)
        references fieldwork.tbl_ow (ow_uid) match simple
        on update cascade
        on delete restrict
        not valid
);

create table if not exists data.test_tbl_ow_leveldata_raw
(
    ow_leveldata_raw_uid integer not null generated always as identity,
    dtime timestamp with time zone not null,
    level_ft numeric(8,4) not null,
    ow_uid integer not null,
    date_added date default ('now'::text)::date,
    constraint sandbox_ow_leveldata_raw_pkey primary key (ow_leveldata_raw_uid),
    constraint sandbox_ow_leveldata_uniqueness unique (ow_uid, dtime),
    constraint sandbox_ow_uid_fkey foreign key (ow_uid)
        references fieldwork.tbl_ow (ow_uid) match simple
        on update cascade
        on delete restrict
        not valid
);

-- radar rainfall

create table if not exists data.test_tbl_radar_event
(
    radar_event_uid integer not null generated always as identity,
    radar_uid integer not null,
    eventdatastart timestamp with time zone not null,
    eventdataend timestamp with time zone not null,
    eventduration_hr numeric(8,4) not null,
    eventpeakintensity_inhr numeric(8,4) not null,
    eventavgintensity_inhr numeric(8,4) not null,
    eventdepth_in numeric(8,4) not null,
    constraint sandbox_radar_event_pkey primary key (radar_event_uid),
    constraint sandbox_radar_event_uniqueness unique (radar_uid, eventdatastart),
    constraint sandbox_radar_uid_fkey foreign key (radar_uid)
        references admin.tbl_radar (radar_uid) match simple
        on update cascade
        on delete no action
);

create table if not exists data.test_tbl_radar_rain
(
    radar_rain_uid integer not null generated always as identity,
    radar_uid integer not null,
    rainfall_in numeric(8,4) not null,
    dtime timestamp with time zone not null,
    constraint sandbox_radar_rain_pkey primary key (radar_rain_uid),
    constraint sandbox_radar_rain_uniqueness unique (radar_uid, dtime)
);

-- baro
CREATE TABLE IF NOT EXISTS data.test_tbl_baro
(
    baro_uid integer NOT NULL generated always as identity,
    dtime timestamp with time zone NOT NULL,
    baro_psi numeric(6,4) NOT NULL,
    baro_rawfile_uid integer NOT NULL,
    temp_f numeric(8,4),
    CONSTRAINT sandbox_baro_pkey PRIMARY KEY (baro_uid),
    CONSTRAINT sandbox_baro_uniqueness UNIQUE (baro_uid, dtime),
    CONSTRAINT sandbox_baro_rawfile_fkey FOREIGN KEY (baro_rawfile_uid)
        REFERENCES admin.tbl_baro_rawfile (baro_rawfile_uid) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE NO ACTION
)