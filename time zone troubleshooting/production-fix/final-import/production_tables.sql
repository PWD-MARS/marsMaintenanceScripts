-- Gage rainfall

create table if not exists data.test_tbl_gage_rain
(
    gage_rain_uid integer not null generated always as identity,
    gage_uid integer not null,
    dtime_local timestamp with time zone not null,
    rainfall_in numeric(8,4) not null,
    constraint local_gage_rain_pkey primary key (gage_rain_uid),
    constraint local_gage_rain_uniqueness unique (gage_uid, dtime_local),
    constraint local_gage_uid_fkey foreign key (gage_uid)
        references admin.tbl_gage (gage_uid) match simple
        on update cascade
        on delete no action
);

create table if not exists data.test_tbl_gage_event
(
    gage_event_uid integer not null generated always as identity,
    gage_uid integer not null,
    eventdatastart_local timestamp with time zone not null,
    eventdataend_local timestamp with time zone not null,
    eventduration_hr numeric(8,4) not null,
    eventpeakintensity_inhr numeric(8,4) not null,
    eventavgintensity_inhr numeric(8,4) not null,
    eventdepth_in numeric(8,4) not null,
    constraint local_gage_event_pkey primary key (gage_event_uid),
    constraint local_gage_uid_fkey foreign key (gage_uid)
        references admin.tbl_gage (gage_uid) match simple
        on update cascade
        on delete no action
);

-- GW and OW

create table if not exists data.test_tbl_gw_depthdata_raw
(
    gw_depthdata_raw_uid integer not null generated always as identity,
    dtime_local timestamp with time zone not null,
    depth_ft numeric(8,6) not null,
    ow_uid integer not null,
    constraint local_gw_depthdata_pkey primary key (gw_depthdata_raw_uid),
    constraint local_gw_depthdata_uniqueness unique (ow_uid, dtime_local),
    constraint local_ow_uid_fkey foreign key (ow_uid)
        references fieldwork.tbl_ow (ow_uid) match simple
        on update cascade
        on delete restrict
        not valid
);

create table if not exists data.test_tbl_ow_leveldata_raw
(
    ow_leveldata_raw_uid integer not null generated always as identity,
    dtime_local timestamp with time zone not null,
    level_ft numeric(8,4) not null,
    ow_uid integer not null,
    date_added date default ('now'::text)::date,
    constraint local_ow_leveldata_raw_pkey primary key (ow_leveldata_raw_uid),
    constraint local_ow_leveldata_uniqueness unique (ow_uid, dtime_local),
    constraint local_ow_uid_fkey foreign key (ow_uid)
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
    eventdatastart_local timestamp with time zone not null,
    eventdataend_local timestamp with time zone not null,
    eventduration_hr numeric(8,4) not null,
    eventpeakintensity_inhr numeric(8,4) not null,
    eventavgintensity_inhr numeric(8,4) not null,
    eventdepth_in numeric(8,4) not null,
    constraint local_radar_event_pkey primary key (radar_event_uid),
    constraint local_radar_event_uniqueness unique (radar_uid, eventdatastart_local),
    constraint local_radar_uid_fkey foreign key (radar_uid)
        references admin.tbl_radar (radar_uid) match simple
        on update cascade
        on delete no action
);

create table if not exists data.test_tbl_radar_rain
(
    radar_rain_uid integer not null generated always as identity,
    radar_uid integer not null,
    rainfall_in numeric(8,4) not null,
    dtime_local timestamp with time zone not null,
    constraint local_radar_rain_pkey primary key (radar_rain_uid),
    constraint local_radar_rain_uniqueness unique (radar_uid, dtime_local)
)

-- baro
CREATE TABLE IF NOT EXISTS data.test_tbl_baro
(
    baro_uid integer NOT NULL generated always as identity,
    dtime_local timestamp with time zone NOT NULL,
    baro_psi numeric(6,4) NOT NULL,
    baro_rawfile_uid integer NOT NULL,
    temp_f numeric(8,4),
    dtime_est timestamp with time zone not null, -- Shim field to transparently serve updated data to end user without updating app first. To be removed when app is updated to target _local field
    CONSTRAINT test_baro_pkey PRIMARY KEY (baro_uid),
    CONSTRAINT test_baro_uniqueness UNIQUE (baro_uid, dtime_est),
    CONSTRAINT test_baro_rawfile_fkey FOREIGN KEY (baro_rawfile_uid)
        REFERENCES admin.tbl_baro_rawfile (baro_rawfile_uid) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE NO ACTION
)