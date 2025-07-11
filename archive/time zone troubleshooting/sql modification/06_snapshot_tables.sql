-- Create new table for snapshot metadata
drop table if exists metrics.tbl_snapshot_hokeypokey;

CREATE TABLE IF NOT EXISTS metrics.tbl_snapshot_hokeypokey
(
    snapshot_metadata_uid integer NOT NULL generated always as identity,
    snapshot_uid integer NOT NULL,
    ow_uid integer NOT NULL,
    date_start date NOT NULL,
    date_end date NOT NULL,
    md5hash text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT snapshot_hokeypokey_pkey PRIMARY KEY (snapshot_metadata_uid),
    CONSTRAINT ow_uid_fkey FOREIGN KEY (ow_uid)
        REFERENCES fieldwork.tbl_ow (ow_uid) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE RESTRICT
        NOT VALID,
    CONSTRAINT snapshot_uid_fkey FOREIGN KEY (snapshot_uid)
        REFERENCES metrics.tbl_snapshot (snapshot_uid) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS metrics.tbl_snapshot_hokeypokey
    OWNER to mars_admin;

REVOKE ALL ON TABLE metrics.tbl_snapshot_hokeypokey FROM mars_readonly;
REVOKE ALL ON TABLE metrics.tbl_snapshot_hokeypokey FROM mars_shiny;

GRANT ALL ON TABLE metrics.tbl_snapshot_hokeypokey TO mars_admin;

GRANT SELECT ON TABLE metrics.tbl_snapshot_hokeypokey TO mars_readonly;

GRANT DELETE, INSERT, SELECT, UPDATE ON TABLE metrics.tbl_snapshot_hokeypokey TO mars_shiny;


-- populate that table with metadata from the old table
insert into metrics.tbl_snapshot_hokeypokey (snapshot_uid, ow_uid, date_start, date_end, md5hash)
   select snapshot_uid, ow_uid, date_start_est::date, date_end_est::date, md5hash
   from metrics.tbl_snapshot_metadata;


-- drop the old table and rename the new table to the correct name
drop table metrics.tbl_snapshot_metadata;

alter table metrics.tbl_snapshot_hokeypokey
	rename to tbl_snapshot_metadata;


-- drop the trigger function and trigger from tbl_snapshot
drop function metrics.fun_insert_snap_meta() cascade;

-- Replace the other trigger functions that expect the date fields to be TS/no TZ
DROP FUNCTION IF EXISTS metrics.fun_update_date_end_snap();

CREATE OR REPLACE FUNCTION metrics.fun_update_date_end_snap()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF SECURITY DEFINER
AS $BODY$
BEGIN 
UPDATE metrics.tbl_snapshot_metadata 
	SET date_end = date_trunc('day', NOW())
		WHERE date_end = '9999-12-31' AND ow_uid = NEW.ow_uid;
	RETURN NEW;
END
$BODY$;

ALTER FUNCTION metrics.fun_update_date_end_snap()
    OWNER TO mars_admin;

GRANT EXECUTE ON FUNCTION metrics.fun_update_date_end_snap() TO PUBLIC;

GRANT EXECUTE ON FUNCTION metrics.fun_update_date_end_snap() TO mars_admin;

GRANT EXECUTE ON FUNCTION metrics.fun_update_date_end_snap() TO mars_analyst;

GRANT EXECUTE ON FUNCTION metrics.fun_update_date_end_snap() TO mars_readonly;

GRANT EXECUTE ON FUNCTION metrics.fun_update_date_end_snap() TO mars_shiny;


-- Add the trigger that the old table had to the new table
-- We didn't do this before because it would have interfered with the insert into statement above
CREATE OR REPLACE TRIGGER update_end_date
    BEFORE INSERT
    ON metrics.tbl_snapshot_metadata
    FOR EACH ROW
    EXECUTE FUNCTION metrics.fun_update_date_end_snap();

CREATE OR REPLACE FUNCTION metrics.fun_insert_snap_meta()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
	old_snapshot_exists boolean := EXISTS (SELECT TRUE FROM metrics.tbl_snapshot_metadata sm WHERE sm.ow_uid = NEW.ow_uid LIMIT 1);
	old_snapshot_date_start date := date_start FROM metrics.tbl_snapshot_metadata sm WHERE sm.ow_uid = NEW.ow_uid ORDER BY date_start DESC LIMIT 1;
BEGIN
INSERT INTO metrics.tbl_snapshot_metadata(snapshot_uid, ow_uid, date_start, date_end, md5hash)
				SELECT ss.snapshot_uid, 
					   ss.ow_uid,
					   CASE WHEN old_snapshot_exists = TRUE AND ss.old_stays_valid = TRUE THEN date_trunc('day', NOW())
					   		WHEN old_snapshot_exists = TRUE AND ss.old_stays_valid = FALSE THEN old_snapshot_date_start
					   						   ELSE '01-01-2001'
											   END,
					   '9999-12-31',
					   md5(ROW(ss.ow_uid, ss.dcia_ft2, ss.storage_footprint_ft2, ss.orifice_diam_in,
							   ss.infil_footprint_ft2,ss.storage_depth_ft, ss.lined, ss.surface,
							   ss.storage_volume_ft3, ss.infil_dsg_rate_inhr)::text) AS md5_hash
					FROM metrics.tbl_snapshot ss
					LEFT JOIN external.viw_greenit_unified g on g.ow_uid = ss.ow_uid
					WHERE ss.snapshot_uid = NEW.snapshot_uid;
					
			RETURN NEW;		
END
$BODY$;

ALTER FUNCTION metrics.fun_insert_snap_meta()
    OWNER TO mars_admin;

GRANT EXECUTE ON FUNCTION metrics.fun_insert_snap_meta() TO PUBLIC;

GRANT EXECUTE ON FUNCTION metrics.fun_insert_snap_meta() TO mars_admin;

GRANT EXECUTE ON FUNCTION metrics.fun_insert_snap_meta() TO mars_analyst;

GRANT EXECUTE ON FUNCTION metrics.fun_insert_snap_meta() TO mars_readonly;

GRANT EXECUTE ON FUNCTION metrics.fun_insert_snap_meta() TO mars_shiny;

-- Replace the trigger on tbl_snapshot
CREATE OR REPLACE TRIGGER write_to_metadata
    AFTER INSERT
    ON metrics.tbl_snapshot
    FOR EACH ROW
    EXECUTE FUNCTION metrics.fun_insert_snap_meta();
