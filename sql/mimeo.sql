-- ########## mimeo table definitions ##########
CREATE TABLE mviews (
    mv_name text NOT NULL,
    v_name text NOT NULL,
    last_refresh timestamp with time zone,
    CONSTRAINT mviews_mv_name_pkey PRIMARY KEY (mv_name)
);
SELECT pg_catalog.pg_extension_config_dump('mviews', '');

CREATE SEQUENCE dblink_mapping_data_source_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
CREATE TABLE dblink_mapping (
    data_source_id integer NOT NULL DEFAULT nextval('@extschema@.dblink_mapping_data_source_id_seq'),
    data_source text NOT NULL,
    username text NOT NULL,
    pwd text,
    dbh_attr text,
    CONSTRAINT dblink_mapping_data_source_id_pkey PRIMARY KEY (data_source_id)
);
SELECT pg_catalog.pg_extension_config_dump('dblink_mapping', '');
ALTER SEQUENCE dblink_mapping_data_source_id_seq OWNED BY dblink_mapping.data_source_id;

CREATE TYPE refresh_type AS ENUM ('snap', 'inserter', 'updater', 'dml', 'logdel');
CREATE TABLE refresh_config (
    dest_table text NOT NULL,
    type mimeo.refresh_type NOT NULL,
    dblink integer NOT NULL,
    last_value timestamp with time zone,
    filter text[],
    condition text
);
SELECT pg_catalog.pg_extension_config_dump('refresh_config', '');
CREATE RULE refresh_config_parent_nodata AS ON INSERT TO @extschema@.refresh_config DO INSTEAD NOTHING;

CREATE TABLE refresh_config_snap (LIKE @extschema@.refresh_config INCLUDING ALL) INHERITS (@extschema@.refresh_config);
SELECT pg_catalog.pg_extension_config_dump('refresh_config_snap', '');
ALTER TABLE @extschema@.refresh_config_snap ADD CONSTRAINT refresh_config_snap_dblink_fkey FOREIGN KEY (dblink) REFERENCES @extschema@.dblink_mapping(data_source_id);
ALTER TABLE @extschema@.refresh_config_snap ADD CONSTRAINT refresh_config_snap_dest_table_pkey PRIMARY KEY (dest_table);
ALTER TABLE @extschema@.refresh_config_snap ADD COLUMN source_table text NOT NULL;
ALTER TABLE @extschema@.refresh_config_snap ADD COLUMN post_script text[];
ALTER TABLE @extschema@.refresh_config_snap ALTER COLUMN type SET DEFAULT 'snap';
ALTER TABLE @extschema@.refresh_config_snap ADD CONSTRAINT refresh_config_snap_type_check CHECK (type = 'snap');

CREATE TABLE refresh_config_inserter (LIKE @extschema@.refresh_config INCLUDING ALL) INHERITS (@extschema@.refresh_config);
SELECT pg_catalog.pg_extension_config_dump('refresh_config_inserter', '');
ALTER TABLE @extschema@.refresh_config_inserter ADD CONSTRAINT refresh_config_inserter_dblink_fkey FOREIGN KEY (dblink) REFERENCES @extschema@.dblink_mapping(data_source_id);
ALTER TABLE @extschema@.refresh_config_inserter ADD CONSTRAINT refresh_config_inserter_dest_table_pkey PRIMARY KEY (dest_table);
ALTER TABLE @extschema@.refresh_config_inserter ADD COLUMN source_table text NOT NULL; 
ALTER TABLE @extschema@.refresh_config_inserter ADD COLUMN control text NOT NULL;   
ALTER TABLE @extschema@.refresh_config_inserter ADD COLUMN boundary interval;
ALTER TABLE @extschema@.refresh_config_inserter ADD COLUMN dst_active boolean NOT NULL DEFAULT true;
ALTER TABLE @extschema@.refresh_config_inserter ADD COLUMN dst_start int NOT NULL DEFAULT 30;
ALTER TABLE @extschema@.refresh_config_inserter ADD COLUMN dst_end int NOT NULL DEFAULT 230;
ALTER TABLE @extschema@.refresh_config_inserter ALTER COLUMN type SET DEFAULT 'inserter';
ALTER TABLE @extschema@.refresh_config_inserter ADD CONSTRAINT refresh_config_inserter_type_check CHECK (type = 'inserter');

CREATE TABLE refresh_config_updater (LIKE @extschema@.refresh_config INCLUDING ALL) INHERITS (@extschema@.refresh_config);
SELECT pg_catalog.pg_extension_config_dump('refresh_config_updater', '');
ALTER TABLE @extschema@.refresh_config_updater ADD CONSTRAINT refresh_config_updater_dblink_fkey FOREIGN KEY (dblink) REFERENCES @extschema@.dblink_mapping(data_source_id);
ALTER TABLE @extschema@.refresh_config_updater ADD CONSTRAINT refresh_config_updater_dest_table_pkey PRIMARY KEY (dest_table);
ALTER TABLE @extschema@.refresh_config_updater ADD COLUMN source_table text NOT NULL;
ALTER TABLE @extschema@.refresh_config_updater ADD COLUMN control text NOT NULL;  
ALTER TABLE @extschema@.refresh_config_updater ADD COLUMN boundary interval;
ALTER TABLE @extschema@.refresh_config_updater ADD COLUMN pk_field text[] NOT NULL; 
ALTER TABLE @extschema@.refresh_config_updater ADD COLUMN pk_type text[] NOT NULL;
ALTER TABLE @extschema@.refresh_config_updater ADD COLUMN dst_active boolean NOT NULL DEFAULT true;
ALTER TABLE @extschema@.refresh_config_updater ADD COLUMN dst_start int NOT NULL DEFAULT 30;
ALTER TABLE @extschema@.refresh_config_updater ADD COLUMN dst_end int NOT NULL DEFAULT 230;
ALTER TABLE @extschema@.refresh_config_updater ALTER COLUMN type SET DEFAULT 'updater';
ALTER TABLE @extschema@.refresh_config_updater ADD CONSTRAINT refresh_config_updater_type_check CHECK (type = 'updater');  

CREATE TABLE refresh_config_dml (LIKE @extschema@.refresh_config INCLUDING ALL) INHERITS (@extschema@.refresh_config);
SELECT pg_catalog.pg_extension_config_dump('refresh_config_dml', '');
ALTER TABLE @extschema@.refresh_config_dml ADD CONSTRAINT refresh_config_dml_dblink_fkey FOREIGN KEY (dblink) REFERENCES @extschema@.dblink_mapping(data_source_id);
ALTER TABLE @extschema@.refresh_config_dml ADD CONSTRAINT refresh_config_dml_dest_table_pkey PRIMARY KEY (dest_table);
ALTER TABLE @extschema@.refresh_config_dml ADD COLUMN source_table text NOT NULL;
ALTER TABLE @extschema@.refresh_config_dml ADD COLUMN control text NOT NULL;  
ALTER TABLE @extschema@.refresh_config_dml ADD COLUMN pk_field text[] NOT NULL; 
ALTER TABLE @extschema@.refresh_config_dml ADD COLUMN pk_type text[] NOT NULL;
ALTER TABLE @extschema@.refresh_config_dml ALTER COLUMN type SET DEFAULT 'dml';
ALTER TABLE @extschema@.refresh_config_dml ADD CONSTRAINT refresh_config_dml_type_check CHECK (type = 'dml');    

CREATE TABLE refresh_config_logdel (LIKE @extschema@.refresh_config INCLUDING ALL) INHERITS (@extschema@.refresh_config);
SELECT pg_catalog.pg_extension_config_dump('refresh_config_logdel', '');
ALTER TABLE @extschema@.refresh_config_logdel ADD CONSTRAINT refresh_config_logdel_dblink_fkey FOREIGN KEY (dblink) REFERENCES @extschema@.dblink_mapping(data_source_id);
ALTER TABLE @extschema@.refresh_config_logdel ADD CONSTRAINT refresh_config_logdel_dest_table_pkey PRIMARY KEY (dest_table);
ALTER TABLE @extschema@.refresh_config_logdel ADD COLUMN source_table text NOT NULL;
ALTER TABLE @extschema@.refresh_config_logdel ADD COLUMN control text NOT NULL;  
ALTER TABLE @extschema@.refresh_config_logdel ADD COLUMN pk_field text[] NOT NULL; 
ALTER TABLE @extschema@.refresh_config_logdel ADD COLUMN pk_type text[] NOT NULL;
ALTER TABLE @extschema@.refresh_config_logdel ALTER COLUMN type SET DEFAULT 'logdel';
ALTER TABLE @extschema@.refresh_config_logdel ADD CONSTRAINT refresh_config_logdel_type_check CHECK (type = 'logdel');
    

-- ########## mimeo function definitions ##########
/*
 *  Authentication for dblink
 */
CREATE FUNCTION auth(integer) RETURNS text
    LANGUAGE sql
    AS $$
    select data_source||' user='||username||' password='||pwd from @extschema@.dblink_mapping where data_source_id = $1; 
$$;


/*
 *  Debug function
 */
CREATE FUNCTION gdb(in_debug boolean, in_notice text) RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF in_debug THEN 
        RAISE NOTICE '%', in_notice;
    END IF;
END
$$;


/*
 *  Checks if DST time change has occured
 */
CREATE FUNCTION dst_change(date timestamp with time zone) RETURNS boolean
    LANGUAGE sql
    AS $$ 
    SELECT to_char( date_trunc('day', $1) , 'TZ' ) <> to_char( date_trunc( 'day', $1 ) + '1 day'::interval, 'TZ' ); 
$$;


/*
 *  Checks to see if the server is using UTC/GMT timezone. Returns TRUE if it is NOT (makes function logic easier)
 */
CREATE FUNCTION dst_utc_check() RETURNS boolean
    LANGUAGE sql
    AS $$
    SELECT to_char( date_trunc('day', now()) , 'TZ' ) <> 'UTC' AND to_char( date_trunc('day', now()) , 'TZ' ) <> 'GMT';
$$;


/*
 *  Function to run any SQL after object recreation due to schema changes on source
 */
CREATE FUNCTION post_script(p_dest_table text) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE
    v_post_script   text[];
    v_sql           text;
BEGIN
    
     SELECT post_script INTO v_post_script FROM @extschema@.refresh_config WHERE dest_table = p_dest_table;

    FOREACH v_sql IN ARRAY v_post_script LOOP
        RAISE NOTICE 'v_sql: %', v_sql;
        EXECUTE v_sql;
    END LOOP;
END
$$;


/*
 *  Snap refresh to repull all table data
 */
CREATE FUNCTION refresh_snap(p_destination text, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock          boolean; 
v_cols_n_types      text;
v_cols              text;
v_create_sql        text;
v_dblink_schema     text;
v_dblink            text;
v_dest_table        text;
v_exists            int;
v_insert_sql        text;
v_job_id            int;
v_jobmon_schema     text;
v_job_name          text;
v_lcols_array       text[];
v_local_sql         text;
v_l                 text;
v_match             boolean := 'f';
v_old_search_path   text;
v_parts             record;
v_post_script       text[];
v_rcols_array       text[];
v_refresh_snap      text;
v_remote_sql        text;
v_rowcount          bigint;
v_r                 text;
v_snap              text;
v_source_table      text;
v_step_id           int;
v_table_exists      int;
v_view_definition   text;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'notice', true );
END IF;

v_job_name := 'Refresh Snap: '||p_destination;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

-- Set custom search path to allow easier calls to other functions, especially job logging
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';

v_job_id := add_job(v_job_name);
PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);

-- Take advisory lock to prevent multiple calls to function overlapping and causing possible deadlock
v_adv_lock := pg_try_advisory_lock(hashtext('refresh_snap'), hashtext(v_job_name));
IF v_adv_lock = 'false' THEN
    v_step_id := add_step(v_job_id,'Obtaining advisory lock for job: '||v_job_name);
    PERFORM gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    PERFORM update_step(v_step_id, 'OK','Found concurrent job. Exiting gracefully');
    PERFORM close_job(v_job_id);
    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
    RETURN;
END IF;

v_step_id := add_step(v_job_id,'Grabbing Mapping, Building SQL');

SELECT source_table, dest_table, dblink, post_script INTO v_source_table, v_dest_table, v_dblink, v_post_script FROM refresh_config_snap
WHERE dest_table = p_destination; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: This table is not set up for snapshot replication: %',v_job_name; 
END IF;  

-- checking for current view

SELECT definition INTO v_view_definition FROM pg_views where
      ((schemaname || '.') || viewname)=v_dest_table;

v_exists := strpos(v_view_definition, 'snap1');
  IF v_exists > 0 THEN
    v_snap := '_snap2';
    ELSE
    v_snap := '_snap1';
 END IF;


v_refresh_snap := v_dest_table||v_snap;

PERFORM gdb(p_debug,'v_refresh_snap: '||v_refresh_snap::text);

-- init sql statements 

v_remote_sql := 'SELECT array_to_string(array_agg(attname),'','') as cols, array_to_string(array_agg(attname||'' ''||atttypid::regtype::text),'','') as cols_n_types FROM pg_attribute WHERE attnum > 0 AND attisdropped is false AND attrelid = ' || quote_literal(v_source_table) || '::regclass';
v_remote_sql := 'SELECT cols, cols_n_types FROM dblink(auth(' || v_dblink || '), ' || quote_literal(v_remote_sql) || ') t (cols text, cols_n_types text)';
perform gdb(p_debug,'v_remote_sql: '||v_remote_sql);
EXECUTE v_remote_sql INTO v_cols, v_cols_n_types;  
perform gdb(p_debug,'v_cols: '||v_cols);
perform gdb(p_debug,'v_cols_n_types: '||v_cols_n_types);

v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
v_insert_sql := 'INSERT INTO ' || v_refresh_snap || ' SELECT '||v_cols||' FROM dblink(auth('||v_dblink||'),'||quote_literal(v_remote_sql)||') t ('||v_cols_n_types||')';

PERFORM update_step(v_step_id, 'OK','Done');

v_step_id := add_step(v_job_id,'Truncate non-active snap table');

-- Create snap table if it doesn't exist
SELECT string_to_array(v_refresh_snap, '.') AS oparts INTO v_parts;
SELECT INTO v_table_exists count(1) FROM pg_tables
    WHERE  schemaname = v_parts.oparts[1] AND
           tablename = v_parts.oparts[2];
IF v_table_exists = 0 THEN

    PERFORM gdb(p_debug,'Snap table does not exist. Creating... ');
    
    v_create_sql := 'CREATE TABLE ' || v_refresh_snap || ' (' || v_cols_n_types || ')';
    perform gdb(p_debug,'v_create_sql: '||v_create_sql::text);
    EXECUTE v_create_sql;
ELSE 

/* Check local column definitions against remote and recreate table if different. Allows automatic recreation of
        snap tables if columns change (add, drop type change)  */  
    v_local_sql := 'SELECT array_agg(attname||'' ''||atttypid::regtype::text) as cols_n_types FROM pg_attribute WHERE attnum > 0 AND attisdropped is false AND attrelid = ' || quote_literal(v_refresh_snap) || '::regclass'; 
        
    PERFORM gdb(p_debug,'v_local_sql: '||v_local_sql::text);

    EXECUTE v_local_sql INTO v_lcols_array;
    SELECT string_to_array(v_cols_n_types, ',') AS cols INTO v_rcols_array;

    -- Check to see if there's a change in the column structure on the remote
    FOREACH v_r IN ARRAY v_rcols_array LOOP
        v_match := 'f';
        FOREACH v_l IN ARRAY v_lcols_array LOOP
            IF v_r = v_l THEN
                v_match := 't';
                EXIT;
            END IF;
        END LOOP;
    END LOOP;

    IF v_match = 'f' THEN
        EXECUTE 'DROP TABLE ' || v_refresh_snap;
        EXECUTE 'DROP VIEW ' || v_dest_table;
        v_create_sql := 'CREATE TABLE ' || v_refresh_snap || ' (' || v_cols_n_types || ')';
        PERFORM gdb(p_debug,'v_create_sql: '||v_create_sql::text);
        EXECUTE v_create_sql;
        v_step_id := add_step(v_job_id,'Source table structure changed.');
        PERFORM update_step(v_step_id, 'OK','Tables and view dropped and recreated. Please double-check snap table attributes (permissions, indexes, etc');
        PERFORM gdb(p_debug,'Source table structure changed. Tables and view dropped and recreated. Please double-check snap table attributes (permissions, indexes, etc)');

    END IF;
    -- truncate non-active snap table
    EXECUTE 'TRUNCATE TABLE ' || v_refresh_snap;

PERFORM update_step(v_step_id, 'OK','Done');
END IF;
-- populating snap table
v_step_id := add_step(v_job_id,'Inserting records into local table');
    PERFORM gdb(p_debug,'Inserting rows... '||v_insert_sql);
    EXECUTE v_insert_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
PERFORM update_step(v_step_id, 'OK','Inserted '||v_rowcount||' records');

IF v_rowcount IS NOT NULL THEN
     EXECUTE 'ANALYZE ' ||v_refresh_snap;

    SET statement_timeout='30 min';
    
    -- swap view
    v_step_id := add_step(v_job_id,'Swap view to '||v_refresh_snap);
    PERFORM gdb(p_debug,'Swapping view to '||v_refresh_snap);
    EXECUTE 'CREATE OR REPLACE VIEW '||v_dest_table||' AS SELECT * FROM '||v_refresh_snap;
    PERFORM update_step(v_step_id, 'OK','View Swapped');

    v_step_id := add_step(v_job_id,'Updating last value');
    UPDATE refresh_config_snap set last_value = now() WHERE dest_table = p_destination;  

    PERFORM update_step(v_step_id, 'OK','Done');

    -- Runs special sql to fix indexes, permissions, etc on recreated objects
    IF v_match = 'f' AND v_post_script IS NOT NULL THEN
        v_step_id := add_step(v_job_id,'Applying post_script sql commands due to schema change');
        PERFORM @extschema@.post_script(v_dest_table);
        PERFORM update_step(v_step_id, 'OK','Done');
    END IF;

    PERFORM close_job(v_job_id);
ELSE
    RAISE EXCEPTION 'No rows found in source table';
END IF;

-- Ensure old search path is reset for the current session
EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

PERFORM pg_advisory_unlock(hashtext('refresh_snap'), hashtext(v_job_name));

EXCEPTION
-- See if there's exception to handle for the timeout
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';
        IF v_step_id IS NULL THEN
            v_step_id := jobmon.add_step(v_job_id, 'EXCEPTION before first step logged');
        END IF;
        PERFORM update_step(v_step_id, 'BAD', 'ERROR: '||coalesce(SQLERRM,'unknown'));
        PERFORM fail_job(v_job_id);

        -- Ensure old search path is reset for the current session
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

        PERFORM pg_advisory_unlock(hashtext('refresh_snap'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;
END
$$;


/*
 *  Refresh insert only table based on timestamp control field
 */
CREATE FUNCTION refresh_inserter(p_destination text, p_limit integer DEFAULT 100000, p_repull boolean DEFAULT false, p_repull_start text DEFAULT NULL, p_repull_end text DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock          boolean; 
v_boundary          timestamptz;
v_cols_n_types      text;
v_cols              text;
v_control           text;
v_create_sql        text;
v_dblink_schema     text;
v_dblink            text;
v_delete_sql        text;
v_dest_table        text;
v_dst_active        boolean;
v_dst_check         boolean;
v_dst_start         int;
v_dst_end           int;
v_filter            text[]; 
v_full_refresh      boolean := false;
v_insert_sql        text;
v_job_id            int;
v_jobmon_schema     text;
v_job_name          text;
v_last_value_sql    text;
v_last_value        timestamptz;
v_now               timestamptz := now();
v_old_search_path   text;
v_remote_sql        text;
v_rowcount          bigint; 
v_source_table      text;
v_step_id           int;
v_tmp_table         text;

v_del_sql           text;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'warning', true );
END IF;

v_job_name := 'Refresh Inserter: '||p_destination;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

-- Set custom search path to allow easier calls to other functions, especially job logging
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';

v_job_id := add_job(v_job_name);
PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := pg_try_advisory_lock(hashtext('refresh_inserter'), hashtext(v_job_name));
IF v_adv_lock = 'false' THEN
    v_step_id := add_step(v_job_id,'Obtaining advisory lock for job: '||v_job_name);
    PERFORM gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    PERFORM update_step(v_step_id, 'OK','Found concurrent job. Exiting gracefully');
    PERFORM close_job(v_job_id);
    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
    RETURN;
END IF;

SELECT source_table
    , dest_table
    , 'tmp_'||replace(dest_table,'.','_')
    , dblink
    , control
    , last_value
    , now() - boundary::interval
    , filter
    , dst_active
    , dst_start
    , dst_end 
FROM refresh_config_inserter WHERE dest_table = p_destination 
INTO v_source_table, v_dest_table, v_tmp_table, v_dblink, v_control
    , v_last_value, v_boundary, v_filter, v_dst_active, v_dst_start, v_dst_end; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: no mapping found for %',v_job_name; 
END IF;  

-- Do not allow this function to run during DST time change if config option is true. Otherwise will miss data from source
IF v_dst_active THEN
    v_dst_check := @extschema@.dst_change(v_now);
    IF v_dst_check THEN 
        IF to_number(to_char(v_now, 'HH24MM'), '0000') > v_dst_start AND to_number(to_char(v_now, 'HH24MM'), '0000') < v_dst_end THEN
            v_step_id := jobmon.add_step( v_job_id, 'DST Check');
            PERFORM jobmon.update_step(v_step_id, 'OK', 'Job CANCELLED - Does not run during DST time change');
            PERFORM jobmon.close_job(v_job_id);
            PERFORM gdb(p_debug, 'Cannot run during DST time change');
            EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
            PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));
            RETURN;
        END IF;
    END IF;
END IF;

v_step_id := add_step(v_job_id,'Building SQL');

IF v_filter IS NULL THEN 
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||atttypid::regtype::text),',') FROM 
    pg_attribute WHERE attnum > 0 AND attisdropped is false AND attrelid = p_destination::regclass INTO v_cols, v_cols_n_types;
ELSE
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||atttypid::regtype::text),',') FROM 
        (SELECT unnest(filter) FROM @extschema@.refresh_config_inserter WHERE dest_table = p_destination) x 
         JOIN pg_attribute ON (unnest=attname::text AND attrelid=p_destination::regclass) INTO v_cols, v_cols_n_types;
END IF;    

-- init sql statements 

v_last_value_sql := 'SELECT max('||v_control||') FROM '||v_tmp_table;

-- Repull old data instead of normal new data pull
IF p_repull THEN
    -- Repull ALL data if no start and end values set
    IF p_repull_start IS NULL AND p_repull_end IS NULL THEN
        -- Actual truncate is done after pull to temp table to minimize lock on dest_table
        PERFORM update_step(v_step_id, 'OK','Request to repull ALL data from source. This could take a while...');
        v_full_refresh := true;
        v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
    ELSE
        PERFORM update_step(v_step_id, 'OK','Request to repull data from '||p_repull_start||' to '||p_repull_end);
        PERFORM gdb(p_debug,'Request to repull data from '||p_repull_start||' to '||p_repull_end);
        v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table||' WHERE '||v_control||' > '||quote_literal(p_repull_start)||' AND '||v_control||' < '||quote_literal(p_repull_end);
        -- Delete the old local data
        v_delete_sql := 'DELETE FROM '||v_dest_table||' WHERE '||v_control||' > '||quote_literal(p_repull_start)||' AND '||v_control||' < '||quote_literal(p_repull_end);
        v_step_id := add_step(v_job_id, 'Deleting current, local data');
        PERFORM gdb(p_debug,'Deleting current, local data');
        EXECUTE v_delete_sql;
        PERFORM update_step(v_step_id, 'OK','Done');
        -- Set last_value equal to local, real table max instead of temp table (just in case)
        v_last_value_sql := 'SELECT max('||v_control||') FROM '||v_dest_table;
    END IF;
ELSE

    -- does < for upper boundary to keep missing data from happening on rare edge case where a newly inserted row outside the transaction batch
    -- has the exact same timestamp as the previous batch's max timestamp
    v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table||' WHERE '||v_control||' > '||quote_literal(v_last_value)||' AND '||v_control||' < '||quote_literal(v_boundary)||' ORDER BY '||v_control||' ASC LIMIT '|| p_limit;

    PERFORM update_step(v_step_id, 'OK','Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);
    PERFORM gdb(p_debug,'Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);

END IF;

v_create_sql := 'CREATE TEMP TABLE '||v_tmp_table||' AS SELECT '||v_cols||' FROM dblink(auth('||v_dblink||'),'||quote_literal(v_remote_sql)||') t ('||v_cols_n_types||')';

v_insert_sql := 'INSERT INTO '||v_dest_table||'('||v_cols||') SELECT '||v_cols||' FROM '||v_tmp_table; 


-- create temp from remote
v_step_id := add_step(v_job_id,'Creating temp table ('||v_tmp_table||') from remote table');
    PERFORM gdb(p_debug,v_create_sql);
    EXECUTE v_create_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    -- Do nothing if no new rows
    IF v_rowcount < 1 THEN 
        PERFORM update_step(v_step_id, 'OK','No new rows found');
        EXECUTE 'DROP TABLE IF EXISTS ' || v_tmp_table;
        PERFORM close_job(v_job_id);
        PERFORM gdb(p_debug, 'No new rows found');
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));
        RETURN;
    -- Not recommended that the batch actually equal the limit set if possible. Handle all edge cases to keep data consistent
    ELSIF (v_rowcount = p_limit) AND (p_repull = false) THEN
        PERFORM update_step(v_step_id, 'WARNING','Row count fetched equal to limit set: '||p_limit||'. Recommend increasing batch limit if possible.');
        PERFORM gdb(p_debug, 'Row count fetched equal to limit set: '||p_limit||'. Recommend increasing batch limit if possible.'); 
        EXECUTE v_last_value_sql INTO v_last_value;
        v_step_id := add_step(v_job_id, 'Removing high boundary rows from this batch to avoid missing data');       
        EXECUTE 'DELETE FROM '||v_tmp_table||' WHERE '||v_control||' = '||quote_literal(v_last_value);
        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        PERFORM update_step(v_step_id, 'OK', 'Removed '||v_rowcount||' rows. Batch now contains '||p_limit - v_rowcount||' records');
        PERFORM gdb(p_debug, 'Removed '||v_rowcount||' rows from batch. Batch table now contains '||p_limit - v_rowcount||' records');
        IF (p_limit - v_rowcount) < 1 THEN
            v_step_id := add_step(v_job_id, 'Reached inconsistent state');
            PERFORM update_step(v_step_id, 'CRITICAL', 'Batch contained max rows ('||p_limit||') and all contained the same timestamp value. Unable to guarentee rows will ever be replicated consistently. Increase row limit parameter to allow a consistent batch.');
            PERFORM gdb(p_debug, 'Batch contained max rows desired ('||p_limit||') and all contained the same timestamp value. Unable to guarentee rows will be replicated consistently. Increase row limit parameter to allow a consistent batch.');
            PERFORM fail_job(v_job_id);
            EXECUTE 'DROP TABLE IF EXISTS ' || v_tmp_table;
            EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
            PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));
            RETURN;
        END IF;
    ELSE
        PERFORM update_step(v_step_id, 'OK','Batch contains '||v_rowcount||' records');
        PERFORM gdb(p_debug, 'Batch contains '||v_rowcount||' records');
    END IF;

IF v_full_refresh THEN        
        EXECUTE 'TRUNCATE '||v_dest_table;
END IF;

-- insert
v_step_id := add_step(v_job_id,'Inserting new records into local table');
    PERFORM gdb(p_debug,v_insert_sql);
    EXECUTE v_insert_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM update_step(v_step_id, 'OK','Inserted '||v_rowcount||' records');
    PERFORM gdb(p_debug, v_rowcount || ' rows added to ' || v_dest_table);

-- Get new last_value
v_step_id := add_step(v_job_id, 'Getting local max control field value for next lower boundary');
    PERFORM gdb(p_debug, v_last_value_sql);
    EXECUTE v_last_value_sql INTO v_last_value;
    PERFORM update_step(v_step_id, 'OK','Max value is: '||v_last_value);
    PERFORM gdb(p_debug, 'Max value is: '||v_last_value);

-- update boundries
v_step_id := add_step(v_job_id,'Updating last_value in config');
UPDATE refresh_config_inserter set last_value = v_last_value WHERE dest_table = p_destination;  
PERFORM update_step(v_step_id, 'OK','Done');

EXECUTE 'DROP TABLE IF EXISTS ' || v_tmp_table;

PERFORM close_job(v_job_id);

-- Ensure old search path is reset for the current session
EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));

EXCEPTION
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';
        IF v_step_id IS NULL THEN
            v_step_id := jobmon.add_step(v_job_id, 'EXCEPTION before first step logged');
        END IF;
        PERFORM update_step(v_step_id, 'BAD', 'ERROR: '||coalesce(SQLERRM,'unknown'));
        PERFORM fail_job(v_job_id);

        -- Ensure old search path is reset for the current session
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

        PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;    
END
$$;


/*
 *  Refresh insert/update only table based on timestamp control field
 */
CREATE FUNCTION refresh_updater(p_destination text, p_limit integer DEFAULT 100000, p_repull boolean DEFAULT false, p_repull_start text DEFAULT NULL, p_repull_end text DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock              boolean;
v_boundary_sql           text;
v_boundary               timestamptz;
v_cols_n_types          text;
v_cols                  text;
v_control               text;
v_create_sql            text;
v_dblink_schema         text;
v_dblink                text;
v_delete_sql            text;
v_dest_table            text;
v_dst_active            boolean;
v_dst_check             boolean;
v_dst_start             int;
v_dst_end               int;
v_field                 text;
v_filter                text[];
v_full_refresh          boolean := false;
v_insert_sql            text;
v_job_id                int;
v_jobmon_schema         text;
v_job_name              text;
v_last_value_sql        text; 
v_last_value            timestamptz;
v_now                   timestamptz := now(); 
v_old_search_path       text;
v_pk_counter            int := 2;
v_pk_field              text[];
v_pk_type               text[];
v_pk_where              text;
v_remote_boundry_sql    text;
v_remote_boundry        timestamptz;
v_remote_sql            text;
v_rowcount              bigint; 
v_source_table          text;
v_step_id               int;
v_tmp_table             text;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'warning', true );
END IF;

v_job_name := 'Refresh Updater: '||p_destination;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

-- Set custom search path to allow easier calls to other functions, especially job logging
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';


v_job_id := add_job(v_job_name);
PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := pg_try_advisory_lock(hashtext('refresh_updater'), hashtext(v_job_name));
IF v_adv_lock = 'false' THEN
    v_step_id := add_step(v_job_id,'Obtaining advisory lock for job: '||v_job_name);
    PERFORM gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    PERFORM update_step(v_step_id, 'OK','Found concurrent job. Exiting gracefully');
    PERFORM close_job(v_job_id);
    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
    RETURN;
END IF;

-- grab boundry
SELECT source_table
    , dest_table
    , 'tmp_'||replace(dest_table,'.','_')
    , dblink, control
    , last_value
    , now() - boundary::interval
    , pk_field
    , pk_type
    , filter
    , dst_active
    , dst_start
    , dst_end  
FROM refresh_config_updater
WHERE dest_table = p_destination INTO v_source_table, v_dest_table, v_tmp_table, v_dblink, v_control, 
    v_last_value, v_boundary, v_pk_field, v_pk_type, v_filter, v_dst_active, v_dst_start, v_dst_end;
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: no mapping found for %',v_job_name;
END IF;

-- Do not allow this function to run during DST time change if config option is true. Otherwise will miss data from source
IF v_dst_active THEN
    v_dst_check := @extschema@.dst_change(v_now);
    IF v_dst_check THEN 
        IF to_number(to_char(v_now, 'HH24MM'), '0000') > v_dst_start AND to_number(to_char(v_now, 'HH24MM'), '0000') < v_dst_end THEN
            v_step_id := jobmon.add_step( v_job_id, 'DST Check');
            PERFORM jobmon.update_step(v_step_id, 'OK', 'Job CANCELLED - Does not run during DST time change');
            PERFORM jobmon.close_job(v_job_id);
            PERFORM gdb(p_debug, 'Cannot run during DST time change');
            EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
            PERFORM pg_advisory_unlock(hashtext('refresh_updater'), hashtext(v_job_name));
            RETURN;
        END IF;
    END IF;
END IF;

v_step_id := add_step(v_job_id,'Building SQL');

-- determine column list, column type list
IF v_filter IS NULL THEN 
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||atttypid::regtype::text),',') FROM 
        pg_attribute WHERE attnum > 0 AND attisdropped is false AND attrelid = p_destination::regclass INTO v_cols, v_cols_n_types;
ELSE
    -- ensure all primary key columns are included in any column filters
    FOREACH v_field IN ARRAY v_pk_field LOOP
        IF v_field = ANY(v_filter) THEN
            CONTINUE;
        ELSE
            RAISE EXCEPTION 'ERROR: filter list did not contain all columns that compose primary key for %',v_job_name; 
        END IF;
    END LOOP;
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||atttypid::regtype::text),',') FROM 
        (SELECT unnest(filter) FROM refresh_config_updater WHERE dest_table = p_destination) x 
         JOIN pg_attribute ON (unnest=attname::text AND attrelid=p_destination::regclass) INTO v_cols, v_cols_n_types;
END IF;    

-- init sql statements 

v_last_value_sql := 'SELECT max('||v_control||') FROM '||v_tmp_table;

-- Repull old data instead of normal new data pull
IF p_repull THEN
    -- Repull ALL data if no start and end values set
    IF p_repull_start IS NULL AND p_repull_end IS NULL THEN
        -- Actual truncate is done after pull to temp table to minimize lock on dest_table
        PERFORM update_step(v_step_id, 'OK','Request to repull ALL data from source. This could take a while...');
        v_full_refresh := true;
        v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
    ELSE
        PERFORM update_step(v_step_id, 'OK','Request to repull data from '||p_repull_start||' to '||p_repull_end);
        PERFORM gdb(p_debug,'Request to repull data from '||p_repull_start||' to '||p_repull_end);
        v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table||' WHERE '||v_control||' > '||quote_literal(p_repull_start)||' AND '||v_control||' < '||quote_literal(p_repull_end);
        -- Delete the old local data. Unlike inserter, just do this in the normal delete step below
        v_delete_sql := 'DELETE FROM '||v_dest_table||' WHERE '||v_control||' > '||quote_literal(p_repull_start)||' AND '||v_control||' < '||quote_literal(p_repull_end);
        -- Set last_value equal to local, real table max instead of temp table (just in case)
        v_last_value_sql := 'SELECT max('||v_control||') FROM '||v_dest_table;
    END IF;
ELSE
    -- does < for upper boundary to keep missing data from happening on rare edge case where a newly inserted row outside the transaction batch
    -- has the exact same timestamp as the previous batch's max timestamp
    v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table||' WHERE '||v_control||' > '||quote_literal(v_last_value)||' AND '||v_control||' < '||quote_literal(v_boundary)||' ORDER BY '||v_control||' ASC LIMIT '|| p_limit;

    v_delete_sql := 'DELETE FROM '||v_dest_table||' USING '||v_tmp_table||' t WHERE '||v_dest_table||'.'||v_pk_field[1]||'=t.'||v_pk_field[1]; 

    PERFORM update_step(v_step_id, 'OK','Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);
    PERFORM gdb(p_debug,'Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);

END IF;


v_create_sql := 'CREATE TEMP TABLE '||v_tmp_table||' AS SELECT '||v_cols||' FROM dblink(auth('||v_dblink||'),'||quote_literal(v_remote_sql)||') t ('||v_cols_n_types||')';


IF array_length(v_pk_field, 1) > 1 THEN
    v_pk_where := '';
    WHILE v_pk_counter <= array_length(v_pk_field,1) LOOP
        v_pk_where := v_pk_where || ' AND '||v_dest_table||'.'||v_pk_field[v_pk_counter]||' = t.'||v_pk_field[v_pk_counter];
        v_pk_counter := v_pk_counter + 1;
    END LOOP;
END IF;

IF v_pk_where IS NOT NULL THEN
    v_delete_sql := v_delete_sql || v_pk_where;
END IF; 

v_insert_sql := 'INSERT INTO '||v_dest_table||'('||v_cols||') SELECT '||v_cols||' FROM '||v_tmp_table; 

-- create temp from remote
v_step_id := add_step(v_job_id,'Creating temp table ('||v_tmp_table||') from remote table');
    PERFORM gdb(p_debug,v_create_sql);
    EXECUTE v_create_sql;     
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    IF v_rowcount < 1 THEN 
        PERFORM update_step(v_step_id, 'OK','No new rows found');
        EXECUTE 'DROP TABLE IF EXISTS ' || v_tmp_table;
        PERFORM close_job(v_job_id);
        PERFORM gdb(p_debug, 'No new rows found');
        -- Ensure old search path is reset for the current session
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        PERFORM pg_advisory_unlock(hashtext('refresh_updater'), hashtext(v_job_name));
        RETURN;
    -- Not recommended that the batch actually equal the limit set if possible.
    ELSIF v_rowcount = p_limit THEN
        PERFORM update_step(v_step_id, 'WARNING','Row count fetched equal to limit set: '||p_limit||'. Recommend increasing batch limit if possible.');
        PERFORM gdb(p_debug, 'Row count fetched equal to limit set: '||p_limit||'. Recommend increasing batch limit if possible.'); 
        EXECUTE v_last_value_sql INTO v_last_value;
        v_step_id := add_step(v_job_id, 'Removing high boundary rows from this batch to avoid missing data');       
        EXECUTE 'DELETE FROM '||v_tmp_table||' WHERE '||v_control||' = '||quote_literal(v_last_value);
        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        PERFORM update_step(v_step_id, 'OK', 'Removed '||v_rowcount||' rows. Batch now contains '||p_limit - v_rowcount||' records');
        PERFORM gdb(p_debug, 'Removed '||v_rowcount||' rows from batch. Batch table now contains '||p_limit - v_rowcount||' records');
        IF (p_limit - v_rowcount) < 1 THEN
            v_step_id := add_step(v_job_id, 'Reached inconsistent state');
            PERFORM update_step(v_step_id, 'CRITICAL', 'Batch contained max rows ('||p_limit||') and all contained the same timestamp value. Unable to guarentee rows will ever be replicated consistently. Increase row limit parameter to allow a consistent batch.');
            PERFORM gdb(p_debug, 'Batch contained max rows desired ('||p_limit||') and all contained the same timestamp value. Unable to guarentee rows will be replicated consistently. Increase row limit parameter to allow a consistent batch.');
            PERFORM fail_job(v_job_id);
            EXECUTE 'DROP TABLE IF EXISTS ' || v_tmp_table;
            EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
            PERFORM pg_advisory_unlock(hashtext('refresh_updater'), hashtext(v_job_name));
            RETURN;
        END IF;
    ELSE
        PERFORM update_step(v_step_id, 'OK','Batch contains '||v_rowcount||' records');
        PERFORM gdb(p_debug, 'Batch contains '||v_rowcount||' records');
    END IF;

IF v_full_refresh THEN        
        EXECUTE 'TRUNCATE '||v_dest_table;
ELSE
    -- delete records to be updated. This step not needed during full refresh
    v_step_id := add_step(v_job_id,'Deleting records marked for update in local table');
        perform gdb(p_debug,v_delete_sql);
        execute v_delete_sql; 
        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM update_step(v_step_id, 'OK','Deleted '||v_rowcount||' records');
END IF;

-- insert
v_step_id := add_step(v_job_id,'Inserting new records into local table');
    perform gdb(p_debug,v_insert_sql);
    execute v_insert_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
PERFORM update_step(v_step_id, 'OK','Inserted '||v_rowcount||' records');

-- Get new last_value
v_step_id := add_step(v_job_id, 'Getting local max control field value for next lower boundary');
    PERFORM gdb(p_debug, v_last_value_sql);
    EXECUTE v_last_value_sql INTO v_last_value;
    PERFORM update_step(v_step_id, 'OK','Max value is: '||v_last_value);
    PERFORM gdb(p_debug, 'Max value is: '||v_last_value);

-- update boundries
v_step_id := add_step(v_job_id,'Updating last_value in config');
UPDATE refresh_config_updater set last_value = v_last_value WHERE dest_table = p_destination;  
PERFORM update_step(v_step_id, 'OK','Done');

EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table;

PERFORM close_job(v_job_id);

-- Ensure old search path is reset for the current session
EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

PERFORM pg_advisory_unlock(hashtext('refresh_updater'), hashtext(v_job_name));

EXCEPTION
    WHEN others THEN
        -- Exception block resets path, so have to reset it again
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';
        IF v_step_id IS NULL THEN
            v_step_id := jobmon.add_step(v_job_id, 'EXCEPTION before first step logged');
        END IF;
        PERFORM update_step(v_step_id, 'BAD', 'ERROR: '||coalesce(SQLERRM,'unknown'));
        PERFORM fail_job(v_job_id);

        -- Ensure old search path is reset for the current session
       EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

        PERFORM pg_advisory_unlock(hashtext('refresh_updater'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;
END
$$;


/*
 *  Refresh based on DML (Insert, Update, Delete)
 */
DROP FUNCTION refresh_dml(text, boolean, int);
CREATE FUNCTION refresh_dml(p_destination text, p_limit int default 100000, p_repull boolean DEFAULT false, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock          boolean;
v_cols_n_types      text;
v_cols              text;
v_control           text;
v_create_f_sql      text;
v_dblink_schema     text;
v_dblink            text;
v_delete_sql        text;
v_dest_table        text;
v_exec_status       text;
v_field             text;
v_filter            text[];
v_insert_sql        text;
v_job_id            int;
v_jobmon_schema     text;
v_job_name          text;
v_last_value_sql    text; 
v_old_search_path   text;
v_pk_counter        int := 2;
v_pk_field_csv      text;
v_pk_field          text[];
v_pk_type           text[];
v_pk_where          text;
v_remote_f_sql      text;
v_remote_q_sql      text;
v_rowcount          bigint; 
v_source_table      text;
v_step_id           int;
v_tmp_table         text;
v_trigger_delete    text; 
v_trigger_update    text;
v_with_update       text;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'warning', true );
END IF;

v_job_name := 'Refresh DML: '||p_destination;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

-- Set custom search path to allow easier calls to other functions, especially job logging
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';

SELECT source_table, dest_table, 'tmp_'||replace(dest_table,'.','_'), dblink, control, pk_field, pk_type, filter FROM refresh_config_dml 
WHERE dest_table = p_destination INTO v_source_table, v_dest_table, v_tmp_table, v_dblink, v_control, v_pk_field, v_pk_type, v_filter; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: no mapping found for %',v_job_name; 
END IF;

v_job_id := add_job(quote_literal(v_job_name));
PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := pg_try_advisory_lock(hashtext('refresh_dml'), hashtext(v_job_name));
IF v_adv_lock = 'false' THEN
    v_step_id := add_step(v_job_id,'Obtaining advisory lock for job: '||v_job_name);
    PERFORM gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    PERFORM update_step(v_step_id, 'OK','Found concurrent job. Exiting gracefully');
    PERFORM close_job(v_job_id);
    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
    RETURN;
END IF;

v_step_id := add_step(v_job_id,'Building SQL');

IF v_pk_field IS NULL OR v_pk_type IS NULL THEN
    RAISE EXCEPTION 'ERROR: primary key fields in refresh_config_dml must be defined';
END IF;

-- determine column list, column type list
IF v_filter IS NULL THEN 
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||atttypid::regtype::text),',') FROM 
        pg_attribute WHERE attnum > 0 AND attisdropped is false AND attrelid = p_destination::regclass INTO v_cols, v_cols_n_types;
ELSE
    -- ensure all primary key columns are included in any column filters
    FOREACH v_field IN ARRAY v_pk_field LOOP
        IF v_field = ANY(v_filter) THEN
            CONTINUE;
        ELSE
            RAISE EXCEPTION 'ERROR: filter list did not contain all columns that compose primary key for %',v_job_name; 
        END IF;
    END LOOP;
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||atttypid::regtype::text),',') FROM 
        (SELECT unnest(filter) FROM refresh_config_dml WHERE dest_table = p_destination) x 
         JOIN pg_attribute ON (unnest=attname::text AND attrelid=p_destination::regclass) INTO v_cols, v_cols_n_types;
END IF;    

-- init sql statements 

v_pk_field_csv := array_to_string(v_pk_field,',');
v_with_update := 'WITH a AS (SELECT '||v_pk_field_csv||' FROM '|| v_control ||' ORDER BY 1 LIMIT '|| p_limit ||') UPDATE '||v_control||' b SET processed = true FROM a WHERE a.'||v_pk_field[1]||' = b.'||v_pk_field[1];

IF array_length(v_pk_field, 1) > 1 THEN
    v_pk_where := '';
    WHILE v_pk_counter <= array_length(v_pk_field,1) LOOP
        v_pk_where := v_pk_where || ' AND a.'||v_pk_field[v_pk_counter]||' = b.'||v_pk_field[v_pk_counter];
        v_pk_counter := v_pk_counter + 1;
    END LOOP;
END IF;

IF v_pk_where IS NOT NULL THEN
    v_with_update := v_with_update || v_pk_where;
END IF;
PERFORM gdb(p_debug, v_with_update);

v_trigger_update := 'SELECT dblink_exec(auth('||v_dblink||'),'|| quote_literal(v_with_update)||')';

v_remote_q_sql := 'SELECT DISTINCT '||v_pk_field_csv||' FROM '||v_control||' WHERE processed = true';

IF p_repull THEN
    v_remote_f_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
        -- Actual truncate is done after pull to temp table to minimize lock on dest_table
    PERFORM update_step(v_step_id, 'OK','Request to repull ALL data from source. This could take a while...');
    PERFORM gdb(p_debug, 'Request to repull ALL data from source. This could take a while...');

ELSE 
    v_remote_f_sql := 'SELECT '||v_cols||' FROM '||v_source_table||' JOIN ('||v_remote_q_sql||') x USING ('||v_pk_field_csv||')';
    
    v_delete_sql := 'DELETE FROM '||v_dest_table||' a USING '||v_tmp_table||'_full b WHERE a.'||v_pk_field[1]||'= b.'||v_pk_field[1];
    IF array_length(v_pk_field, 1) > 1 THEN
        v_delete_sql := v_delete_sql || v_pk_where;
    END IF; 

    PERFORM update_step(v_step_id, 'OK','Done');

END IF;

v_create_f_sql := 'CREATE TEMP TABLE '||v_tmp_table||'_full AS SELECT '||v_cols||' 
        FROM dblink(auth('||v_dblink||'),'||quote_literal(v_remote_f_sql)||') t ('||v_cols_n_types||')';

v_insert_sql := 'INSERT INTO '||v_dest_table||'('||v_cols||') SELECT '||v_cols||' FROM '||v_tmp_table||'_full'; 

v_trigger_delete := 'SELECT dblink_exec(auth('||v_dblink||'),'||quote_literal('DELETE FROM '||v_control||' WHERE processed = true')||')'; 



-- update remote entries
v_step_id := add_step(v_job_id,'Updating remote trigger table');
    PERFORM gdb(p_debug,v_trigger_update);
    EXECUTE v_trigger_update INTO v_exec_status;    
PERFORM update_step(v_step_id, 'OK','Result was '||v_exec_status);

-- create temp table for insertion 
v_step_id := add_step(v_job_id,'Create temp table from remote full table');
    PERFORM gdb(p_debug,v_create_f_sql);
    EXECUTE v_create_f_sql;  
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM gdb(p_debug,'Temp table row count '||v_rowcount::text);
    IF v_rowcount < 1 THEN 
        PERFORM update_step(v_step_id, 'OK','No new rows found');
        EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_full';
        PERFORM close_job(v_job_id);
        -- Ensure old search path is reset for the current session
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        PERFORM pg_advisory_unlock(hashtext('refresh_dml'), hashtext(v_job_name));
        RETURN;
    END IF;
PERFORM update_step(v_step_id, 'OK','Table contains '||v_rowcount||' records');

-- remove records from local table 
IF p_repull THEN
    v_step_id := add_step(v_job_id,'Truncating local table');
    PERFORM gdb(p_debug,'Truncating local table');
    EXECUTE 'TRUNCATE '||v_dest_table;
    PERFORM update_step(v_step_id, 'OK','Done');
ELSE
    v_step_id := add_step(v_job_id,'Deleting records from local table');
        PERFORM gdb(p_debug,v_delete_sql);
        EXECUTE v_delete_sql; 
        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        PERFORM gdb(p_debug,'Rows removed from local table before applying changes: '||v_rowcount::text);
        PERFORM update_step(v_step_id, 'OK','Removed '||v_rowcount||' records');
END IF;

-- insert records to local table
v_step_id := add_step(v_job_id,'Inserting new records into local table');
    PERFORM gdb(p_debug,v_insert_sql);
    EXECUTE v_insert_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM gdb(p_debug,'Rows inserted: '||v_rowcount::text);
PERFORM update_step(v_step_id, 'OK','Inserted '||v_rowcount||' records');

-- clean out rows from txn table
v_step_id := add_step(v_job_id,'Cleaning out rows from txn table');
    PERFORM gdb(p_debug,v_trigger_delete);
    EXECUTE v_trigger_delete INTO v_exec_status;
PERFORM update_step(v_step_id, 'OK','Result was '||v_exec_status);

-- update activity status
v_step_id := add_step(v_job_id,'Updating last_value in config table');
    v_last_value_sql := 'UPDATE refresh_config_dml SET last_value = '|| quote_literal(current_timestamp::timestamp) ||' WHERE dest_table = ' ||quote_literal(p_destination); 
    PERFORM gdb(p_debug,v_last_value_sql);
    EXECUTE v_last_value_sql; 
PERFORM update_step(v_step_id, 'OK','Last Value was '||current_timestamp);

PERFORM close_job(v_job_id);

EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_full';

-- Ensure old search path is reset for the current session
EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

PERFORM pg_advisory_unlock(hashtext('refresh_dml'), hashtext(v_job_name));

EXCEPTION
    WHEN others THEN
        -- Exception block resets path, so have to reset it again
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';
        IF v_step_id IS NULL THEN
            v_step_id := jobmon.add_step(v_job_id, 'EXCEPTION before first step logged');
        END IF;
        PERFORM update_step(v_step_id, 'BAD', 'ERROR: '||coalesce(SQLERRM,'unknown'));
        PERFORM fail_job(v_job_id);

        -- Ensure old search path is reset for the current session
       EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

        PERFORM pg_advisory_unlock(hashtext('refresh_dml'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;
END
$$;


/*
 *  Refresh based on DML (Insert, Update, Delete), but logs all deletes on the destination table
 *  Destination table requires extra column: source_deleted timestamptz
 */
CREATE FUNCTION refresh_logdel(p_destination text, p_limit int default 100000, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock              boolean;
v_cols_n_types          text;
v_cols                  text;
v_control               text;
v_create_d_sql          text;
v_create_f_sql          text;
v_dblink_schema         text;
v_dblink                text;
v_delete_d_sql          text;
v_delete_f_sql          text;
v_dest_table            text;
v_exec_status           text;
v_field                 text;
v_filter                text[];
v_insert_deleted_sql    text;
v_insert_sql            text;
v_job_id                int;
v_jobmon_schema         text;
v_job_name              text;
v_last_value_sql        text; 
v_old_search_path       text;
v_pk_counter            int := 2;
v_pk_field_csv          text;
v_pk_field              text[];
v_pk_type               text[];
v_pk_where              text;
v_remote_d_sql          text;
v_remote_f_sql          text;
v_remote_q_sql          text;
v_rowcount              bigint; 
v_source_table          text;
v_step_id               int;
v_tmp_table             text;
v_trigger_delete        text; 
v_trigger_update        text;
v_with_update           text;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'warning', true );
END IF;

v_job_name := 'Refresh Log Del: '||p_destination;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

-- Set custom search path to allow easier calls to other functions, especially job logging
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';

SELECT source_table, dest_table, 'tmp_'||replace(dest_table,'.','_'), dblink, control, pk_field, pk_type, filter FROM refresh_config_logdel 
WHERE dest_table = p_destination INTO v_source_table, v_dest_table, v_tmp_table, v_dblink, v_control, v_pk_field, v_pk_type, v_filter; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: no mapping found for %',v_job_name; 
END IF;

v_job_id := add_job(quote_literal(v_job_name));
PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := pg_try_advisory_lock(hashtext('refresh_logdel'), hashtext(v_job_name));
IF v_adv_lock = 'false' THEN
    v_step_id := add_step(v_job_id,'Obtaining advisory lock for job: '||v_job_name);
    PERFORM gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    PERFORM update_step(v_step_id, 'OK','Found concurrent job. Exiting gracefully');
    PERFORM close_job(v_job_id);
    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
    RETURN;
END IF;

v_step_id := add_step(v_job_id,'Grabbing Boundries, Building SQL');

IF v_pk_field IS NULL OR v_pk_type IS NULL THEN
    RAISE EXCEPTION 'ERROR: primary key fields in refresh_config_logdel must be defined';
END IF;

-- determine column list, column type list
IF v_filter IS NULL THEN 
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||atttypid::regtype::text),',') FROM 
        pg_attribute WHERE attnum > 0 AND attisdropped is false AND attrelid = p_destination::regclass AND attname != 'source_deleted' INTO v_cols, v_cols_n_types;
ELSE
    -- ensure all primary key columns are included in any column filters
    FOREACH v_field IN ARRAY v_pk_field LOOP
        IF v_field = ANY(v_filter) THEN
            CONTINUE;
        ELSE
            RAISE EXCEPTION 'ERROR: filter list did not contain all columns that compose primary key for %',v_job_name; 
        END IF;
    END LOOP;
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||atttypid::regtype::text),',') FROM 
        (SELECT unnest(filter) FROM refresh_config_logdel WHERE dest_table = p_destination) x 
         JOIN pg_attribute ON (unnest=attname::text AND attrelid=p_destination::regclass) WHERE attname != 'source_deleted' INTO v_cols, v_cols_n_types;
END IF;    

-- init sql statements 

v_pk_field_csv := array_to_string(v_pk_field,',');
v_with_update := 'WITH a AS (SELECT '||v_pk_field_csv||' FROM '|| v_control ||' ORDER BY 1 LIMIT '|| p_limit ||') UPDATE '||v_control||' b SET processed = true FROM a WHERE a.'||v_pk_field[1]||' = b.'||v_pk_field[1];

IF array_length(v_pk_field, 1) > 1 THEN
    v_pk_where := '';
    WHILE v_pk_counter <= array_length(v_pk_field,1) LOOP
        v_pk_where := v_pk_where || ' AND a.'||v_pk_field[v_pk_counter]||' = b.'||v_pk_field[v_pk_counter];
        v_pk_counter := v_pk_counter + 1;
    END LOOP;
END IF;

IF v_pk_where IS NOT NULL THEN
    v_with_update := v_with_update || v_pk_where;
END IF;
PERFORM gdb(p_debug, v_with_update);

v_trigger_update := 'SELECT dblink_exec(auth('||v_dblink||'),'|| quote_literal(v_with_update)||')';

v_remote_q_sql := 'SELECT DISTINCT '||v_pk_field_csv||' FROM '||v_control||' WHERE processed = true and source_deleted IS NULL';

v_remote_f_sql := 'SELECT '||v_cols||' FROM '||v_source_table||' JOIN ('||v_remote_q_sql||') x USING ('||v_pk_field_csv||')';
v_create_f_sql := 'CREATE TEMP TABLE '||v_tmp_table||'_full AS SELECT '||v_cols||' 
    FROM dblink(auth('||v_dblink||'),'||quote_literal(v_remote_f_sql)||') t ('||v_cols_n_types||')';

v_remote_d_sql = 'SELECT '||v_cols||', source_deleted FROM '||v_control||' WHERE processed = true and source_deleted IS NOT NULL';
v_create_d_sql = 'CREATE TEMP TABLE '||v_tmp_table||'_deleted AS SELECT '||v_cols||', source_deleted
    FROM dblink(auth('||v_dblink||'),'||quote_literal(v_remote_d_sql)||') t ('||v_cols_n_types||', source_deleted timestamptz)';

v_delete_f_sql := 'DELETE FROM '||v_dest_table||' a USING '||v_tmp_table||'_full b WHERE a.'||v_pk_field[1]||'= b.'||v_pk_field[1];
IF array_length(v_pk_field, 1) > 1 THEN
    v_delete_f_sql := v_delete_f_sql || v_pk_where;
END IF; 

-- remove rows that were deleted on source to ensure most recently deleted data is logged 
v_delete_d_sql := 'DELETE FROM '||v_dest_table||' a USING '||v_tmp_table||'_deleted b WHERE a.'||v_pk_field[1]||'= b.'||v_pk_field[1];
IF array_length(v_pk_field, 1) > 1 THEN
    v_delete_d_sql := v_delete_d_sql || v_pk_where;
END IF; 

v_insert_sql := 'INSERT INTO '||v_dest_table||'('||v_cols||') SELECT '||v_cols||' FROM '||v_tmp_table||'_full';
v_insert_deleted_sql := 'INSERT INTO '||v_dest_table||'('||v_cols||', source_deleted) SELECT '||v_cols||', source_deleted FROM '||v_tmp_table||'_deleted'; 

v_trigger_delete := 'SELECT dblink_exec(auth('||v_dblink||'),'||quote_literal('DELETE FROM '||v_control||' WHERE processed = true')||')'; 

PERFORM update_step(v_step_id, 'OK','Remote table is '||v_source_table);

-- update remote entries
v_step_id := add_step(v_job_id,'Updating remote trigger table');
    PERFORM gdb(p_debug,v_trigger_update);
    EXECUTE v_trigger_update INTO v_exec_status;    
PERFORM update_step(v_step_id, 'OK','Result was '||v_exec_status);

-- create temp table for insertion (inserts/updates)
v_step_id := add_step(v_job_id,'Create temp table from remote full table');
    PERFORM gdb(p_debug,v_create_f_sql);
    EXECUTE v_create_f_sql;  
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM gdb(p_debug,'Insert/Update Temp table row count '||v_rowcount::text);
    IF v_rowcount < 1 THEN 
        PERFORM update_step(v_step_id, 'OK','No new rows found');
        EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_full';
        EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_deleted';
        PERFORM close_job(v_job_id);
        -- Ensure old search path is reset for the current session
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        PERFORM pg_advisory_unlock(hashtext('refresh_logdel'), hashtext(v_job_name));
        RETURN;
    END IF;
PERFORM update_step(v_step_id, 'OK','Table contains '||v_rowcount||' records');

-- create temp table for insertion (deleted rows)
v_step_id := add_step(v_job_id,'Create temp table from remote delete table');
    PERFORM gdb(p_debug,v_create_d_sql);
    EXECUTE v_create_d_sql;  
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM gdb(p_debug,'Delete Temp table row count '||v_rowcount::text);
PERFORM update_step(v_step_id, 'OK','Table contains '||v_rowcount||' records');

-- remove records from local table (inserts/updates)
v_step_id := add_step(v_job_id,'Deleting insert/update records from local table');
    PERFORM gdb(p_debug,v_delete_f_sql);
    EXECUTE v_delete_f_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM gdb(p_debug,'Insert/Update rows removed from local table before applying changes: '||v_rowcount::text);
PERFORM update_step(v_step_id, 'OK','Removed '||v_rowcount||' records');

-- remove records from local table (deleted rows)
v_step_id := add_step(v_job_id,'Deleting removed records from local table');
    PERFORM gdb(p_debug,v_delete_d_sql);
    EXECUTE v_delete_d_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM gdb(p_debug,'Deleted Rows removed from local table before applying changes: '||v_rowcount::text);
PERFORM update_step(v_step_id, 'OK','Removed '||v_rowcount||' records');

-- insert records to local table (inserts/updates)
v_step_id := add_step(v_job_id,'Inserting new/updated records into local table');
    PERFORM gdb(p_debug,v_insert_sql);
    EXECUTE v_insert_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM gdb(p_debug,'Rows inserted: '||v_rowcount::text);
PERFORM update_step(v_step_id, 'OK','Inserted '||v_rowcount||' records');

-- insert records to local table (deleted rows to be kepts)
v_step_id := add_step(v_job_id,'Inserting deleted records into local table');
    PERFORM gdb(p_debug,v_insert_deleted_sql);
    EXECUTE v_insert_deleted_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM gdb(p_debug,'Rows inserted: '||v_rowcount::text);
PERFORM update_step(v_step_id, 'OK','Inserted '||v_rowcount||' records');

-- clean out rows from txn table
v_step_id := add_step(v_job_id,'Cleaning out rows from txn table');
    PERFORM gdb(p_debug,v_trigger_delete);
    EXECUTE v_trigger_delete INTO v_exec_status;
PERFORM update_step(v_step_id, 'OK','Result was '||v_exec_status);

-- update activity status
v_step_id := add_step(v_job_id,'Updating last_value in config table');
    v_last_value_sql := 'UPDATE refresh_config_logdel SET last_value = '|| quote_literal(current_timestamp::timestamp) ||' WHERE dest_table = ' ||quote_literal(p_destination); 
    PERFORM gdb(p_debug,v_last_value_sql);
    EXECUTE v_last_value_sql; 
PERFORM update_step(v_step_id, 'OK','Last Value was '||current_timestamp);

PERFORM close_job(v_job_id);

EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_full';
EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_deleted';

-- Ensure old search path is reset for the current session
EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

PERFORM pg_advisory_unlock(hashtext('refresh_logdel'), hashtext(v_job_name));

EXCEPTION
    WHEN others THEN
        -- Exception block resets path, so have to reset it again
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';
        IF v_step_id IS NULL THEN
            v_step_id := jobmon.add_step(v_job_id, 'EXCEPTION before first step logged');
        END IF;
        PERFORM update_step(v_step_id, 'BAD', 'ERROR: '||coalesce(SQLERRM,'unknown'));
        PERFORM fail_job(v_job_id);

        -- Ensure old search path is reset for the current session
       EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

        PERFORM pg_advisory_unlock(hashtext('refresh_logdel'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;
END
$$;


/*
 *  Snapshot maker function. Assumes source and destination are the same tablename.
 */
CREATE FUNCTION snapshot_maker(p_src_table text, p_dblink_id int) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_data_source               text;
v_insert_refresh_config     text;

BEGIN

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'Database link ID does not exist in @extschema@.dblink_mapping: %', p_dblink_id; 
END IF;  

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_src_table)||', '|| p_dblink_id||');';

RAISE NOTICE 'Inserting record in @extschema@.refresh_config';
EXECUTE v_insert_refresh_config;	
RAISE NOTICE 'Insert successful';	

RAISE NOTICE 'attempting first snapshot';
PERFORM @extschema@.refresh_snap(p_src_table, FALSE);

RAISE NOTICE 'attempting second snapshot';
PERFORM @extschema@.refresh_snap(p_src_table, FALSE);

RAISE NOTICE 'all done';

RETURN;

END
$$;


/*
 *  Snapshot maker function. Accepts custom destination name.
 */
CREATE FUNCTION snapshot_maker(p_src_table text, p_dest_table text, p_dblink_id int) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_data_source               text;
v_insert_refresh_config     text;

BEGIN

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'ERROR: Database link ID does not exist in @extschema@.dblink_mapping: %', p_dblink_id; 
END IF;  

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||');';

RAISE NOTICE 'Inserting record in @extschema@.refresh_config';
EXECUTE v_insert_refresh_config;	
RAISE NOTICE 'Insert successful';	

RAISE NOTICE 'attempting first snapshot';
PERFORM @extschema@.refresh_snap(p_dest_table, FALSE);

RAISE NOTICE 'attempting second snapshot';
PERFORM @extschema@.refresh_snap(p_dest_table, FALSE);

RAISE NOTICE 'all done';

RETURN;

END
$$;


/*
 *  Snapshot destroyer function. Pass archive to keep permanent copy of snap view.
 */
CREATE FUNCTION snapshot_destroyer(p_dest_table text, p_archive_option text) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_dest_table        text;
v_exists            int;
v_snap_suffix       text;
v_src_table         text;
v_view_definition   text;

BEGIN

SELECT source_table, dest_table INTO v_src_table, v_dest_table
    FROM @extschema@.refresh_config_snap WHERE dest_table = p_dest_table;
IF NOT FOUND THEN
    RAISE EXCEPTION 'This table is not set up for snapshot replication: %', v_dest_table;
END IF;

-- Make a brand new, real table to keep the data that is not part of the snap system anymore
IF p_archive_option = 'ARCHIVE' THEN

    SELECT definition INTO v_view_definition FROM pg_views WHERE schemaname || '.' || viewname = v_dest_table;
    v_exists := strpos(v_view_definition, 'snap1');
    IF v_exists > 0 THEN
        v_snap_suffix := 'snap1';
    ELSE
        v_snap_suffix := 'snap2';
    END IF;
    
    EXECUTE 'DROP VIEW ' || v_dest_table;
    EXECUTE 'CREATE TEMPORARY TABLE tmp_snapshot_destroy AS SELECT * FROM ' || v_dest_table || '_' || v_snap_suffix;
    EXECUTE 'CREATE TABLE ' || v_dest_table || ' AS SELECT * FROM tmp_snapshot_destroy';
    
ELSE

    EXECUTE 'DROP VIEW ' || v_dest_table;    

END IF;

EXECUTE 'DROP TABLE ' || v_dest_table || '_snap1';
EXECUTE 'DROP TABLE ' || v_dest_table || '_snap2';

EXECUTE 'DELETE FROM @extschema@.refresh_config_snap WHERE dest_table = ' || quote_literal(v_dest_table);

EXECUTE 'DROP TABLE IF EXISTS tmp_snapshot_destroy';

END
$$;


/*
 *  Inserter maker function. Assumes source and destination are the same tablename.
 */
CREATE FUNCTION inserter_maker(p_src_table text, p_control_field text, p_dblink_id int, p_boundary interval DEFAULT '00:10:00') RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_data_source               text;
v_dst_active                boolean;
v_exists                    int;
v_insert_refresh_config     text;
v_max_timestamp             timestamptz;
v_snap_suffix               text;
v_view_definition           text;

BEGIN

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'ERROR: database link ID is incorrect %', p_dblink_id; 
END IF;  

-- Temp snap config
v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_src_table)||', '|| p_dblink_id||')';

RAISE NOTICE 'Inserting record in @extschema@.refresh_config';
EXECUTE v_insert_refresh_config;	
RAISE NOTICE 'Insert successful';	

RAISE NOTICE 'attempting snapshot';
PERFORM @extschema@.refresh_snap(p_src_table, FALSE);

RAISE NOTICE 'attempting to destroy snapshot';

SELECT definition INTO v_view_definition FROM pg_views WHERE schemaname || '.' || viewname = p_src_table;
	v_exists := strpos(v_view_definition, 'snap1');
	IF v_exists > 0 THEN
    	v_snap_suffix := 'snap1';
	END IF;

	EXECUTE 'DROP VIEW ' || p_src_table;
	EXECUTE 'CREATE TABLE ' || p_src_table || ' AS SELECT * FROM ' || p_src_table || '_' || v_snap_suffix;
EXECUTE 'DROP TABLE ' || p_src_table || '_snap1';

RAISE NOTICE 'Destroyed successfully';

RAISE NOTICE 'Taking the maximum timestamp';
EXECUTE 'SELECT max('||p_control_field||') FROM '||p_src_table||';' INTO v_max_timestamp;
RAISE NOTICE 'The select statement ran successfully.';

v_dst_active := @extschema@.dst_utc_check();

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_inserter(source_table, dest_table, dblink, control, boundary, last_value, dst_active) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_src_table)||', '|| p_dblink_id||', '
    ||quote_literal(p_control_field)||', '''||p_boundary||'''::interval,'''||v_max_timestamp||'''::timestamptz, '||v_dst_active||');';

RAISE NOTICE 'Updating config table with highest timestamp value';
EXECUTE v_insert_refresh_config;
RAISE NOTICE 'Update successful';

-- Remove temp snap from config
EXECUTE 'DELETE FROM @extschema@.refresh_config_snap WHERE source_table = '||quote_literal(p_src_table)||' AND dest_table = '||quote_literal(p_src_table);

RAISE NOTICE 'all done';

RETURN;

END
$$;


/*
 *  Inserter maker function. Accepts custom destination name.
 */
CREATE FUNCTION inserter_maker(p_src_table text, p_dest_table text, p_control_field text, p_dblink_id int, p_boundary interval DEFAULT '00:10:00') RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_data_source               text;
v_dst_active                boolean;
v_exists                    int;
v_insert_refresh_config     text;
v_max_timestamp             timestamptz;
v_snap_suffix               text;
v_view_definition           text;

BEGIN

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'ERROR: database link ID is incorrect %', p_dblink_id; 
END IF;  

-- Temp snap config
v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||')';

RAISE NOTICE 'Inserting record in @extschema@.refresh_config';
EXECUTE v_insert_refresh_config;	
RAISE NOTICE 'Insert successful';	

RAISE NOTICE 'attempting snapshot';
PERFORM @extschema@.refresh_snap(p_dest_table, FALSE);

RAISE NOTICE 'attempting to destroy snapshot';

SELECT definition INTO v_view_definition FROM pg_views WHERE schemaname || '.' || viewname = p_dest_table;
	v_exists := strpos(v_view_definition, 'snap1');
	IF v_exists > 0 THEN
    	v_snap_suffix := 'snap1';
	END IF;

	EXECUTE 'DROP VIEW ' || p_dest_table;
	EXECUTE 'CREATE TABLE ' || p_dest_table || ' AS SELECT * FROM ' || p_dest_table || '_' || v_snap_suffix;
EXECUTE 'DROP TABLE ' || p_dest_table || '_snap1';

RAISE NOTICE 'Destroyed successfully';

RAISE NOTICE 'Taking the maximum timestamp';
EXECUTE 'SELECT max('||p_control_field||') FROM '||p_dest_table||';' INTO v_max_timestamp;
RAISE NOTICE 'The select statement ran successfully.';

v_dst_active := @extschema@.dst_utc_check();

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_inserter(source_table, dest_table, dblink, control, boundary, last_value, dst_active) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||', '
    ||quote_literal(p_control_field)||', '''||p_boundary||'''::interval, '''||v_max_timestamp||'''::timestamptz, '||v_dst_active||');';

RAISE NOTICE 'Updating config table with highest timestamp value';
EXECUTE v_insert_refresh_config;
RAISE NOTICE 'Update successful';

-- Remove temp snap from config
EXECUTE 'DELETE FROM @extschema@.refresh_config_snap WHERE source_table = '||quote_literal(p_src_table)||' AND dest_table = '||quote_literal(p_dest_table);

RAISE NOTICE 'all done';

RETURN;

END
$$;


/*
 *  Inserter destroyer function. Pass archive to keep table intact.
 */
CREATE FUNCTION inserter_destroyer(p_dest_table text, p_archive_option text) RETURNS void
    LANGUAGE plpgsql
    AS $$
    
DECLARE

v_dest_table        text;
v_src_table         text;
    
BEGIN

SELECT source_table, dest_table INTO v_src_table, v_dest_table
		FROM @extschema@.refresh_config_inserter WHERE dest_table = p_dest_table;
IF NOT FOUND THEN
		RAISE EXCEPTION 'This table is not set up for inserter replication: %', v_dest_table;
END IF;

-- Keep destination table
IF p_archive_option != 'ARCHIVE' THEN 
    EXECUTE 'DROP TABLE ' || v_dest_table;
END IF;

EXECUTE 'DELETE FROM @extschema@.refresh_config_inserter WHERE dest_table = ' || quote_literal(v_dest_table);	

END
$$;


/*
 *  Updater maker function. Assumes source and destination are the same tablename.
 */
CREATE FUNCTION updater_maker(p_src_table text, p_control_field text, p_dblink_id int, p_pk_field text[], p_pk_type text[], p_boundary interval DEFAULT '00:10:00') RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_alter_table               text;
v_data_source               text;
v_dst_active                boolean;
v_exists                    int;
v_insert_refresh_config     text;
v_max_timestamp             timestamptz;
v_pk_field_csv              text;
v_pk_type_csv               text;
v_primary_key               text;
v_snap_suffix               text;
v_update_refresh_config     text;
v_view_definition           text;

BEGIN

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'ERROR: database link ID is incorrect %', p_dblink_id; 
END IF;  

v_pk_field_csv := ''''||array_to_string(p_pk_field,''',''')||'''';
v_pk_type_csv := ''''||array_to_string(p_pk_type,''',''')||'''';
v_primary_key := array_to_string(p_pk_field,',');

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_src_table)||', '|| p_dblink_id||');';

RAISE NOTICE 'Inserting record in @extschema@.refresh_config';
EXECUTE v_insert_refresh_config;	
RAISE NOTICE 'Insert successful';	

RAISE NOTICE 'attempting snapshot';
PERFORM @extschema@.refresh_snap(p_src_table, FALSE);

RAISE NOTICE 'attempting to destroy snapshot';

SELECT definition INTO v_view_definition FROM pg_views WHERE schemaname || '.' || viewname = p_src_table;
	v_exists := strpos(v_view_definition, 'snap1');
	IF v_exists > 0 THEN
    	v_snap_suffix := 'snap1';
	END IF;

	EXECUTE 'DROP VIEW ' || p_src_table;
	EXECUTE 'CREATE TABLE ' || p_src_table || ' AS SELECT * FROM ' || p_src_table || '_' || v_snap_suffix;
EXECUTE 'DROP TABLE ' || p_src_table || '_snap1';

RAISE NOTICE 'Destroyed successfully';

v_alter_table := 'ALTER TABLE '||p_src_table||' ADD PRIMARY KEY('||v_primary_key||');';

RAISE NOTICE 'Adding primary key constraint to table';
EXECUTE v_alter_table;
RAISE NOTICE 'Constraint added successfully';

RAISE NOTICE 'Taking the maximum timestamp';
EXECUTE 'SELECT max('||p_control_field||') FROM '||p_src_table||';' INTO v_max_timestamp;
RAISE NOTICE 'The select statement ran successfully.';

v_dst_active := @extschema@.dst_utc_check();

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_updater(source_table, dest_table, dblink, control, boundary, pk_field, pk_type, last_value, dst_active) VALUES('||quote_literal(p_src_table)||', '||quote_literal(p_src_table)||', '|| p_dblink_id||', '||quote_literal(p_control_field)||', '''||p_boundary||'''::interval, ARRAY['||v_pk_field_csv||'], ARRAY['||v_pk_type_csv||'], '''||v_max_timestamp||'''::timestamptz, '||v_dst_active||')';

RAISE NOTICE 'Updating config table with highest timestamp value';
EXECUTE v_insert_refresh_config;
RAISE NOTICE 'Update successful';

-- Remove temp snap from config
EXECUTE 'DELETE FROM @extschema@.refresh_config_snap WHERE source_table = '||quote_literal(p_src_table)||' AND dest_table = '||quote_literal(p_src_table);

RAISE NOTICE 'All Done';

RETURN;

END
$$;


/*
 *  Updater maker function. Accepts custom destination name.
 */
CREATE FUNCTION updater_maker(p_src_table text, p_dest_table text, p_control_field text, p_dblink_id int, p_pk_field text[], p_pk_type text[], p_boundary interval DEFAULT '00:10:00') RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_alter_table               text;
v_data_source               text;
v_dst_active                boolean;
v_exists                    int;
v_insert_refresh_config     text;
v_max_timestamp             timestamptz;
v_pk_field_csv              text;
v_pk_type_csv               text;
v_primary_key               text;
v_snap_suffix               text;
v_update_refresh_config     text;
v_view_definition           text;

BEGIN

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'ERROR: database link ID is incorrect %', p_dblink_id; 
END IF;  

v_pk_field_csv := ''''||array_to_string(p_pk_field,''',''')||'''';
v_pk_type_csv := ''''||array_to_string(p_pk_type,''',''')||'''';
v_primary_key := array_to_string(p_pk_field,',');

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||')';

RAISE NOTICE 'Inserting record in @extschema@.refresh_config';
EXECUTE v_insert_refresh_config;	
RAISE NOTICE 'Insert successful';	

RAISE NOTICE 'attempting snapshot';
PERFORM @extschema@.refresh_snap(p_dest_table, FALSE);

RAISE NOTICE 'attempting to destroy snapshot';

SELECT definition INTO v_view_definition FROM pg_views WHERE schemaname || '.' || viewname = p_dest_table;
	v_exists := strpos(v_view_definition, 'snap1');
	IF v_exists > 0 THEN
    	v_snap_suffix := 'snap1';
	END IF;

	EXECUTE 'DROP VIEW ' || p_dest_table;
	EXECUTE 'CREATE TABLE ' || p_dest_table || ' AS SELECT * FROM ' || p_dest_table || '_' || v_snap_suffix;
EXECUTE 'DROP TABLE ' || p_dest_table || '_snap1';

RAISE NOTICE 'Destroyed successfully';

v_alter_table := 'ALTER TABLE '||p_dest_table||' ADD PRIMARY KEY('||v_primary_key||');';

RAISE NOTICE 'Adding primary key constraint to table';
EXECUTE v_alter_table;
RAISE NOTICE 'Constraint added successfully';

RAISE NOTICE 'Taking the maximum timestamp';
EXECUTE 'SELECT max('||p_control_field||') FROM '||p_dest_table||';' INTO v_max_timestamp;
RAISE NOTICE 'The select statement ran successfully.';

v_dst_active := @extschema@.dst_utc_check();

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_updater(source_table, dest_table, dblink, control, boundary, pk_field, pk_type, last_value, dst_active) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||', '||quote_literal(p_control_field)||', '''
    ||p_boundary||'''::interval, ARRAY['||v_pk_field_csv||'], ARRAY['||v_pk_type_csv||'], '''||v_max_timestamp||'''::timestamptz, '||v_dst_active||')';

RAISE NOTICE 'Updating config table with highest timestamp value';
EXECUTE v_insert_refresh_config;
RAISE NOTICE 'Update successful';

-- Remove temp snap from config
EXECUTE 'DELETE FROM @extschema@.refresh_config_snap WHERE source_table = '||quote_literal(p_src_table)||' AND dest_table = '||quote_literal(p_dest_table);

RAISE NOTICE 'All Done';

RETURN;

END
$$;


/*
 *  Updater destroyer function. Pass archive to keep table intact.
 */
CREATE FUNCTION updater_destroyer(p_dest_table text, p_archive_option text) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_dest_table        text;
v_src_table         text;
    
BEGIN

SELECT source_table, dest_table INTO v_src_table, v_dest_table
    FROM @extschema@.refresh_config_updater WHERE dest_table = p_dest_table;
IF NOT FOUND THEN
    RAISE EXCEPTION 'This table is not set up for updater replication: %', v_dest_table;
END IF;

-- Keep destination table
IF p_archive_option != 'ARCHIVE' THEN 
    EXECUTE 'DROP TABLE ' || v_dest_table;
END IF;

EXECUTE 'DELETE FROM @extschema@.refresh_config_updater WHERE dest_table = ' || quote_literal(v_dest_table);

END
$$;

/*
 *  Custom query refresh to pull data
 */
CREATE OR REPLACE FUNCTION refresh_custom(p_destination text, p_debug boolean) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock          boolean; 
v_cols_n_types      text;
v_cols              text;
v_create_sql        text;
v_dblink_schema     text;
v_dblink            integer;
v_dest_table        text;
v_exists            int;
v_insert_sql        text;
v_job_id            int;
v_jobmon_schema     text;
v_job_name          text;
v_lcols_array       text[];
v_local_sql         text;
v_l                 text;
v_match             boolean := 'f';
v_old_search_path   text;
v_parts             record;
v_post_script       text[];
v_rcols_array       text[];
v_refresh_snap      text;
v_remote_sql        text;
v_rowcount          bigint;
v_r                 text;
v_snap              text;
v_step_id           int;
v_table_exists      int;
v_view_definition   text;
v_custom_query      text;
v_tmp_table         text;
v_data_source       text;
v_query             text;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'notice', true );
END IF;

v_job_name := 'Refresh custom: '||p_destination;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

-- Set custom search path to allow easier calls to other functions, especially job logging
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';

v_job_id := add_job(v_job_name);
PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);

-- Take advisory lock to prevent multiple calls to function overlapping and causing possible deadlock
v_adv_lock := pg_try_advisory_lock(hashtext('refresh_custom'), hashtext(v_job_name));
IF v_adv_lock = 'false' THEN
    v_step_id := add_step(v_job_id,'Obtaining advisory lock for job: '||v_job_name);
    PERFORM gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    PERFORM update_step(v_step_id, 'OK','Found concurrent job. Exiting gracefully');
    PERFORM close_job(v_job_id);
    RETURN;
END IF;

v_step_id := add_step(v_job_id,'Grabbing Mapping, Building SQL');

SELECT dest_table, 'tmp_'||replace(dest_table,'.','_'), dblink, query INTO v_dest_table, v_tmp_table, v_dblink, v_custom_query FROM refresh_config_custom
WHERE dest_table = p_destination; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: This table is not set up for custom query replication: %',v_job_name; 
END IF;  

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = v_dblink;

-- Checking if destination table already exists.
SELECT string_to_array(v_dest_table, '.') AS oparts INTO v_parts;
SELECT INTO v_table_exists count(1) FROM pg_tables
    WHERE  schemaname = v_parts.oparts[1] AND
           tablename = v_parts.oparts[2];

-- If table doesn't already exist, create a new one
IF v_table_exists = 0 THEN

	v_query := 'create table '||v_dest_table||'_custom_table as '||v_custom_query;
	v_remote_sql := 'select dblink_exec('||quote_literal(v_data_source)||', '||quote_literal(v_query)||');';
	EXECUTE v_remote_sql;
	RAISE NOTICE 'Table created at remote server based on the custom query';

	v_remote_sql := 'SELECT array_to_string(array_agg(attname),'','') as cols, array_to_string(array_agg(attname||'' ''||atttypid::regtype::text),'','') as cols_n_types FROM pg_attribute WHERE attnum > 0 AND attisdropped is false AND attrelid = '''||v_dest_table||'_custom_table''::regclass';
	v_remote_sql := 'SELECT cols, cols_n_types FROM dblink(auth(' || v_dblink || '), ' || quote_literal(v_remote_sql) || ') t (cols text, cols_n_types text)';
	perform gdb(p_debug,'v_remote_sql: '||v_remote_sql);
	EXECUTE v_remote_sql INTO v_cols, v_cols_n_types;  
	perform gdb(p_debug,'v_cols: '||v_cols);
	perform gdb(p_debug,'v_cols_n_types: '||v_cols_n_types);
	v_create_sql := 'CREATE TABLE ' || v_dest_table || ' (' || v_cols_n_types || ');';
    	perform gdb(p_debug,'v_create_sql: '||v_create_sql::text);
    	EXECUTE v_create_sql;

	PERFORM update_step(v_step_id, 'OK','Done');
	v_step_id := add_step(v_job_id,'Destination table created.');

END IF;

-- init sql statements 
    v_remote_sql := 'SELECT array_to_string(array_agg(attname),'','') as cols, array_to_string(array_agg(attname||'' ''||atttypid::regtype::text),'','') as cols_n_types FROM pg_attribute WHERE attnum > 0 AND attisdropped is false AND attrelid = ' || quote_literal(v_dest_table) || '::regclass';
    perform gdb(p_debug,'v_remote_sql: '||v_remote_sql);
    EXECUTE v_remote_sql INTO v_cols, v_cols_n_types;  
    perform gdb(p_debug,'v_cols: '||v_cols);
    perform gdb(p_debug,'v_cols_n_types: '||v_cols_n_types);
    v_create_sql := 'CREATE TEMP TABLE '||v_tmp_table||' AS SELECT '||v_cols||' FROM dblink(auth('||v_dblink||'),'||quote_literal(v_custom_query)||') t ('||v_cols_n_types||')';
    v_insert_sql := 'INSERT INTO '||v_dest_table||'('||v_cols||') SELECT '||v_cols||' FROM '||v_tmp_table; 

PERFORM update_step(v_step_id, 'OK','Done');

-- create temp from remote
v_step_id := add_step(v_job_id,'Creating temp table ('||v_tmp_table||') from remote query');
    PERFORM gdb(p_debug,v_create_sql);
    EXECUTE v_create_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM update_step(v_step_id, 'OK','Table contains '||v_rowcount||' records');
    PERFORM gdb(p_debug, v_rowcount || ' rows added to temp table');

-- insert
v_step_id := add_step(v_job_id,'Inserting new records into local table');
    PERFORM gdb(p_debug,v_insert_sql);
    EXECUTE v_insert_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM update_step(v_step_id, 'OK','Inserted '||v_rowcount||' records');
    PERFORM gdb(p_debug, v_rowcount || ' rows added to ' || v_dest_table);

EXECUTE 'DROP TABLE IF EXISTS ' || v_tmp_table;

-- To delete the custom table created at the remote server
v_query := 'drop table '||v_dest_table||'_custom_table';

-- Deleting custom table at remote server
v_remote_sql := 'select dblink_exec('||quote_literal(v_data_source)||', '||quote_literal(v_query)||');';
EXECUTE v_remote_sql;

PERFORM close_job(v_job_id);

-- Ensure old search path is reset for the current session
EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

PERFORM pg_advisory_unlock(hashtext('refresh_custom'), hashtext(v_job_name));

EXCEPTION
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';
        PERFORM update_step(v_step_id, 'BAD', 'ERROR: '||coalesce(SQLERRM,'unknown'));
        PERFORM fail_job(v_job_id);

        -- Ensure old search path is reset for the current session
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

        PERFORM pg_advisory_unlock(hashtext('refresh_custom'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;    
END
$$;

/*
 *  Custom maker function. Accepts custom destination name.
 */
CREATE OR REPLACE FUNCTION custom_maker (p_dest_table text, p_dblink_id int, p_query text) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_data_source               text;
v_insert_refresh_config     text;

BEGIN

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'ERROR: database link ID is incorrect %', p_dblink_id; 
END IF;  

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_custom(dest_table, dblink, query) VALUES('
    ||quote_literal(p_dest_table)||', '|| p_dblink_id||', '||quote_literal(p_query)||')';

-- Insert record in refresh_config_custom
RAISE NOTICE 'Inserting record in @extschema@.refresh_config_custom';
EXECUTE v_insert_refresh_config;	
RAISE NOTICE 'Insert successful';

RAISE NOTICE 'Calling custom refresh function';
PERFORM @extschema@.refresh_custom(p_dest_table, FALSE);
RAISE NOTICE 'all done';

RETURN;

END
$$;

/*
 *  Custom refresh destroyer function. Pass archive to keep table intact.
 */
CREATE OR REPLACE FUNCTION custom_destroyer(p_dest_table text, p_archive_option text) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_dest_table        text;
    
BEGIN

SELECT dest_table INTO v_dest_table
    FROM @extschema@.refresh_config_custom WHERE dest_table = p_dest_table;
IF NOT FOUND THEN
    RAISE EXCEPTION 'This table is not set up for updater replication: %', v_dest_table;
END IF;

-- Keep destination table
IF p_archive_option != 'ARCHIVE' THEN 
    EXECUTE 'DROP TABLE ' || v_dest_table;
END IF;

EXECUTE 'DELETE FROM @extschema@.refresh_config_custom WHERE dest_table = ' || quote_literal(v_dest_table);

END
$$;
