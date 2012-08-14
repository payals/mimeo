-- Altered the refresh_type enum. Added new value 'custom' to it.
-- Created child table refresh_config_custom inheriting from refresh_config, for the custom_refresh related functions.
-- Created refresh_custom, custom_maker and custom_destroyer functions to facilitate replication based on custom queries like joins.

ALTER type @extschema@.refresh_type RENAME TO old_refresh_type;
CREATE TYPE @extschema@.refresh_type AS ENUM ('snap', 'inserter', 'updater', 'dml', 'logdel', 'custom');
ALTER TABLE @extschema@.refresh_config RENAME column type TO old_type;
ALTER TABLE @extschema@.refresh_config ADD type @extschema@.refresh_type;

UPDATE @extschema@.refresh_config_snap SET type = old_type::text::@extschema@.refresh_type;
UPDATE @extschema@.refresh_config_inserter SET type = old_type::text::@extschema@.refresh_type;
UPDATE @extschema@.refresh_config_updater SET type = old_type::text::@extschema@.refresh_type;
UPDATE @extschema@.refresh_config_dml SET type = old_type::text::@extschema@.refresh_type;
UPDATE @extschema@.refresh_config_logdel SET type = old_type::text::@extschema@.refresh_type;

-- Creating refresh_config_custom table
CREATE TABLE refresh_config_custom (LIKE @extschema@.refresh_config INCLUDING ALL) INHERITS (@extschema@.refresh_config);
SELECT pg_catalog.pg_extension_config_dump('refresh_config_custom', '');
ALTER TABLE @extschema@.refresh_config_custom ADD CONSTRAINT refresh_config_custom_dblink_fkey FOREIGN KEY (dblink) REFERENCES @extschema@.dblink_mapping(data_source_id);
ALTER TABLE @extschema@.refresh_config_custom ADD CONSTRAINT refresh_config_custom_dest_table_pkey PRIMARY KEY (dest_table);
ALTER TABLE @extschema@.refresh_config_custom ALTER COLUMN type SET DEFAULT 'custom';
ALTER TABLE @extschema@.refresh_config_custom ADD CONSTRAINT refresh_config_custom_type_check CHECK (type = 'custom');
ALTER TABLE @extschema@.refresh_config_custom ADD COLUMN query text;

ALTER TABLE @extschema@.refresh_config DROP COLUMN old_type CASCADE;
ALTER TABLE @extschema@.refresh_config_snap DROP COLUMN old_type;
ALTER TABLE @extschema@.refresh_config_snap ALTER COLUMN type SET DEFAULT 'snap';
ALTER TABLE @extschema@.refresh_config_inserter DROP COLUMN old_type;
ALTER TABLE @extschema@.refresh_config_inserter ALTER COLUMN type SET DEFAULT 'inserter';
ALTER TABLE @extschema@.refresh_config_updater DROP COLUMN old_type;
ALTER TABLE @extschema@.refresh_config_updater ALTER COLUMN type SET DEFAULT 'updater';
ALTER TABLE @extschema@.refresh_config_dml DROP COLUMN old_type;
ALTER TABLE @extschema@.refresh_config_dml ALTER COLUMN type SET DEFAULT 'dml';
ALTER TABLE @extschema@.refresh_config_logdel DROP COLUMN old_type;
ALTER TABLE @extschema@.refresh_config_logdel ALTER COLUMN type SET DEFAULT 'logdel';
ALTER TABLE @extschema@.refresh_config_custom DROP COLUMN old_type;

ALTER TABLE @extschema@.refresh_config ALTER COLUMN type SET NOT NULL;

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

