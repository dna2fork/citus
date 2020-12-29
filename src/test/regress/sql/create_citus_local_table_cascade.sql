\set VERBOSITY terse

SET citus.next_shard_id TO 1516000;
SET citus.shard_replication_factor TO 1;

CREATE SCHEMA create_citus_local_table_cascade;
SET search_path TO create_citus_local_table_cascade;

SET client_min_messages to ERROR;

-- ensure that coordinator is added to pg_dist_node
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);

CREATE TABLE local_table_1 (col_1 INT UNIQUE);
CREATE TABLE local_table_2 (col_1 INT UNIQUE);
CREATE TABLE local_table_3 (col_1 INT UNIQUE);
CREATE TABLE local_table_4 (col_1 INT UNIQUE);

-- local_table_2 -> local_table_1 -> local_table_4
--                      ^ |               |
--                      | v               |
--                  local_table_3 <--------
ALTER TABLE local_table_2 ADD CONSTRAINT fkey_1 FOREIGN KEY (col_1) REFERENCES local_table_1(col_1);
ALTER TABLE local_table_3 ADD CONSTRAINT fkey_2 FOREIGN KEY (col_1) REFERENCES local_table_1(col_1);
ALTER TABLE local_table_1 ADD CONSTRAINT fkey_3 FOREIGN KEY (col_1) REFERENCES local_table_3(col_1);
ALTER TABLE local_table_1 ADD CONSTRAINT fkey_4 FOREIGN KEY (col_1) REFERENCES local_table_4(col_1);
ALTER TABLE local_table_4 ADD CONSTRAINT fkey_5 FOREIGN KEY (col_1) REFERENCES local_table_3(col_1);

-- show that all of below fails as we didn't provide cascade=true
SELECT create_citus_local_table('local_table_1');
SELECT create_citus_local_table('local_table_4', cascade=>false);

-- In each of below two transaction blocks, show that we preserve foreign keys.
-- Also show that we converted all local_table_xxx tables in current schema
-- to citus local tables after create_citus_local_table (cascade).
-- So in each transaction, both selects should return true.

BEGIN;
  SELECT create_citus_local_table('local_table_1', cascade=>true);

  -- show that we switch to sequential execution
  show citus.multi_shard_modify_mode;

  SELECT COUNT(*)=5 FROM pg_constraint WHERE conname ~ '^fkey\_\d+$';

  SELECT COUNT(*)=4 FROM pg_dist_partition, pg_tables
  WHERE tablename=logicalrelid::regclass::text AND
        schemaname='create_citus_local_table_cascade';
ROLLBACK;

BEGIN;
  SELECT create_citus_local_table('local_table_4', cascade=>true);

  SELECT COUNT(*)=5 FROM pg_constraint WHERE conname ~ '^fkey\_\d+$';

  SELECT COUNT(*)=4 FROM pg_dist_partition, pg_tables
  WHERE tablename=logicalrelid::regclass::text AND
        schemaname='create_citus_local_table_cascade';
ROLLBACK;

BEGIN;
  CREATE TABLE partitioned_table (col_1 INT REFERENCES local_table_1 (col_1)) PARTITION BY RANGE (col_1);
  -- now that we introduced a partitioned table into our foreign key subgraph,
  -- create_citus_local_table(cascade) would fail for partitioned_table as
  -- create_citus_local_table doesn't support partitioned tables
  SELECT create_citus_local_table('local_table_2', cascade=>true);
ROLLBACK;

BEGIN;
  DROP TABLE local_table_2;
  -- show that create_citus_local_table(cascade) works fine after
  -- dropping one of the relations from foreign key graph
  SELECT create_citus_local_table('local_table_1', cascade=>true);
ROLLBACK;

BEGIN;
  -- split local_table_2 from foreign key subgraph
  ALTER TABLE local_table_1 DROP CONSTRAINT local_table_1_col_1_key CASCADE;
  -- now local_table_2 does not have any foreign keys, cascade=true
  -- is not needed but show that it works fine
  SELECT create_citus_local_table('local_table_2', cascade=>true);
ROLLBACK;

CREATE TABLE distributed_table(col INT);
SELECT create_distributed_Table('distributed_table', 'col');

BEGIN;
  SELECT * FROM distributed_table;
  -- fails as above select executes in parallel mode but
  -- create_citus_local_table(cascade) needs to be executed
  -- via sequential execution
  SELECT create_citus_local_table('local_table_4', cascade=>true);
ROLLBACK;

-- test behaviour when outside of transaction block
SELECT create_citus_local_table('local_table_4', cascade=>true);

-- cleanup at exit
DROP SCHEMA create_citus_local_table_cascade CASCADE;
