-- test cases for #3970
SET citus.next_shard_id TO 1690000;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

CREATE SCHEMA test_3970;
SET search_path = test_3970;

--1. create a partitioned table
CREATE TABLE part_table (
    work_ymdt timestamp without time zone NOT NULL,
    seq bigint NOT NULL,
    my_seq bigint NOT NULL,
    work_memo character varying(150),
    CONSTRAINT work_memo_check CHECK ((octet_length((work_memo)::text) <= 150))
)
PARTITION BY RANGE (work_ymdt);

--2. perform create_distributed_table
SELECT create_distributed_table('part_table', 'seq');

--3. add a partition
CREATE TABLE part_table_p202008 PARTITION OF part_table FOR VALUES FROM ('2020-08-01 00:00:00') TO ('2020-09-01 00:00:00');

--4. add a check constraint
ALTER TABLE part_table ADD CONSTRAINT my_seq CHECK (my_seq > 0);

--5. add a partition
CREATE TABLE part_table_p202009 PARTITION OF part_table FOR VALUES FROM ('2020-09-01 00:00:00') TO ('2020-10-01 00:00:00');

-- check the constraint names on the coordinator node
\d+ test_3970.part_table*

-- check the constraint names on the worker node
-- See that we append shardid to constraints created by an
-- ALTER TABLE .. ADD CONSTRAINT, whereas we leave the constraint name as is if
-- the constraint existed before we distributed the table.
\c - - - :worker_1_port
\d+ test_3970.part_table*

\c - - - :master_port
SET search_path = test_3970;

-- verify that we can drop the constraints on partitioned tables
ALTER TABLE part_table DROP CONSTRAINT my_seq;

DROP TABLE part_table CASCADE;
DROP SCHEMA test_3970;
