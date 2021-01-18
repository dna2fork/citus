CREATE FUNCTION pg_catalog.worker_fix_partition_constraints(table_name regclass,
												 shardid integer)
  RETURNS void
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$worker_fix_partition_constraints$$;
COMMENT ON FUNCTION pg_catalog.worker_fix_partition_constraints(table_name regclass,
													 shardid integer)
  IS 'fix constraint names on partition shards on worker nodes';
REVOKE ALL ON FUNCTION pg_catalog.worker_fix_partition_constraints(regclass,int) FROM PUBLIC;
