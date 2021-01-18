CREATE FUNCTION pg_catalog.fix_partition_constraints(table_name regclass)
  RETURNS void
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$fix_partition_constraints$$;
COMMENT ON FUNCTION pg_catalog.fix_partition_constraints(table_name regclass)
  IS 'fix constraint names on partition shards';
