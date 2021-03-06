--
-- Test the CREATE statements related to columnar.
--


-- Create uncompressed table
CREATE TABLE contestant (handle TEXT, birthdate DATE, rating INT,
	percentile FLOAT, country CHAR(3), achievements TEXT[])
	USING columnar;

-- should fail
CREATE INDEX contestant_idx on contestant(handle);

-- Create compressed table with automatically determined file path
-- COMPRESSED
CREATE TABLE contestant_compressed (handle TEXT, birthdate DATE, rating INT,
	percentile FLOAT, country CHAR(3), achievements TEXT[])
	USING columnar;

-- Test that querying an empty table works
ANALYZE contestant;
SELECT count(*) FROM contestant;

-- Should fail: unlogged tables not supported
CREATE UNLOGGED TABLE columnar_unlogged(i int) USING columnar;

-- Should fail: temporary tables not supported
CREATE TEMPORARY TABLE columnar_temp(i int) USING columnar;

--
-- Utility functions to be used throughout tests
--

CREATE FUNCTION columnar_relation_storageid(relid oid) RETURNS bigint
    LANGUAGE C STABLE STRICT
    AS 'citus', $$columnar_relation_storageid$$;

CREATE FUNCTION compression_type_supported(type text) RETURNS boolean
AS $$
BEGIN
   EXECUTE 'SET LOCAL columnar.compression TO ' || quote_literal(type);
   return true;
EXCEPTION WHEN invalid_parameter_value THEN
   return false;
END;
$$ LANGUAGE plpgsql;
