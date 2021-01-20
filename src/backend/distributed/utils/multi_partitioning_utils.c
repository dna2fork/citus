/*
 * multi_partitioning_utils.c
 *	  Utility functions for declarative partitioning
 *
 * Copyright (c) Citus Data, Inc.
 */
#include "postgres.h"

#include "distributed/pg_version_constants.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "catalog/partition.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_inherits.h"
#include "common/string.h"
#include "distributed/citus_nodes.h"
#include "distributed/adaptive_executor.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/listutils.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/relay_utility.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/version_compat.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "pgstat.h"
#if PG_VERSION_NUM >= PG_VERSION_12
#include "partitioning/partdesc.h"
#endif
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

static char * PartitionBound(Oid partitionId);
static Relation try_relation_open_nolock(Oid relationId);
static List * CreateFixPartitionConstraintsTaskList(Oid relationId);
static List * GetShardIdAppendedConstraintNameList(Oid relationId, int32 shardId);
static char * GetRenameShardConstraintToOriginalCommand(Oid relationId,
														char *constraintName, int32
														shardId);

PG_FUNCTION_INFO_V1(fix_partition_constraints);
PG_FUNCTION_INFO_V1(worker_fix_partition_constraints);


/*
 * fix_partition_constraints fixes the constraint names of partitioned table shards on
 * workers.
 */
Datum
fix_partition_constraints(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	EnsureCoordinator();

	if (!PartitionedTable(relationId))
	{
		ereport(ERROR, (errmsg("could not fix partition constraints: "
							   "relation does not exist or is not partitioned")));
	}
	if (!IsCitusTable(relationId))
	{
		ereport(ERROR, (errmsg("fix_partition_constraints can only be called "
							   "for distributed partitioned tables")));
	}

	List *taskList = CreateFixPartitionConstraintsTaskList(relationId);
	bool localExecutionSupported = true;
	ExecuteUtilityTaskList(taskList, localExecutionSupported);

	PG_RETURN_VOID();
}


/*
 * worker_fix_partition_constraints fixes the constraint names on a worker given a shell
 * table name and shard id.
 */
Datum
worker_fix_partition_constraints(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	int32 shardId = PG_GETARG_INT32(1);

	if (!PartitionedTable(relationId))
	{
		ereport(ERROR, (errmsg("could not fix partition constraints: "
							   "relation does not exist or is not partitioned")));
	}

	List *constraintNameList = GetShardIdAppendedConstraintNameList(relationId, shardId);
	char *constraintName = NULL;
	foreach_ptr(constraintName, constraintNameList)
	{
		char *shardRenameDDLCommand =
			GetRenameShardConstraintToOriginalCommand(relationId, constraintName,
													  shardId);
		ExecuteAndLogDDLCommand(shardRenameDDLCommand);
	}

	PG_RETURN_VOID();
}


/*
 * CreateFixPartitionConstraintsTaskList goes over all the partitions of a distributed
 * partitioned table, and creates the list of tasks to execute worker_fix_partition_constraints
 * UDF on worker nodes.
 */
static List *
CreateFixPartitionConstraintsTaskList(Oid relationId)
{
	/* resulting task list */
	List *taskList = NIL;

	/* enumerate the tasks when putting them to the taskList */
	int taskId = 1;

	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *relationName = get_rel_name(relationId);

	List *shardIntervalList = LoadShardIntervalList(relationId);

	/* lock metadata before getting placement lists */
	LockShardListMetadata(shardIntervalList, ShareLock);

	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		uint64 shardId = shardInterval->shardId;
		char *shardRelationName = pstrdup(relationName);

		/* build shard relation name */
		AppendShardIdToName(&shardRelationName, shardId);

		char *quotedShardName = quote_qualified_identifier(schemaName, shardRelationName);

		StringInfo shardQueryString = makeStringInfo();
		appendStringInfo(shardQueryString,
						 "SELECT worker_fix_partition_constraints('%s', " UINT64_FORMAT
						 ")", quotedShardName, shardId);

		Task *task = CitusMakeNode(Task);
		task->jobId = INVALID_JOB_ID;
		task->taskId = taskId++;

		task->taskType = DDL_TASK;
		SetTaskQueryString(task, shardQueryString->data);
		task->dependentTaskList = NULL;
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->anchorShardId = shardId;
		task->taskPlacementList = ActiveShardPlacementList(shardId);

		taskList = lappend(taskList, task);
	}

	return taskList;
}


/*
 * GetShardIdAppendedConstraintNameList returns a list of constraint names that have
 * a shardId suffix matching the supplied value.
 *
 * Note that we skip all foreign key and unique constraints, as the single caller of this
 * function needs the list of constraints that should be renamed, and foreign keys and
 * unique constraints should always have shardId suffixes in their names.
 */
static List *
GetShardIdAppendedConstraintNameList(Oid relationId, int32 shardId)
{
	List *constraintNameList = NIL;

	int scanKeyCount = 1;
	ScanKeyData scanKey[1];

	Relation pgConstraint = table_open(ConstraintRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber, F_OIDEQ, relationId);

	bool useIndex = true;
	SysScanDesc scanDescriptor = systable_beginscan(pgConstraint,
													ConstraintRelidTypidNameIndexId,
													useIndex, NULL, scanKeyCount,
													scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);
		char *constraintName = NameStr(constraintForm->conname);

		/* skip foreign key and unique constraints */
		if (constraintForm->contype != CONSTRAINT_FOREIGN &&
			constraintForm->contype != CONSTRAINT_UNIQUE &&
			NameHasShardIdSuffix(constraintName, shardId))
		{
			constraintNameList = lappend(constraintNameList, pstrdup(constraintName));
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	table_close(pgConstraint, NoLock);

	return constraintNameList;
}


/*
 * GetRenameShardConstraintToOriginalCommand creates the command that will remove the
 * shardId suffix from a constraint name.
 */
static char *
GetRenameShardConstraintToOriginalCommand(Oid relationId, char *constraintName,
										  int32 shardId)
{
	char *qualifiedRelationName = generate_qualified_relation_name(relationId);
	const char *quotedConstraintName = quote_identifier(constraintName);

	/* create the shardId suffix string */
	StringInfo shardIdSuffix = makeStringInfo();
	appendStringInfo(shardIdSuffix, "_%d", shardId);
	Assert(pg_str_endswith(constraintName, shardIdSuffix->data));

	/* remove the shardId suffix */
	char *newConstraintName =
		pnstrdup(constraintName,
				 strlen(constraintName) - strlen(shardIdSuffix->data));
	const char *quotedNewConstraintName = quote_identifier(newConstraintName);

	StringInfo renameCommand = makeStringInfo();
	appendStringInfo(renameCommand, "ALTER TABLE %s RENAME CONSTRAINT %s TO %s;",
					 qualifiedRelationName, quotedConstraintName,
					 quotedNewConstraintName);

	return renameCommand->data;
}


/*
 * Returns true if the given relation is a partitioned table.
 */
bool
PartitionedTable(Oid relationId)
{
	Relation rel = try_relation_open(relationId, AccessShareLock);

	/* don't error out for tables that are dropped */
	if (rel == NULL)
	{
		return false;
	}

	bool partitionedTable = false;

	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		partitionedTable = true;
	}

	/* keep the lock */
	table_close(rel, NoLock);

	return partitionedTable;
}


/*
 * Returns true if the given relation is a partitioned table. The function
 * doesn't acquire any locks on the input relation, thus the caller is
 * reponsible for holding the appropriate locks.
 */
bool
PartitionedTableNoLock(Oid relationId)
{
	Relation rel = try_relation_open_nolock(relationId);
	bool partitionedTable = false;

	/* don't error out for tables that are dropped */
	if (rel == NULL)
	{
		return false;
	}

	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		partitionedTable = true;
	}

	/* keep the lock */
	table_close(rel, NoLock);

	return partitionedTable;
}


/*
 * Returns true if the given relation is a partition.
 */
bool
PartitionTable(Oid relationId)
{
	Relation rel = try_relation_open(relationId, AccessShareLock);

	/* don't error out for tables that are dropped */
	if (rel == NULL)
	{
		return false;
	}

	bool partitionTable = rel->rd_rel->relispartition;

	/* keep the lock */
	table_close(rel, NoLock);

	return partitionTable;
}


/*
 * Returns true if the given relation is a partition.  The function
 * doesn't acquire any locks on the input relation, thus the caller is
 * reponsible for holding the appropriate locks.
 */
bool
PartitionTableNoLock(Oid relationId)
{
	Relation rel = try_relation_open_nolock(relationId);

	/* don't error out for tables that are dropped */
	if (rel == NULL)
	{
		return false;
	}

	bool partitionTable = rel->rd_rel->relispartition;

	/* keep the lock */
	table_close(rel, NoLock);

	return partitionTable;
}


/*
 * try_relation_open_nolock opens a relation with given relationId without
 * acquiring locks. PostgreSQL's try_relation_open() asserts that caller
 * has already acquired a lock on the relation, which we don't always do.
 *
 * ATTENTION:
 *   1. Sync this with try_relation_open(). It hasn't changed for 10 to 12
 *      releases though.
 *   2. We should remove this after we fix the locking/distributed deadlock
 *      issues with MX Truncate. See https://github.com/citusdata/citus/pull/2894
 *      for more discussion.
 */
static Relation
try_relation_open_nolock(Oid relationId)
{
	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relationId)))
	{
		return NULL;
	}

	Relation relation = RelationIdGetRelation(relationId);
	if (!RelationIsValid(relation))
	{
		return NULL;
	}

	pgstat_initstats(relation);

	return relation;
}


/*
 * IsChildTable returns true if the table is inherited. Note that
 * partition tables inherites by default. However, this function
 * returns false if the given table is a partition.
 */
bool
IsChildTable(Oid relationId)
{
	ScanKeyData key[1];
	HeapTuple inheritsTuple = NULL;
	bool tableInherits = false;

	Relation pgInherits = table_open(InheritsRelationId, AccessShareLock);

	ScanKeyInit(&key[0], Anum_pg_inherits_inhrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relationId));

	SysScanDesc scan = systable_beginscan(pgInherits, InvalidOid, false,
										  NULL, 1, key);

	while ((inheritsTuple = systable_getnext(scan)) != NULL)
	{
		Oid inheritedRelationId =
			((Form_pg_inherits) GETSTRUCT(inheritsTuple))->inhrelid;

		if (relationId == inheritedRelationId)
		{
			tableInherits = true;
			break;
		}
	}

	systable_endscan(scan);
	table_close(pgInherits, AccessShareLock);

	if (tableInherits && PartitionTable(relationId))
	{
		tableInherits = false;
	}

	return tableInherits;
}


/*
 * IsParentTable returns true if the table is inherited. Note that
 * partitioned tables inherited by default. However, this function
 * returns false if the given table is a partitioned table.
 */
bool
IsParentTable(Oid relationId)
{
	ScanKeyData key[1];
	bool tableInherited = false;

	Relation pgInherits = table_open(InheritsRelationId, AccessShareLock);

	ScanKeyInit(&key[0], Anum_pg_inherits_inhparent,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relationId));

	SysScanDesc scan = systable_beginscan(pgInherits, InheritsParentIndexId, true,
										  NULL, 1, key);

	if (systable_getnext(scan) != NULL)
	{
		tableInherited = true;
	}
	systable_endscan(scan);
	table_close(pgInherits, AccessShareLock);

	if (tableInherited && PartitionedTable(relationId))
	{
		tableInherited = false;
	}

	return tableInherited;
}


/*
 * Wrapper around get_partition_parent
 *
 * Note: Because this function assumes that the relation whose OID is passed
 * as an argument will have precisely one parent, it should only be called
 * when it is known that the relation is a partition.
 */
Oid
PartitionParentOid(Oid partitionOid)
{
	Oid partitionParentOid = get_partition_parent(partitionOid);

	return partitionParentOid;
}


/*
 * LongestPartitionName is a uitility function that returns the partition
 * name which is the longest in terms of number of characters.
 */
char *
LongestPartitionName(Oid parentRelationId)
{
	char *longestName = NULL;
	int longestNameLength = 0;
	List *partitionList = PartitionList(parentRelationId);

	Oid partitionRelationId = InvalidOid;
	foreach_oid(partitionRelationId, partitionList)
	{
		char *partitionName = get_rel_name(partitionRelationId);
		int partitionNameLength = strnlen(partitionName, NAMEDATALEN);
		if (partitionNameLength > longestNameLength)
		{
			longestName = partitionName;
			longestNameLength = partitionNameLength;
		}
	}

	return longestName;
}


/*
 * Takes a parent relation and returns Oid list of its partitions. The
 * function errors out if the given relation is not a parent.
 */
List *
PartitionList(Oid parentRelationId)
{
	Relation rel = table_open(parentRelationId, AccessShareLock);
	List *partitionList = NIL;


	if (!PartitionedTable(parentRelationId))
	{
		char *relationName = get_rel_name(parentRelationId);

		ereport(ERROR, (errmsg("\"%s\" is not a parent table", relationName)));
	}
	PartitionDesc partDesc = RelationGetPartitionDesc(rel);
	Assert(partDesc != NULL);

	int partitionCount = partDesc->nparts;
	for (int partitionIndex = 0; partitionIndex < partitionCount; ++partitionIndex)
	{
		partitionList =
			lappend_oid(partitionList, partDesc->oids[partitionIndex]);
	}

	/* keep the lock */
	table_close(rel, NoLock);

	return partitionList;
}


/*
 * GenerateDetachPartitionCommand gets a partition table and returns
 * "ALTER TABLE parent_table DETACH PARTITION partitionName" command.
 */
char *
GenerateDetachPartitionCommand(Oid partitionTableId)
{
	StringInfo detachPartitionCommand = makeStringInfo();

	if (!PartitionTable(partitionTableId))
	{
		char *relationName = get_rel_name(partitionTableId);

		ereport(ERROR, (errmsg("\"%s\" is not a partition", relationName)));
	}

	Oid parentId = get_partition_parent(partitionTableId);
	char *tableQualifiedName = generate_qualified_relation_name(partitionTableId);
	char *parentTableQualifiedName = generate_qualified_relation_name(parentId);

	appendStringInfo(detachPartitionCommand,
					 "ALTER TABLE IF EXISTS %s DETACH PARTITION %s;",
					 parentTableQualifiedName, tableQualifiedName);

	return detachPartitionCommand->data;
}


/*
 * GenereatePartitioningInformation returns the partitioning type and partition column
 * for the given parent table in the form of "PARTITION TYPE (partitioning column(s)/expression(s))".
 */
char *
GeneratePartitioningInformation(Oid parentTableId)
{
	char *partitionBoundCString = "";

	if (!PartitionedTable(parentTableId))
	{
		char *relationName = get_rel_name(parentTableId);

		ereport(ERROR, (errmsg("\"%s\" is not a parent table", relationName)));
	}

	Datum partitionBoundDatum = DirectFunctionCall1(pg_get_partkeydef,
													ObjectIdGetDatum(parentTableId));

	partitionBoundCString = TextDatumGetCString(partitionBoundDatum);

	return partitionBoundCString;
}


/*
 * GenerateAttachShardPartitionCommand generates command to attach a child table
 * table to its parent in a partitioning hierarchy.
 */
char *
GenerateAttachShardPartitionCommand(ShardInterval *shardInterval)
{
	Oid schemaId = get_rel_namespace(shardInterval->relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *escapedSchemaName = quote_literal_cstr(schemaName);

	char *command = GenerateAlterTableAttachPartitionCommand(shardInterval->relationId);
	char *escapedCommand = quote_literal_cstr(command);
	int shardIndex = ShardIndex(shardInterval);


	StringInfo attachPartitionCommand = makeStringInfo();

	Oid parentRelationId = PartitionParentOid(shardInterval->relationId);
	if (parentRelationId == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cannot attach partition"),
						errdetail("Referenced relation cannot be found.")));
	}

	Oid parentSchemaId = get_rel_namespace(parentRelationId);
	char *parentSchemaName = get_namespace_name(parentSchemaId);
	char *escapedParentSchemaName = quote_literal_cstr(parentSchemaName);
	uint64 parentShardId = ColocatedShardIdInRelation(parentRelationId, shardIndex);

	appendStringInfo(attachPartitionCommand,
					 WORKER_APPLY_INTER_SHARD_DDL_COMMAND, parentShardId,
					 escapedParentSchemaName, shardInterval->shardId,
					 escapedSchemaName, escapedCommand);

	return attachPartitionCommand->data;
}


/*
 * GenerateAlterTableAttachPartitionCommand returns the necessary command to
 * attach the given partition to its parent.
 */
char *
GenerateAlterTableAttachPartitionCommand(Oid partitionTableId)
{
	StringInfo createPartitionCommand = makeStringInfo();


	if (!PartitionTable(partitionTableId))
	{
		char *relationName = get_rel_name(partitionTableId);

		ereport(ERROR, (errmsg("\"%s\" is not a partition", relationName)));
	}

	Oid parentId = get_partition_parent(partitionTableId);
	char *tableQualifiedName = generate_qualified_relation_name(partitionTableId);
	char *parentTableQualifiedName = generate_qualified_relation_name(parentId);

	char *partitionBoundCString = PartitionBound(partitionTableId);

	appendStringInfo(createPartitionCommand, "ALTER TABLE %s ATTACH PARTITION %s %s;",
					 parentTableQualifiedName, tableQualifiedName,
					 partitionBoundCString);

	return createPartitionCommand->data;
}


/*
 * This function heaviliy inspired from RelationBuildPartitionDesc()
 * which is avaliable in src/backend/catalog/partition.c.
 *
 * The function simply reads the pg_class and gets the partition bound.
 * Later, converts it to text format and returns.
 */
static char *
PartitionBound(Oid partitionId)
{
	bool isnull = false;

	HeapTuple tuple = SearchSysCache1(RELOID, partitionId);
	if (!HeapTupleIsValid(tuple))
	{
		elog(ERROR, "cache lookup failed for relation %u", partitionId);
	}

	/*
	 * It is possible that the pg_class tuple of a partition has not been
	 * updated yet to set its relpartbound field.  The only case where
	 * this happens is when we open the parent relation to check using its
	 * partition descriptor that a new partition's bound does not overlap
	 * some existing partition.
	 */
	if (!((Form_pg_class) GETSTRUCT(tuple))->relispartition)
	{
		ReleaseSysCache(tuple);
		return "";
	}

	Datum datum = SysCacheGetAttr(RELOID, tuple,
								  Anum_pg_class_relpartbound,
								  &isnull);
	Assert(!isnull);

	Datum partitionBoundDatum =
		DirectFunctionCall2(pg_get_expr, datum, ObjectIdGetDatum(partitionId));

	char *partitionBoundString = TextDatumGetCString(partitionBoundDatum);

	ReleaseSysCache(tuple);

	return partitionBoundString;
}
