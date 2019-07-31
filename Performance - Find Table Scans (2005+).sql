/*********************************************************************************************
Find Table Scans in Cached Plans v1.00 (2010-07-27)
(C) 2010, Kendal Van Dyke

Feedback: mailto:kendal.vandyke@gmail.com

License: 
	This query is free to download and use for personal, educational, and internal 
	corporate purposes, provided that this header is preserved. Redistribution or sale 
	of this query, in whole or in part, is prohibited without the author's express 
	written consent.
	
Note: 
	Exercise caution when running this in production!

	The function sys.dm_exec_query_plan() is resource intensive and can put strain
	on a server when used to retrieve all cached query plans. 

	Consider using TOP in the initial select statement (insert into #Plans)
	to limit the impact of running this query or run during non-peak hours
*********************************************************************************************/
DECLARE	@sql NVARCHAR(4000)
	, @DatabaseName sysname
	, @CurrentDatabaseOnly BIT = 0;

CREATE TABLE #Plans (
		PlanId INT IDENTITY(1, 1)
		, query_text NVARCHAR(MAX)
		, o_name sysname
		, execution_plan XML
		, last_execution_time DATETIME
		, execution_count BIGINT
		, total_worker_time BIGINT
		, total_physical_reads BIGINT
		, total_logical_reads BIGINT
	);

WITH	cteQueryStats
			AS (
				SELECT	sql_handle
						, plan_handle
						, MAX(last_execution_time) AS last_execution_time
						, SUM(execution_count) AS execution_count
						, SUM(total_worker_time) AS total_worker_time
						, SUM(total_physical_reads) AS total_physical_reads
						, SUM(total_logical_reads) AS total_logical_reads
				FROM	sys.dm_exec_query_stats
				GROUP BY sql_handle
						, plan_handle
				HAVING	SUM(execution_count) > 1	-- Ignore queries with a single execution
						
				)
	INSERT	INTO #Plans
			(	query_text
				, o_name
				, execution_plan
				, last_execution_time
				, execution_count
				, total_worker_time
				, total_physical_reads
				, total_logical_reads
			)
	SELECT	sql_text.text
			, CASE	WHEN sql_text.objectid IS NOT NULL THEN ISNULL(OBJECT_NAME(sql_text.objectid, sql_text.dbid), 'Unresolved')
					ELSE CAST('Ad-hoc\Prepared' AS SYSNAME)
				END
			, query_plan.query_plan
			, cteQueryStats.last_execution_time
			, cteQueryStats.execution_count
			, cteQueryStats.total_worker_time
			, cteQueryStats.total_physical_reads
			, cteQueryStats.total_logical_reads
	FROM	cteQueryStats
			CROSS APPLY sys.dm_exec_sql_text(cteQueryStats.sql_handle) AS sql_text
			CROSS APPLY sys.dm_exec_query_plan(cteQueryStats.plan_handle) AS query_plan
	WHERE	query_plan.query_plan IS NOT NULL;
;


WITH XMLNAMESPACES (
	DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan'
),
cteTableScanOperations AS (
	SELECT  DISTINCT
			plans.PlanId
		  , ObjectXML.value('(Object/@Database)[1]', 'sysname') AS [database]
		  , ObjectXML.value('(Object/@Schema)[1]', 'sysname') AS [schema]
		  , ObjectXML.value('(Object/@Table)[1]', 'sysname') AS [table]
		  , ObjectXML.value('(Object/@Index)[1]', 'sysname') AS [index]
		  , ObjectXML.value('(Object/@IndexKind)[1]', 'sysname') AS indexkind
		  , REPLACE(ObjectXML.query('for $column in DefinedValues/DefinedValue/ColumnReference return string($column/@Column)').value('.', 'varchar(max)'), ' ', ', ') AS columns
	FROM	#Plans AS plans
			CROSS APPLY execution_plan.nodes('//RelOp') AS RelOps ( RelOpXML )
			CROSS APPLY RelOpXML.nodes('./TableScan') AS TableScans ( ObjectXML )
	WHERE	ObjectXML.value('(Object/@Database)[1]', 'sysname') = CASE WHEN @CurrentDatabaseOnly = 1 THEN QUOTENAME(DB_NAME())
																	   ELSE ObjectXML.value('(Object/@Database)[1]', 'sysname')
																  END
),
cteTableScans AS (
	SELECT	DB_ID(PARSENAME(cteTableScanOperations.[database], 1)) AS database_id
		  , OBJECT_ID(cteTableScanOperations.[database] + '.' + cteTableScanOperations.[schema] + '.' + cteTableScanOperations.[table]) AS object_id
		  , cteTableScanOperations.[database]
		  , cteTableScanOperations.[schema]
		  , cteTableScanOperations.[table]
		  , cteTableScanOperations.[index]
		  , cteTableScanOperations.indexkind
		  , cteTableScanOperations.columns
		  , plans.o_name
		  , plans.last_execution_time
		  , plans.execution_count
		  , plans.total_worker_time
		  , plans.total_physical_reads
		  , plans.total_logical_reads
		  , plans.query_text
		  , plans.execution_plan
	FROM	cteTableScanOperations
			INNER JOIN #Plans AS plans ON cteTableScanOperations.PlanId = plans.PlanId
)
SELECT	cteTableScans.database_id
	  , cteTableScans.object_id
	  , cteTableScans.[database]
	  , cteTableScans.[schema]
	  , cteTableScans.[table]
	  , cteTableScans.[index]
	  , cteTableScans.indexkind
	  , cteTableScans.columns
	  , cteTableScans.o_name
	  , cteTableScans.last_execution_time
	  , cteTableScans.execution_count
	  , cteTableScans.total_worker_time
	  , cteTableScans.total_physical_reads
	  , cteTableScans.total_logical_reads
	  , cteTableScans.query_text
	  , cteTableScans.execution_plan
FROM	cteTableScans
WHERE	[database] NOT IN ( '[master]', '[model]', '[msdb]', '[tempdb]' )
ORDER BY total_logical_reads DESC;


DROP TABLE #Plans;