/*********************************************************************************************
Find Indexes Used in Cached Plans v1.00 (2015-05-13)
(C) 2015, Kendal Van Dyke

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
		, total_logical_reads BIGINT ,
	);

CREATE TABLE #Indexes (
		database_id INT NOT NULL
		, object_id INT NOT NULL
		, index_id INT NOT NULL
		, index_name sysname NOT NULL
		, PRIMARY KEY CLUSTERED (database_id, object_id, index_name)
	);

WITH	cteQueryStats
			AS (
				SELECT --TOP 10
						sql_handle
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
			   --ORDER BY	SUM(total_logical_reads) DESC
						
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


/* Retrieve index IDs from each database to correlate indexes in plans with their usage counts */
DECLARE curDatabase CURSOR LOCAL FAST_FORWARD
FOR
SELECT	name
FROM	sys.databases
WHERE	name NOT IN ('[master]', '[model]', '[msdb]', '[tempdb]')
		AND name = CASE	WHEN @CurrentDatabaseOnly = 1 THEN DB_NAME()
						ELSE name
					END;
OPEN curDatabase;
FETCH NEXT FROM curDatabase INTO @DatabaseName;
WHILE @@FETCH_STATUS = 0
BEGIN
	SET @sql = N'INSERT INTO #Indexes (database_id, [object_id], index_id, index_name) SELECT DB_ID(''' + @DatabaseName
		+ '''), [object_id], index_id, name FROM ' + QUOTENAME(@DatabaseName) + '.sys.indexes WHERE name IS NOT NULL;';
	--PRINT @sql;
	EXECUTE sp_executesql @sql;
	FETCH NEXT FROM curDatabase INTO @DatabaseName;
END;
CLOSE curDatabase;
DEALLOCATE curDatabase;



WITH XMLNAMESPACES (
	DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan'
),
cteIndexUsageOperations AS (
	SELECT  DISTINCT
			plans.PlanId
		  , ObjectXML.value('@Database', 'sysname') AS [database]
		  , ObjectXML.value('@Schema', 'sysname') AS [schema]
		  , ObjectXML.value('@Table', 'sysname') AS [table]
		  , ObjectXML.value('@Index', 'sysname') AS [index]
	FROM	#Plans AS plans
			CROSS APPLY execution_plan.nodes('//RelOp') AS RelOps ( RelOpXML )
			CROSS APPLY RelOpXML.nodes('./IndexScan/Object[@Index]') AS IndexScans ( ObjectXML )
	WHERE	ObjectXML.value('@Database', 'sysname') = CASE WHEN @CurrentDatabaseOnly = 1 THEN QUOTENAME(DB_NAME())
														   ELSE ObjectXML.value('@Database', 'sysname')
													  END
), cteIndexUsage AS (
	SELECT	cteIndexUsageOperations.PlanId
		  , DB_ID(PARSENAME(cteIndexUsageOperations.[database], 1)) AS database_id
		  , OBJECT_ID(cteIndexUsageOperations.[database] + '.' + cteIndexUsageOperations.[schema] + '.' + cteIndexUsageOperations.[table]) AS object_id
		  , cteIndexUsageOperations.[database]
		  , cteIndexUsageOperations.[schema]
		  , cteIndexUsageOperations.[table]
		  , cteIndexUsageOperations.[index]
		  , plans.o_name
		  , plans.last_execution_time
		  , plans.execution_count
		  , plans.total_worker_time
		  , plans.total_physical_reads
		  , plans.total_logical_reads
		  , plans.query_text
		  , plans.execution_plan
	FROM	cteIndexUsageOperations
			INNER JOIN #Plans AS plans ON cteIndexUsageOperations.PlanId = plans.PlanId
)
SELECT	cteIndexUsage.[database]
	  , cteIndexUsage.[schema]
	  , cteIndexUsage.[table]
	  , cteIndexUsage.[index]
	  , indexes.index_id
	  , index_stats.user_lookups
	  , index_stats.last_user_lookup
	  , index_stats.user_scans
	  , index_stats.last_user_scan
	  , index_stats.user_seeks
	  , index_stats.last_user_seek
	  , ( index_stats.user_lookups + index_stats.user_scans + index_stats.user_seeks ) AS TotalUserReadOperations
	  , index_stats.user_updates
	  , index_stats.last_user_update
	  , cteIndexUsage.execution_count
	  , cteIndexUsage.total_worker_time
	  , cteIndexUsage.total_physical_reads
	  , cteIndexUsage.total_logical_reads
	  , cteIndexUsage.last_execution_time
	  , cteIndexUsage.o_name AS object_name
	  , cteIndexUsage.query_text
	  , cteIndexUsage.execution_plan
FROM	cteIndexUsage
		LEFT OUTER JOIN #Indexes AS indexes ON cteIndexUsage.database_id = indexes.database_id
												 AND cteIndexUsage.object_id = indexes.object_id
												 AND cteIndexUsage.[index] = QUOTENAME(indexes.index_name)
		LEFT OUTER JOIN sys.dm_db_index_usage_stats AS index_stats ON indexes.database_id = index_stats.database_id
																		AND indexes.object_id = index_stats.object_id
																		AND indexes.index_id = index_stats.index_id
WHERE	cteIndexUsage.[database] NOT IN ( '[master]', '[model]', '[msdb]', '[tempdb]' )
	--AND index_stats.user_lookups > 0
ORDER BY cteIndexUsage.execution_count DESC;


DROP TABLE #Plans;
DROP TABLE #Indexes;