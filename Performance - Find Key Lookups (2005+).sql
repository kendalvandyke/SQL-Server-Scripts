/*********************************************************************************************
Find Key Lookups in Cached Plans v1.00 (2010-07-27)
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

CREATE TABLE #Indexes (
		database_id INT NOT NULL
		, object_id INT NOT NULL
		, index_id INT NOT NULL
		, index_name sysname NOT NULL
		, PRIMARY KEY CLUSTERED (database_id, object_id, index_name)
	);

PRINT 'DO' +
'Something' +
'here'


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
				HAVING	SUM(execution_count) > 1		-- Ignore queries with a single execution
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
	SELECT --TOP 100
			sql_text.text
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
cteLookups AS (
	SELECT	plans.PlanId
		  , DB_ID(PARSENAME(ObjectXML.value('(Object/@Database)[1]', 'sysname'), 1)) AS database_id
		  , OBJECT_ID(ObjectXML.value('(Object/@Database)[1]', 'sysname') + '.' + ObjectXML.value('(Object/@Schema)[1]', 'sysname') + '.' + ObjectXML.value('(Object/@Table)[1]', 'sysname')) AS object_id
		  , ObjectXML.value('(Object/@Database)[1]', 'sysname') AS [database]
		  , ObjectXML.value('(Object/@Schema)[1]', 'sysname') AS [schema]
		  , ObjectXML.value('(Object/@Table)[1]', 'sysname') AS [table]
		  , ObjectXML.value('(Object/@Index)[1]', 'sysname') AS [index]
		  , REPLACE(ObjectXML.query('for $column in DefinedValues/DefinedValue/ColumnReference return string($column/@Column)').value('.', 'varchar(max)'), ' ', ', ') AS columns
	FROM	#Plans AS plans
			CROSS APPLY execution_plan.nodes('//RelOp') AS RelOps ( RelOpXML )
			CROSS APPLY RelOpXML.nodes('./IndexScan[@Lookup="1"]') AS KeyLookups ( ObjectXML )
),
cteLookupPlanGroups AS (
	SELECT	PlanId
		  , database_id
		  , object_id
		  , [database]
		  , [schema]
		  , [table]
		  , [index]
		  , columns
		  , DENSE_RANK() OVER ( ORDER BY database_id, object_id, [index] ) AS PlanGroupId
		  , DENSE_RANK() OVER ( PARTITION BY database_id, object_id, [index] ORDER BY database_id, object_id, [index], PlanId ) AS PlanNumber
	FROM	cteLookups
),
cteLookupPlanGroupSummary AS (
	SELECT	PlanGroupId
		  , MAX(PlanNumber) AS PlanCount
	FROM	cteLookupPlanGroups
	GROUP BY PlanGroupId
)
SELECT	cteLookups.PlanId
	  , cteLookups.[database]
	  , cteLookups.[schema]
	  , cteLookups.[table]
	  , cteLookups.[index]
	  , cteLookups.columns
	  , cteLookupPlanGroupSummary.PlanCount AS PlansUsingThisIndex
	  , index_stats.user_lookups
	  , index_stats.last_user_lookup
	  , plans.execution_count
	  , plans.total_worker_time
	  , plans.total_physical_reads
	  , plans.total_logical_reads
	  , plans.last_execution_time
	  , plans.o_name AS object_name
	  , plans.query_text
	  , plans.execution_plan
FROM	cteLookupPlanGroups AS cteLookups
		INNER JOIN cteLookupPlanGroupSummary ON cteLookups.PlanGroupId = cteLookupPlanGroupSummary.PlanGroupId
		INNER JOIN #Plans AS plans ON cteLookups.PlanId = plans.PlanId
		INNER JOIN sys.dm_db_index_usage_stats AS index_stats ON cteLookups.database_id = index_stats.database_id
																   AND cteLookups.object_id = index_stats.object_id
WHERE	index_stats.user_lookups > 0
		AND cteLookups.[database] NOT IN ( '[master]', '[model]', '[msdb]', '[tempdb]' )
ORDER BY plans.execution_count DESC;
--ORDER BY index_stats.user_lookups DESC
--ORDER BY [plans].total_logical_reads DESC



DROP TABLE #Plans;
DROP TABLE #Indexes;