/*********************************************************************************************
Identify Missing Rowstore Disk Indexes v1.00 (2019-06-19)
(C) 2019, Kendal Van Dyke

Feedback: mailto:kendal.vandyke@gmail.com

License: 
	This query is free to download and use for personal, educational, and internal 
	corporate purposes, provided that this header is preserved. Redistribution or sale 
	of this query, in whole or in part, is prohibited without the author's express 
	written consent.
	
Note: 
- Some background reading to understand the motivation behind this script:
	- Limitations of the Missing Indexes Feature (SQL 2008 R2 docs)
		- https://docs.microsoft.com/en-us/previous-versions/sql/sql-server-2008-r2/ms345485(v=sql.105)
	- How does SQL Server determine key column order in missing index requests?
		- https://dba.stackexchange.com/questions/208947/how-does-sql-server-determine-key-column-order-in-missing-index-requests
	- Why Missing Index Recommendations Aren’t Perfect
		- https://www.brentozar.com/archive/2017/08/missing-index-recommendations-arent-perfect/
	- Query Tuning Fundamentals: Density, Predicates, Selectivity, and Cardinality
		- https://blogs.msdn.microsoft.com/bartd/2011/01/25/query-tuning-fundamentals-density-predicates-selectivity-and-cardinality/
	- Are you using SQL’s Missing Index DMVs?
		- https://blogs.msdn.microsoft.com/bartd/2007/07/19/are-you-using-sqls-missing-index-dmvs/
		- In particular, "It's possible that the DMVs may not recommend the ideal column order for multi-column indexes"

- How it works
	1) Use column stats for each column in the missing index to determine ideal order based on selectivity. 
		This is done with DBCC SHOW_STATISTICS and grab the density value for that column. 
		If you have multiple density values on that column then average them together (or take the highest density value to be conservative?). 
		Equality columns are most important, inequality columns are nice if we've got stats. 
		Within each usage group (EQUALITY and INEQUALITY) order by density (i.e selectivity) if there's stats on all columns within the group, 
		otherwise order by column name (the way missing index recommendations does today).
	
	2) Group indexes together. There are two ways to group: 1) equality columns and 2) key columns (equality + inequality). 
		INCLUDE columns are the distinct list of columns from the cartesian product of all INCLUDE columns for the group. 
		(e.g. Index 1 has key cols A & B with include cols C & D, index 2 has key cols A & B with include cols D & E, 
		so the resulting index has key cols A & B with include cols C, D, & E)
	
	3) Examine grouped indexes to see if any of them overlap each other, overlap existing indexes, or if they are overlapped by existing indexes.

*********************************************************************************************/

--USE [Database];
--GO

SET NOCOUNT ON;

CREATE TABLE #DbccShowStatistics (
	density FLOAT
	, avgLength FLOAT
	, cols NVARCHAR(1000)
);

CREATE TABLE #StatsDensity (
	database_id INT
	, object_id INT
	, column_id INT
	, density_ordinal INT
	, avg_density FLOAT
	, min_density FLOAT
	, max_density FLOAT
	, stats_count SMALLINT
);

CREATE TABLE #MissingIndexColumns (
	index_handle INT
	, column_id INT
	, column_name sysname
	, column_usage VARCHAR(20)
	, column_group_has_stats BIT
	, column_ordinal BIGINT
	, column_alpha_ordinal BIGINT
);

CREATE TABLE #MissingIndexes (
	database_id INT
	, object_id INT
	, obj_name sysname
	, index_handle INT
	, keycol_group_id INT
	, eqcol_group_id INT
	, all_improvement_measure FLOAT
	, db_improvement_measure FLOAT
	, keycol_group_improvement_measure FLOAT
	, eqcol_group_improvement_measure FLOAT
	, index_improvement_measure FLOAT
	, unique_compiles BIGINT
	, user_seeks BIGINT
	, user_scans BIGINT
	, last_user_seek DATETIME
	, last_user_scan DATETIME
	, avg_total_user_cost FLOAT
	, avg_user_impact FLOAT
	, equality_columns_original NVARCHAR(4000)
	, equality_columns_ordered NVARCHAR(4000)
	, inequality_columns_original NVARCHAR(4000)
	, inequality_columns_ordered NVARCHAR(4000)
	, is_equality_col_order_stats_based BIT
	, is_inequality_col_order_stats_based BIT
	, equality_columns NVARCHAR(4000)
	, inequality_columns NVARCHAR(4000)
	, key_columns NVARCHAR(4000)
	, included_columns NVARCHAR(4000)
);

CREATE TABLE #AllIndexes (
	SchemaId INT NOT NULL
	, ObjectId INT NOT NULL
	, IndexId INT NOT NULL
	, IndexColumnId INT NOT NULL
	, SchemaName sysname NOT NULL
	, TableName sysname NOT NULL
	, IndexName sysname NOT NULL
	, ColumnName sysname NOT NULL
	, key_ordinal TINYINT NOT NULL
	, partition_ordinal TINYINT NOT NULL
	, is_descending_key BIT NOT NULL
	, is_included_column BIT NOT NULL
	, has_filter BIT NOT NULL
	, is_disabled BIT NOT NULL
	, is_unique BIT NOT NULL
	, filter_definition NVARCHAR(MAX) NULL
	, MaxKeyOrdinal TINYINT NOT NULL
	, MaxPartitionOrdinal TINYINT NOT NULL
	, TotalColumnCount INT NOT NULL
	, IsMissingIndex BIT NOT NULL
	,
	PRIMARY KEY CLUSTERED (
		SchemaId
		, ObjectId
		, IndexId
		, IndexColumnId
		, key_ordinal
		, partition_ordinal
	)
);


/*
Step 1: Get density of missing index columns (EQUALITY and INEQUALITY) using index & column statistics
- To do this, we'll run DBCC SHOW_STATISTICS(...) WITH DENSITY_VECTOR 
	(see https://docs.microsoft.com/en-us/sql/t-sql/database-console-commands/dbcc-show-statistics-transact-sql?view=sql-server-2017#density)
- The density of the 1st column is a rough measure of selectivity that we'll use to reorder the equality columns 
	(since columns in missing index recommendations are simply alpha sorted)
- When there are multiple stats on a column use the average density of all stats on that column
*/
DECLARE @object_id INT
		, @statement NVARCHAR(4000);

DECLARE curObject CURSOR LOCAL FAST_FORWARD FOR
SELECT DISTINCT
	   object_id
FROM sys.dm_db_missing_index_details
WHERE database_id = DB_ID();
OPEN curObject;
FETCH NEXT FROM curObject
INTO @object_id;
WHILE @@fetch_status = 0
BEGIN

	TRUNCATE TABLE #DbccShowStatistics;

	DECLARE curStats CURSOR LOCAL FAST_FORWARD FOR
	SELECT DISTINCT
		   'INSERT INTO #DbccShowStatistics EXECUTE (''DBCC SHOW_STATISTICS (''''' + QUOTENAME(s.name) + '.' + QUOTENAME(o.name) + ''''',' + QUOTENAME(st.name) + ') WITH NO_INFOMSGS, DENSITY_VECTOR'');'
	FROM sys.schemas AS s
		INNER JOIN sys.objects AS o ON s.schema_id = o.schema_id
		INNER JOIN sys.dm_db_missing_index_details AS mid ON o.object_id = mid.object_id
		CROSS APPLY sys.dm_db_missing_index_columns(mid.index_handle) AS cols
		INNER JOIN sys.columns AS c ON o.object_id = c.object_id
									   AND cols.column_id = c.column_id
		INNER JOIN sys.stats_columns AS sc ON c.object_id = sc.object_id
											  AND c.column_id = sc.column_id
		INNER JOIN sys.stats AS st ON sc.object_id = st.object_id
									  AND sc.stats_id = st.stats_id
	WHERE o.object_id = @object_id
		  AND sc.stats_column_id = 1
		  AND cols.column_usage IN ( 'EQUALITY', 'INEQUALITY' );
	OPEN curStats;
	FETCH NEXT FROM curStats
	INTO @statement;
	WHILE @@fetch_status = 0
	BEGIN
		EXECUTE (@statement);
		FETCH NEXT FROM curStats
		INTO @statement;
	END;
	CLOSE curStats;
	DEALLOCATE curStats;


	INSERT INTO #StatsDensity
	(
		database_id
		, object_id
		, column_id
		, density_ordinal
		, avg_density
		, min_density
		, max_density
		, stats_count
	)
	SELECT DB_ID() AS database_id
		   , @object_id AS object_id
		   , c.column_id
		   , ROW_NUMBER() OVER (ORDER BY AVG(s.density), c.column_id) AS density_ordinal
		   , AVG(s.density) AS avg_density
		   , MIN(s.density) AS min_density
		   , MAX(s.density) AS max_density
		   , COUNT(*) AS stats_count
	FROM #DbccShowStatistics AS s
		INNER JOIN sys.columns AS c ON s.cols = c.name
	WHERE c.object_id = @object_id
		  AND CHARINDEX(',', s.cols) = 0
	GROUP BY c.column_id;

	FETCH NEXT FROM curObject
	INTO @object_id;
END;
CLOSE curObject;
DEALLOCATE curObject;

/*
-- Show the final results for collected stats
SELECT s.name AS [SchemaName]
	, o.name AS ObjectName
	, o.type_desc AS ObjectType
	, c.name AS column_name
	, sd.column_id
	, sd.density_ordinal
	, sd.avg_density
	, sd.min_density
	, sd.max_density
	, sd.stats_count
FROM #StatsDensity AS sd
	INNER JOIN sys.columns AS c ON sd.object_id = c.object_id
								AND sd.column_id = c.column_id
	INNER JOIN sys.objects AS o ON c.object_id = o.object_id
	INNER JOIN sys.schemas AS s ON o.schema_id = s.schema_id
ORDER BY s.name
	, o.name
	, sd.density_ordinal;
*/


/*
Step 2: Determine missing index column ordinal position within each usage type (EQUALITY, INEQUALITY, and INCLUDE)
	- Ordering by selectivity *really* only matters for EQUALITY. 
	- It doesn't hurt to order INEQUALITY columns, too, and it may even help when we consolidate indexes later on.
	- INCLUDE column order doesn't matter; Just keeping INCLUDE columns in this temp table because they'll be used later.
	- For each usage type, if we don't have stats on every column then fall back to ordering by column name.
*/
WITH
cteMissingIndexCols AS (
	SELECT DISTINCT
		   mid.database_id
		   , mid.object_id
		   , mid.index_handle
		   , cols.column_id
		   , cols.column_name
		   , cols.column_usage
		   , CASE
				 WHEN sc.stats_column_id IS NULL THEN 0
				 ELSE 1
			 END AS has_stats
	FROM sys.dm_db_missing_index_details AS mid
		CROSS APPLY sys.dm_db_missing_index_columns(mid.index_handle) AS cols
		LEFT OUTER JOIN sys.stats_columns AS sc ON mid.object_id = sc.object_id
												   AND cols.column_id = sc.column_id
												   AND sc.stats_column_id = 1
)
,
cteMissingIndexCols2 AS (
	SELECT mic.index_handle
		   , mic.column_id
		   , mic.column_name
		   , mic.column_usage
		   , MIN(mic.has_stats) OVER (PARTITION BY mic.index_handle, mic.column_usage) AS column_group_has_stats
		   , ROW_NUMBER() OVER (PARTITION BY mic.index_handle
											 , mic.column_usage
								ORDER BY sd.density_ordinal
										 , mic.column_name
						  ) AS density_ordinal_in_group
		   , ROW_NUMBER() OVER (PARTITION BY mic.index_handle
											 , mic.column_usage
								ORDER BY mic.column_name
						  ) AS column_name_ordinal_in_group
		   , CASE mic.column_usage
				 WHEN 'EQUALITY' THEN 1000.0
				 WHEN 'INEQUALITY' THEN 2000.0
				 ELSE 3000.0
			 END AS column_group
	FROM cteMissingIndexCols AS mic
		LEFT OUTER JOIN #StatsDensity AS sd ON mic.database_id = sd.database_id
											   AND mic.object_id = sd.object_id
											   AND mic.column_id = sd.column_id
)
,
cteMissingIndexCols3 AS (
	SELECT index_handle
		   , column_id
		   , column_name
		   , column_usage
		   , column_group_has_stats
		   , CASE column_group_has_stats
				 WHEN 1 THEN column_group + density_ordinal_in_group
				 ELSE column_group + column_name_ordinal_in_group
			 END AS column_calculated_order
		   , column_group + column_name_ordinal_in_group AS column_alpha_order
	FROM cteMissingIndexCols2
)
INSERT INTO #MissingIndexColumns
(
	index_handle
	, column_id
	, column_name
	, column_usage
	, column_group_has_stats
	, column_ordinal
	, column_alpha_ordinal
)
SELECT index_handle
	   , column_id
	   , column_name
	   , column_usage
	   , column_group_has_stats
	   , ROW_NUMBER() OVER (PARTITION BY index_handle ORDER BY column_calculated_order) AS column_ordinal
	   , ROW_NUMBER() OVER (PARTITION BY index_handle ORDER BY column_alpha_order) AS column_alpha_ordinal
FROM cteMissingIndexCols3;




/*
Step 3:  Stage missing indexes data
*/
TRUNCATE TABLE #MissingIndexes;

WITH
cteIncompleteEqualityStats AS (
	SELECT DISTINCT
		   index_handle
	FROM #MissingIndexColumns
	WHERE column_usage = 'EQUALITY'
		  AND column_group_has_stats = 0
)
,
cteIncompleteInequalityStats AS (
	SELECT DISTINCT
		   index_handle
	FROM #MissingIndexColumns
	WHERE column_usage = 'INEQUALITY'
		  AND column_group_has_stats = 0
)
,
cteMissingIndex AS (
	SELECT mid.database_id
		   , mid.object_id
		   , mid.statement AS obj_name
		   , mig.index_handle
		   , CONVERT(DECIMAL(28, 1), migs.avg_total_user_cost * migs.avg_user_impact * (migs.user_seeks + migs.user_scans)) AS index_improvement_measure
		   , migs.unique_compiles
		   , migs.user_seeks
		   , migs.user_scans
		   , migs.last_user_seek
		   , migs.last_user_scan
		   , migs.avg_total_user_cost
		   , migs.avg_user_impact
		   , mid.equality_columns
		   , (
				 SELECT QUOTENAME(cols.column_name) + ', '
				 FROM #MissingIndexColumns AS cols
				 WHERE cols.index_handle = mig.index_handle
					   AND cols.column_usage = 'EQUALITY'
				 ORDER BY cols.column_ordinal
				 FOR XML PATH('')
			 ) AS equality_columns_ordered
		   , mid.inequality_columns
		   , (
				 SELECT QUOTENAME(cols.column_name) + ', '
				 FROM #MissingIndexColumns AS cols
				 WHERE cols.index_handle = mig.index_handle
					   AND cols.column_usage = 'INEQUALITY'
				 ORDER BY cols.column_ordinal
				 FOR XML PATH('')
			 ) AS inequality_columns_ordered
		   , mid.included_columns
	FROM sys.dm_db_missing_index_groups AS mig
		INNER JOIN sys.dm_db_missing_index_group_stats AS migs ON migs.group_handle = mig.index_group_handle
		INNER JOIN sys.dm_db_missing_index_details AS mid ON mig.index_handle = mid.index_handle
)
,
cteMissingIndex2 AS (
	SELECT cteMissingIndex.database_id
		   , cteMissingIndex.object_id
		   , cteMissingIndex.obj_name
		   , cteMissingIndex.index_handle
		   , SUM(cteMissingIndex.index_improvement_measure) OVER () AS all_improvement_measure
		   , SUM(cteMissingIndex.index_improvement_measure) OVER (PARTITION BY cteMissingIndex.database_id) AS db_improvement_measure
		   , cteMissingIndex.index_improvement_measure
		   , cteMissingIndex.unique_compiles
		   , cteMissingIndex.user_seeks
		   , cteMissingIndex.user_scans
		   , cteMissingIndex.last_user_seek
		   , cteMissingIndex.last_user_scan
		   , cteMissingIndex.avg_total_user_cost
		   , cteMissingIndex.avg_user_impact
		   , cteMissingIndex.equality_columns AS equality_columns_original
		   , cteMissingIndex.equality_columns_ordered AS equality_columns_ordered
		   , cteMissingIndex.inequality_columns AS inequality_columns_original
		   , cteMissingIndex.inequality_columns_ordered
		   , CASE
				 WHEN cteIncompleteEqualityStats.index_handle IS NOT NULL THEN 0
				 ELSE 1
			 END AS is_equality_col_order_stats_based
		   , CASE
				 WHEN cteIncompleteInequalityStats.index_handle IS NOT NULL THEN 0
				 ELSE 1
			 END AS is_inequality_col_order_stats_based
		   , CASE
				 WHEN cteIncompleteEqualityStats.index_handle IS NOT NULL THEN cteMissingIndex.equality_columns
				 ELSE LEFT(cteMissingIndex.equality_columns_ordered, LEN(cteMissingIndex.equality_columns_ordered) - 1)
			 END AS equality_columns
		   , CASE
				 WHEN cteIncompleteInequalityStats.index_handle IS NOT NULL THEN cteMissingIndex.inequality_columns
				 ELSE LEFT(cteMissingIndex.inequality_columns_ordered, LEN(cteMissingIndex.inequality_columns_ordered) - 1)
			 END AS inequality_columns
		   , cteMissingIndex.included_columns
	FROM cteMissingIndex
		LEFT OUTER JOIN cteIncompleteEqualityStats ON cteMissingIndex.index_handle = cteIncompleteEqualityStats.index_handle
		LEFT OUTER JOIN cteIncompleteInequalityStats ON cteMissingIndex.index_handle = cteIncompleteInequalityStats.index_handle
)
,
cteMissingIndex3 AS (
	SELECT database_id
		   , object_id
		   , obj_name
		   , index_handle
		   , all_improvement_measure
		   , db_improvement_measure
		   , index_improvement_measure
		   , unique_compiles
		   , user_seeks
		   , user_scans
		   , last_user_seek
		   , last_user_scan
		   , avg_total_user_cost
		   , avg_user_impact
		   , equality_columns
		   , equality_columns_ordered
		   , inequality_columns
		   , inequality_columns_ordered
		   , is_equality_col_order_stats_based
		   , is_inequality_col_order_stats_based
		   , ISNULL(equality_columns, N'') + CASE
												 WHEN equality_columns IS NOT NULL
													  AND inequality_columns IS NOT NULL THEN N', '
												 ELSE N''
											 END + ISNULL(inequality_columns, N'') AS key_columns
		   , included_columns
	FROM cteMissingIndex2
	WHERE database_id = DB_ID()
)
,
cteMissingIndex4 AS (
	SELECT database_id
		   , object_id
		   , obj_name
		   , index_handle
		   , all_improvement_measure
		   , db_improvement_measure
		   , SUM(index_improvement_measure) OVER (PARTITION BY database_id, object_id, key_columns) AS keycol_group_improvement_measure
		   , SUM(index_improvement_measure) OVER (PARTITION BY database_id, object_id, key_columns, equality_columns) AS eqcol_group_improvement_measure
		   , index_improvement_measure
		   , unique_compiles
		   , user_seeks
		   , user_scans
		   , last_user_seek
		   , last_user_scan
		   , avg_total_user_cost
		   , avg_user_impact
		   , equality_columns
		   , equality_columns_ordered
		   , inequality_columns
		   , inequality_columns_ordered
		   , is_equality_col_order_stats_based
		   , is_inequality_col_order_stats_based
		   , key_columns
		   , included_columns
	FROM cteMissingIndex3
)
INSERT INTO #MissingIndexes
(
	database_id
	, object_id
	, obj_name
	, index_handle
	, keycol_group_id
	, eqcol_group_id
	, all_improvement_measure
	, db_improvement_measure
	, keycol_group_improvement_measure
	, eqcol_group_improvement_measure
	, index_improvement_measure
	, unique_compiles
	, user_seeks
	, user_scans
	, last_user_seek
	, last_user_scan
	, avg_total_user_cost
	, avg_user_impact
	, equality_columns_original
	, equality_columns_ordered
	, inequality_columns_original
	, inequality_columns_ordered
	, is_equality_col_order_stats_based
	, is_inequality_col_order_stats_based
	, equality_columns
	, inequality_columns
	, key_columns
	, included_columns
)
SELECT database_id
	   , object_id
	   , obj_name
	   , index_handle
	   , DENSE_RANK() OVER (ORDER BY keycol_group_improvement_measure DESC) AS keycol_group_id
	   , DENSE_RANK() OVER (ORDER BY eqcol_group_improvement_measure DESC) AS eqcol_group_id
	   , all_improvement_measure
	   , db_improvement_measure
	   , keycol_group_improvement_measure
	   , eqcol_group_improvement_measure
	   , index_improvement_measure
	   , unique_compiles
	   , user_seeks
	   , user_scans
	   , last_user_seek
	   , last_user_scan
	   , avg_total_user_cost
	   , avg_user_impact
	   , equality_columns
	   , equality_columns_ordered
	   , inequality_columns
	   , inequality_columns_ordered
	   , is_equality_col_order_stats_based
	   , is_inequality_col_order_stats_based
	   , equality_columns
	   , inequality_columns
	   , key_columns
	   , included_columns
FROM cteMissingIndex4;




/*
Step 4:  Results!
*/

-- This shows missing indexes in index groups (i.e. no consolidation, just the original missing index suggestions)
-- #region IndexGroup
SELECT 'Missing - Index Group' AS [Result Type]
	   , keycol_group_id AS KeyColGroup
	   , eqcol_group_id AS EqColGroup
	   --, all_improvement_measure
	   --, db_improvement_measure
	   --, keycol_group_improvement_measure
	   --, eqcol_group_improvement_measure
	   --, index_improvement_measure
	   --, key_columns
	   , (db_improvement_measure / all_improvement_measure) * 100.0 AS [DB % Grand Total]
	   , (keycol_group_improvement_measure / all_improvement_measure) * 100.0 AS [KeyColGroup % Grand Total]
	   , (keycol_group_improvement_measure / db_improvement_measure) * 100.0 AS [KeyColGroup % DB Total]
	   , (eqcol_group_improvement_measure / keycol_group_improvement_measure) * 100.0 AS [EqColGroup % KeyColGroup Total]
	   , (eqcol_group_improvement_measure / all_improvement_measure) * 100.0 AS [EqColGroup % Grand Total]
	   , (eqcol_group_improvement_measure / db_improvement_measure) * 100.0 AS [EqColGroup % DB Total]
	   , (index_improvement_measure / eqcol_group_improvement_measure) * 100.0 AS [Index % EqColGroup Total]
	   , (index_improvement_measure / keycol_group_improvement_measure) * 100.0 AS [Index % KeyColGroup Total]
	   , (index_improvement_measure / all_improvement_measure) * 100.0 AS [Index % Grand Total]
	   , (index_improvement_measure / db_improvement_measure) * 100.0 AS [Index % DB Total]
	   , obj_name
	   , SUM(unique_compiles) OVER (PARTITION BY keycol_group_id) AS keqcol_group_unique_compiles
	   , SUM(user_seeks) OVER (PARTITION BY keycol_group_id) AS keycol_group_user_seeks
	   , SUM(user_scans) OVER (PARTITION BY keycol_group_id) AS keycol_group_user_scans
	   , MAX(last_user_seek) OVER (PARTITION BY keycol_group_id) AS keycol_group_last_user_seek
	   , MAX(last_user_scan) OVER (PARTITION BY keycol_group_id) AS keycol_group_last_user_scan
	   --
	   , SUM(unique_compiles) OVER (PARTITION BY eqcol_group_id) AS eqcol_group_unique_compiles
	   , SUM(user_seeks) OVER (PARTITION BY eqcol_group_id) AS eqcol_group_user_seeks
	   , SUM(user_scans) OVER (PARTITION BY eqcol_group_id) AS eqcol_group_user_scans
	   , MAX(last_user_seek) OVER (PARTITION BY eqcol_group_id) AS eqcol_group_last_user_seek
	   , MAX(last_user_scan) OVER (PARTITION BY eqcol_group_id) AS eqcol_group_last_user_scan
	   , unique_compiles
	   , user_seeks
	   , user_scans
	   , last_user_seek
	   , last_user_scan
	   , avg_total_user_cost
	   , avg_user_impact
	   --, object_id
	   , index_handle
	   , equality_columns
	   , inequality_columns
	   , included_columns
	   , is_equality_col_order_stats_based
	   , is_inequality_col_order_stats_based
	   , obj_name + N' (' + CASE
								WHEN included_columns IS NULL THEN key_columns
								ELSE key_columns + N') INCLUDE (' + included_columns
							END + N')' AS create_index_statement
FROM #MissingIndexes
ORDER BY eqcol_group_id
		 , index_improvement_measure DESC;
-- #endregion






-- This shows missing indexes in equality column groups
-- An equality group is defined as same DB, same Object, same key columns, and same equality columns
-- equality and inequality columns are ordered by density, grouped by equality first and inequality second
-- INCLUDE columns are rolled up into a distinct set of columns for each equality group
-- #region EqualityColumnGroup
WITH
cteIncludedCols AS (
	SELECT mi.object_id
		   , mi.key_columns
		   , ISNULL(mi.equality_columns, N'') AS equality_columns
		   , cols.column_name
	FROM #MissingIndexes AS mi
		INNER JOIN #MissingIndexColumns AS cols ON mi.index_handle = cols.index_handle
	WHERE cols.column_usage = 'INCLUDE'
	GROUP BY mi.object_id
			 , mi.key_columns
			 , ISNULL(mi.equality_columns, N'')
			 , cols.column_name
)
,
cteDistinctIncludedCols AS (
	-- This gets the distinct, consolidated INCLUDE column list from missing index recommendations
	SELECT DISTINCT
		   object_id
		   , key_columns
		   , equality_columns
		   , (
				 SELECT QUOTENAME(child.column_name) + ', '
				 FROM cteIncludedCols AS child
				 WHERE child.key_columns = parent.key_columns
					   AND child.object_id = parent.object_id
				 ORDER BY child.column_name
				 FOR XML PATH('')
			 ) AS included_columns
	FROM cteIncludedCols AS parent
)
,
cteMissingIndex AS (
	SELECT mi.eqcol_group_id
		   , mi.keycol_group_id
		   , mi.all_improvement_measure
		   , mi.db_improvement_measure
		   , mi.keycol_group_improvement_measure
		   , mi.eqcol_group_improvement_measure
		   , mi.object_id
		   , mi.obj_name
		   , mi.key_columns
		   , mi.equality_columns
		   , mi.inequality_columns
		   , mi.is_equality_col_order_stats_based
		   , mi.is_inequality_col_order_stats_based
		   , LEFT(miIncludeCols.included_columns, LEN(miIncludeCols.included_columns) - 1) AS included_columns
		   , mi.obj_name + N' (' + CASE
									   WHEN miIncludeCols.included_columns IS NULL THEN mi.key_columns
									   ELSE mi.key_columns + N') INCLUDE (' + LEFT(miIncludeCols.included_columns, LEN(miIncludeCols.included_columns) - 1)
								   END + N')' AS create_index_statement
		   , SUM(mi.unique_compiles) AS eqcol_group_unique_compiles
		   , SUM(mi.user_seeks) AS eqcol_group_user_seeks
		   , SUM(mi.user_scans) AS eqcol_group_user_scans
		   , MAX(mi.last_user_seek) AS eqcol_group_last_user_seek
		   , MAX(mi.last_user_scan) AS eqcol_group_last_user_scan
		   , MAX(mi.avg_total_user_cost) AS eqcol_group_max_avg_total_user_cost
		   , MAX(mi.avg_user_impact) AS eqcol_group_max_avg_user_impact
	FROM #MissingIndexes AS mi
		LEFT OUTER JOIN cteDistinctIncludedCols AS miIncludeCols ON mi.object_id = miIncludeCols.object_id
																	AND mi.key_columns = miIncludeCols.key_columns
																	AND mi.equality_columns = miIncludeCols.equality_columns
	GROUP BY mi.eqcol_group_id
			 , mi.keycol_group_id
			 , mi.object_id
			 , mi.obj_name
			 , mi.key_columns
			 , mi.equality_columns
			 , mi.inequality_columns
			 , mi.all_improvement_measure
			 , mi.db_improvement_measure
			 , mi.keycol_group_improvement_measure
			 , mi.eqcol_group_improvement_measure
			 , miIncludeCols.included_columns
			 , mi.is_equality_col_order_stats_based
			 , mi.is_inequality_col_order_stats_based
)
SELECT 'Missing - Equality Group' AS [Result Type]
	   , keycol_group_id AS KeyColGroup
	   , eqcol_group_id AS EqColGroup
	   --, eqcol_group_improvement_measure
	   -- 
	   , (db_improvement_measure / all_improvement_measure) * 100.0 AS [DB % Grand Total]
	   --
	   , (keycol_group_improvement_measure / all_improvement_measure) * 100.0 AS [KeyColGroup % Grand Total]
	   , (keycol_group_improvement_measure / db_improvement_measure) * 100.0 AS [KeyColGroup % DB Total]
	   --
	   , (eqcol_group_improvement_measure / all_improvement_measure) * 100.0 AS [EqColGroup % Grand Total]
	   , (eqcol_group_improvement_measure / db_improvement_measure) * 100.0 AS [EqColGroup % DB Total]
	   --
	   , (eqcol_group_improvement_measure / keycol_group_improvement_measure) * 100.0 AS [EqColGroup % KeyColGroup Total]
	   , obj_name
	   --
	   , SUM(eqcol_group_unique_compiles) OVER (PARTITION BY keycol_group_id) AS keycol_group_unique_compiles
	   , SUM(eqcol_group_user_seeks) OVER (PARTITION BY keycol_group_id) AS keycol_group_user_seeks
	   , SUM(eqcol_group_user_scans) OVER (PARTITION BY keycol_group_id) AS keycol_group_user_scans
	   , MAX(eqcol_group_last_user_seek) OVER (PARTITION BY keycol_group_id) AS keycol_group_last_user_seek
	   , MAX(eqcol_group_last_user_scan) OVER (PARTITION BY keycol_group_id) AS keycol_group_last_user_scan
	   , MAX(eqcol_group_max_avg_total_user_cost) OVER (PARTITION BY keycol_group_id) AS keycol_group_max_avg_total_user_cost
	   , MAX(eqcol_group_max_avg_user_impact) OVER (PARTITION BY keycol_group_id) AS keycol_group_max_avg_user_impact
	   --
	   , eqcol_group_unique_compiles
	   , eqcol_group_user_seeks
	   , eqcol_group_user_scans
	   , eqcol_group_last_user_seek
	   , eqcol_group_last_user_scan
	   , eqcol_group_max_avg_total_user_cost
	   , eqcol_group_max_avg_user_impact
	   , key_columns
	   , equality_columns
	   , inequality_columns
	   , included_columns
	   , is_equality_col_order_stats_based
	   , is_inequality_col_order_stats_based
	   , create_index_statement
FROM cteMissingIndex
ORDER BY eqcol_group_id ASC;
-- #endregion





-- This shows missing indexes in key groups
-- A key group is defined as same DB, same Object, and same key columns
-- equality and inequality columns are ordered by density, grouped by equality first and inequality second
-- INCLUDE columns are rolled up into a distinct set of columns for each key group

-- #region KeyGolumnGroup
WITH
cteIncludedCols AS (
	SELECT mi.object_id
		   , mi.key_columns
		   , cols.column_name
	FROM #MissingIndexes AS mi
		INNER JOIN #MissingIndexColumns AS cols ON mi.index_handle = cols.index_handle
	WHERE cols.column_usage = 'INCLUDE'
	GROUP BY mi.object_id
			 , mi.key_columns
			 , cols.column_name
)
,
cteDistinctIncludedCols AS (
	-- This gets the distinct, consolidated INCLUDE column list from missing index recommendations
	SELECT DISTINCT
		   object_id
		   , key_columns
		   , (
				 SELECT QUOTENAME(child.column_name) + ', '
				 FROM cteIncludedCols AS child
				 WHERE child.key_columns = parent.key_columns
					   AND child.object_id = parent.object_id
				 ORDER BY child.column_name
				 FOR XML PATH('')
			 ) AS included_columns
	FROM cteIncludedCols AS parent
)
,
cteMissingIndex AS (
	SELECT mi.keycol_group_id
		   , mi.all_improvement_measure
		   , mi.db_improvement_measure
		   , mi.keycol_group_improvement_measure
		   , mi.object_id
		   , mi.obj_name
		   , mi.key_columns
		   , mi.is_equality_col_order_stats_based
		   , mi.is_inequality_col_order_stats_based
		   , LEFT(miIncludeCols.included_columns, LEN(miIncludeCols.included_columns) - 1) AS included_columns
		   , mi.obj_name + N' (' + CASE
									   WHEN miIncludeCols.included_columns IS NULL THEN mi.key_columns
									   ELSE mi.key_columns + N') INCLUDE (' + LEFT(miIncludeCols.included_columns, LEN(miIncludeCols.included_columns) - 1)
								   END + N')' AS create_index_statement
		   , SUM(mi.unique_compiles) AS keycol_group_unique_compiles
		   , SUM(mi.user_seeks) AS keycol_group_user_seeks
		   , SUM(mi.user_scans) AS keycol_group_user_scans
		   , MAX(mi.last_user_seek) AS keycol_group_last_user_seek
		   , MAX(mi.last_user_scan) AS keycol_group_last_user_scan
		   , MAX(mi.avg_total_user_cost) AS keycol_group_max_avg_total_user_cost
		   , MAX(mi.avg_user_impact) AS keycol_group_max_avg_user_impact
	FROM #MissingIndexes AS mi
		LEFT OUTER JOIN cteDistinctIncludedCols AS miIncludeCols ON mi.object_id = miIncludeCols.object_id
																	AND mi.key_columns = miIncludeCols.key_columns
	GROUP BY mi.keycol_group_id
			 , mi.object_id
			 , mi.obj_name
			 , mi.key_columns
			 , mi.all_improvement_measure
			 , mi.db_improvement_measure
			 , mi.keycol_group_improvement_measure
			 , miIncludeCols.included_columns
			 , mi.is_equality_col_order_stats_based
			 , mi.is_inequality_col_order_stats_based
)
SELECT 'Missing - Key Group' AS [Result Type]
	   , keycol_group_id AS KeyColGroup
	   --, DENSE_RANK() OVER (ORDER BY eqcol_group_improvement_measure DESC, key_columns ASC, equality_columns ASC) AS [EqColGroup]
	   --, eqcol_group_improvement_measure
	   -- 
	   , (db_improvement_measure / all_improvement_measure) * 100.0 AS [DB % Grand Total]
	   --
	   , (keycol_group_improvement_measure / all_improvement_measure) * 100.0 AS [KeyColGroup % Grand Total]
	   , (keycol_group_improvement_measure / db_improvement_measure) * 100.0 AS [KeyColGroup % DB Total]
	   --
	   , obj_name
	   , keycol_group_unique_compiles
	   , keycol_group_user_seeks
	   , keycol_group_user_scans
	   , keycol_group_last_user_seek
	   , keycol_group_last_user_scan
	   , keycol_group_max_avg_total_user_cost
	   , keycol_group_max_avg_user_impact
	   , key_columns
	   , included_columns
	   , is_equality_col_order_stats_based
	   , is_inequality_col_order_stats_based
	   , create_index_statement
FROM cteMissingIndex
ORDER BY keycol_group_id ASC;
-- #endregion




-- NOW....let's look at if missing indexes (using key groups) are overlapped/overlapping with each other and/or existing indexes

-- Populate #AllIndexes using Existing indexes first
INSERT INTO #AllIndexes
(
	SchemaId
	, ObjectId
	, IndexId
	, IndexColumnId
	, SchemaName
	, TableName
	, IndexName
	, ColumnName
	, key_ordinal
	, partition_ordinal
	, is_descending_key
	, is_included_column
	, has_filter
	, is_disabled
	, is_unique
	, filter_definition
	, MaxKeyOrdinal
	, MaxPartitionOrdinal
	, TotalColumnCount
	, IsMissingIndex
)
SELECT s.schema_id AS SchemaId
	   , o.object_id AS ObjectId
	   , i.index_id AS IndexId
	   , ic.index_column_id AS IndexColumnId
	   , s.name AS SchemaName
	   , o.name AS TableName
	   , i.name AS IndexName
	   , c.name AS ColumnName
	   , ic.key_ordinal
	   , ic.partition_ordinal
	   , ic.is_descending_key
	   , ic.is_included_column
	   , i.has_filter
	   , i.is_disabled
	   , i.is_unique
	   , i.filter_definition
	   , MAX(ic.key_ordinal) OVER (PARTITION BY s.schema_id, o.object_id, i.index_id) AS MaxKeyOrdinal
	   , MAX(ic.partition_ordinal) OVER (PARTITION BY s.schema_id, o.object_id, i.index_id) AS MaxPartitionOrdinal
	   , COUNT(ic.index_column_id) OVER (PARTITION BY s.schema_id, o.object_id, i.index_id) AS TotalColumnCount
	   , 0 AS IsMissingIndex
FROM sys.schemas AS s
	INNER JOIN sys.objects AS o ON s.schema_id = o.schema_id
	INNER JOIN sys.indexes AS i ON o.object_id = i.object_id
	INNER JOIN sys.index_columns AS ic ON o.object_id = ic.object_id
										  AND i.index_id = ic.index_id
	INNER JOIN sys.columns AS c ON o.object_id = c.object_id
								   AND ic.column_id = c.column_id
WHERE INDEXPROPERTY(o.object_id, i.name, 'IsStatistics') = 0
	  AND i.is_hypothetical = 0
	  AND i.type_desc <> 'heap';




-- Populate #AllIndexes using Missing Index Key Groups so we can apply the overlapping/overlapped logic
WITH
cteDistinctIncludedCols AS (
	SELECT mi.object_id
		   , mi.key_columns
		   , cols.column_name
	FROM #MissingIndexes AS mi
		INNER JOIN #MissingIndexColumns AS cols ON mi.index_handle = cols.index_handle
	WHERE cols.column_usage = 'INCLUDE'
	GROUP BY mi.object_id
			 , mi.key_columns
			 , cols.column_name
)
,
cteActualIndex AS (
	SELECT SchemaId
		   , ObjectId
		   , MAX(IndexId) AS MaxIndexId
	FROM #AllIndexes
	GROUP BY SchemaId
			 , ObjectId
)
,
cteMissingIndex AS (
	-- Need to do this to get an artificial IndexId
	SELECT s.schema_id AS SchemaId
		   , o.object_id AS ObjectId
		   , s.name AS SchemaName
		   , o.name AS TableName
		   , DENSE_RANK() OVER (PARTITION BY o.object_id ORDER BY mi.keycol_group_id ASC) + ISNULL(cteActualIndex.MaxIndexId, 0) AS IndexId
		   , mi.keycol_group_id AS KeyColGroup
		   , mi.index_handle
		   , mi.key_columns
	FROM sys.schemas AS s
		INNER JOIN sys.objects AS o ON s.schema_id = o.schema_id
		INNER JOIN #MissingIndexes AS mi ON o.object_id = mi.object_id
		LEFT OUTER JOIN cteActualIndex ON s.schema_id = cteActualIndex.SchemaId
										  AND o.object_id = cteActualIndex.ObjectId
)
,
cteMissingIndexCols AS (
	SELECT DISTINCT
		   mi.SchemaId
		   , mi.ObjectId
		   , mi.IndexId
		   , mi.KeyColGroup
		   , mi.SchemaName
		   , mi.TableName
		   , mic.column_name AS ColumnName
		   , mic.column_ordinal AS key_ordinal
		   , 0 AS is_included_column
	FROM cteMissingIndex AS mi
		INNER JOIN #MissingIndexColumns AS mic ON mi.index_handle = mic.index_handle
	WHERE mic.column_usage IN ( 'EQUALITY', 'INEQUALITY' )
	UNION ALL
	SELECT DISTINCT
		   mi.SchemaId
		   , mi.ObjectId
		   , mi.IndexId
		   , mi.KeyColGroup
		   , mi.SchemaName
		   , mi.TableName
		   , mic.column_name AS ColumnName
		   , 0 AS key_ordinal
		   , 1 AS is_included_column
	FROM cteMissingIndex AS mi
		INNER JOIN cteDistinctIncludedCols AS mic ON mi.ObjectId = mic.object_id
													 AND mi.key_columns = mic.key_columns
)
INSERT INTO #AllIndexes
(
	SchemaId
	, ObjectId
	, IndexId
	, IndexColumnId
	, SchemaName
	, TableName
	, IndexName
	, ColumnName
	, key_ordinal
	, partition_ordinal
	, is_descending_key
	, is_included_column
	, has_filter
	, is_disabled
	, is_unique
	, filter_definition
	, MaxKeyOrdinal
	, MaxPartitionOrdinal
	, TotalColumnCount
	, IsMissingIndex
)
SELECT SchemaId
	   , ObjectId
	   , IndexId
	   , ROW_NUMBER() OVER (PARTITION BY SchemaId
										 , ObjectId
										 , IndexId
							ORDER BY is_included_column
									 , key_ordinal
									 , ColumnName
					  ) AS IndexColumnId
	   , SchemaName
	   , TableName
	   , N'IX_' + TableName + N'_KeyColGroup_' + LEFT(N'00' + CONVERT(NVARCHAR(3), KeyColGroup), 3) + N'_Missing_' + LEFT(N'00' + CONVERT(NVARCHAR(3), IndexId), 3) AS IndexName
	   , ColumnName
	   , key_ordinal
	   , 1 AS partition_ordinal
	   , 0 AS is_descending_key
	   , is_included_column
	   , 0 AS has_filter
	   , 0 AS is_disabled
	   , 0 AS is_unique
	   , NULL AS filter_definition
	   , MAX(key_ordinal) OVER (PARTITION BY SchemaId, ObjectId, IndexId) AS MaxKeyOrdinal
	   , 0 AS MaxPartitionOrdinal
	   , COUNT(*) OVER (PARTITION BY SchemaId, ObjectId, IndexId) AS TotalColumnCount
	   , 1 AS IsMissingIndex
FROM cteMissingIndexCols
ORDER BY SchemaName
		 , TableName
		 , IndexId;



-- Now apply the overlapping indexes checks
-- #region OverlappingIndexes
CREATE STATISTICS IX_#AllIndexes_1
ON #AllIndexes (
	IndexColumnId
)
WITH FULLSCAN;

WITH
cteIndexes AS (
	SELECT SchemaId
		   , ObjectId
		   , IndexId
		   , SchemaName
		   , TableName
		   , IndexName
		   , is_disabled
		   , is_unique
		   , IsMissingIndex
		   , MaxPartitionOrdinal
	FROM #AllIndexes
	GROUP BY SchemaId
			 , ObjectId
			 , IndexId
			 , SchemaName
			 , SchemaName
			 , TableName
			 , IndexName
			 , is_disabled
			 , is_unique
			 , IsMissingIndex
			 , MaxPartitionOrdinal
)
,
ctePartitionStatsSummary AS (
	SELECT object_id AS ObjectId
		   , index_id AS IndexId
		   , SUM(row_count) AS [RowCount]
		   , SUM(used_page_count) AS UsedPageCount
		   , SUM(reserved_page_count) AS ReservedPageCount
	FROM sys.dm_db_partition_stats
	GROUP BY object_id
			 , index_id
	UNION ALL
	SELECT ObjectId
		   , IndexId
		   , 0 AS [RowCount]
		   , 0 AS UsedPageCount
		   , 0 AS ReservedPageCount
	FROM #AllIndexes
	WHERE IsMissingIndex = 1
	GROUP BY ObjectId
			 , IndexId
)
/* 
		Candidates for overlapping/overlapped are on the same schema and table, not the same index, and the last key column of the indexes are the same 
		Exclude CLUSTERED indexes as overlapped candidates
		*/
,
cteOverlapIndexCandidate AS (
	SELECT DISTINCT
		   iLeft.SchemaId AS OverlappingSchemaId
		   , iLeft.ObjectId AS OverlappingObjectId
		   , iLeft.IndexId AS OverlappingIndexId
		   , iRight.SchemaId AS OverlappedSchemaId
		   , iRight.ObjectId AS OverlappedObjectId
		   , iRight.IndexId AS OverlappedIndexId
		   , iLeft.key_ordinal AS iLeftKeyOrdinal
		   , iRight.key_ordinal AS iRightKeyOrdinal
		   , iLeft.MaxKeyOrdinal AS iLeftMaxKeyOrdinal
		   , iLeft.TotalColumnCount AS iLeftTotalColumnCount
		   , iRight.MaxKeyOrdinal AS iRightMaxKeyOrdinal
		   , iRight.TotalColumnCount AS iRightTotalColumnCount
		   , iLeft.partition_ordinal AS iLeftPartition_ordinal
		   , iRight.partition_ordinal AS iRightPartition_ordinal
	FROM #AllIndexes AS iLeft
		INNER JOIN #AllIndexes AS iRight ON iLeft.SchemaId = iRight.SchemaId
											AND iLeft.ObjectId = iRight.ObjectId
	WHERE iLeft.IndexId <> iRight.IndexId
		  AND iLeft.key_ordinal = iRight.key_ordinal
		  AND iLeft.partition_ordinal = iRight.partition_ordinal
		  AND iLeft.ColumnName = iRight.ColumnName
		  AND iLeft.is_descending_key = iRight.is_descending_key
		  AND iLeft.is_included_column = 0
		  AND iRight.is_included_column = 0
		  AND (
			  (
				  iLeft.IndexId = 1
				  AND iLeft.key_ordinal = iRight.MaxKeyOrdinal
			  )
			  OR (
				  iRight.IndexId <> 1
				  AND iLeft.key_ordinal = iRight.MaxKeyOrdinal
				  AND (
					  iLeft.MaxKeyOrdinal > iRight.MaxKeyOrdinal
					  OR (
						  iLeft.MaxKeyOrdinal = iRight.MaxKeyOrdinal
						  AND (
							  (iLeft.TotalColumnCount > iRight.TotalColumnCount)
							  OR (
								  iLeft.TotalColumnCount = iRight.TotalColumnCount
								  AND iLeft.IndexId < iRight.IndexId
							  )
						  )
					  )
				  )
			  )
		  )
)
,
cteOverlappingIndexes AS (
	SELECT DISTINCT
		   iOverlapping.SchemaId
		   , iOverlapping.ObjectId
		   , iOverlapping.IndexId AS OverlappingIndexId
		   , iOverlapped.IndexId AS OverlappedIndexId
		   , iOverlapped.MaxKeyOrdinal AS OverlappedMaxKeyOrdinal
		   , COUNT(iOverlapping.key_ordinal) OVER (PARTITION BY iOverlapping.SchemaId
																, iOverlapping.ObjectId
																, iOverlapping.IndexId
																, iOverlapped.IndexId
											 ) AS OverlappingKeyOrdinalCount
	FROM #AllIndexes AS iOverlapping
		INNER JOIN cteOverlapIndexCandidate ON iOverlapping.SchemaId = cteOverlapIndexCandidate.OverlappingSchemaId
											   AND iOverlapping.ObjectId = cteOverlapIndexCandidate.OverlappingObjectId
											   AND iOverlapping.IndexId = cteOverlapIndexCandidate.OverlappingIndexId
		INNER JOIN #AllIndexes AS iOverlapped ON cteOverlapIndexCandidate.OverlappedSchemaId = iOverlapped.SchemaId
												 AND cteOverlapIndexCandidate.OverlappedObjectId = iOverlapped.ObjectId
												 AND cteOverlapIndexCandidate.OverlappedIndexId = iOverlapped.IndexId
	WHERE iOverlapping.key_ordinal = iOverlapped.key_ordinal
		  AND iOverlapping.partition_ordinal = iOverlapped.partition_ordinal
		  AND iOverlapping.ColumnName = iOverlapped.ColumnName
		  AND iOverlapping.is_descending_key = iOverlapped.is_descending_key
		  AND iOverlapping.has_filter = iOverlapped.has_filter
		  AND iOverlapped.is_included_column = 0
		  AND iOverlapping.is_included_column = 0
		  AND (
			  iOverlapping.has_filter = 0
			  OR (
				  iOverlapping.has_filter = 1
				  AND iOverlapping.filter_definition = iOverlapped.filter_definition
			  )
		  )
)
SELECT 'Overlapping Indexes' AS [Result Type]
	   , DB_NAME() AS DatabaseName
	   , iOverlapping.SchemaName
	   , iOverlapping.TableName
	   , iOverlapping.IndexName AS OverlappingIndex
	   , iOverlapped.IndexName AS OverlappedIndex
	   , OverlappingIndexColumns.ColumnNames AS OverlappingKeyColumns
	   , OverlappedIndexColumns.ColumnNames AS OverlappedKeyColumns
	   , LEFT(OverlappingIncludeColumns.ColumnNames, LEN(OverlappingIncludeColumns.ColumnNames) - 1) AS OverlappingIncludeColumns
	   , LEFT(OverlappedIncludeColumns.ColumnNames, LEN(OverlappedIncludeColumns.ColumnNames) - 1) AS OverlappedIncludeColumns
	   , CASE
			 WHEN (OverlappingIndexColumns.ColumnNames = OverlappedIndexColumns.ColumnNames) THEN '*'
			 ELSE ''
		 END AS ExactKeyColumnMatch
	   , CASE
			 WHEN (COALESCE(OverlappingIncludeColumns.ColumnNames, '') = COALESCE(OverlappedIncludeColumns.ColumnNames, '')) THEN '*'
			 ELSE ''
		 END AS ExactIncludeColumnMatch
	   , iOverlapping.IsMissingIndex AS OverlappingIndexIsMissing
	   , iOverlapped.IsMissingIndex AS OverlappedIndexIsMissing
	   , iOverlapping.is_disabled AS OverlappingIndexIsDisabled
	   , iOverlapped.is_disabled AS OverlappedIndexIsDisabled
	   , iOverlapping.is_unique AS OverlappingIndexIsUnique
	   , iOverlapped.is_unique AS OverlappedIndexIsUnique
	   , CASE
			 WHEN (iOverlapping.MaxPartitionOrdinal > 0) THEN 1
			 ELSE 0
		 END AS OverlappingIndexIsPartitioned
	   , CASE
			 WHEN (iOverlapped.MaxPartitionOrdinal > 0) THEN 1
			 ELSE 0
		 END AS OverlappedIndexIsPartitioned
	   , OverlappingPartitionStatsSummary.[RowCount] AS OverlappingRowCount
	   , OverlappedPartitionStatsSummary.[RowCount] AS OverlappedRowCount
	   , ((OverlappingPartitionStatsSummary.ReservedPageCount * 8) / POWER(1024.0, 2)) AS OverlappingReservedPageCountGB
	   , ((OverlappedPartitionStatsSummary.ReservedPageCount * 8) / POWER(1024.0, 2)) AS OverlappedReservedPageCountGB
	   , ((OverlappingPartitionStatsSummary.UsedPageCount * 8) / POWER(1024.0, 2)) AS OverlappingUsedPageCountGB
	   , ((OverlappedPartitionStatsSummary.UsedPageCount * 8) / POWER(1024.0, 2)) AS OverlappedUsedPageCountGB
	   , OverlappingIndexUsageStats.user_lookups AS OverlappingUserLookups
	   , OverlappedIndexUsageStats.user_lookups AS OverlappedUserLookups
	   , OverlappingIndexUsageStats.user_scans AS OverlappingUserScans
	   , OverlappedIndexUsageStats.user_scans AS OverlappedUserScans
	   , OverlappingIndexUsageStats.user_seeks AS OverlappingUserSeeks
	   , OverlappedIndexUsageStats.user_seeks AS OverlappedUserSeeks
	   , OverlappingIndexUsageStats.user_updates AS OverlappingUserUpdates
	   , OverlappedIndexUsageStats.user_updates AS OverlappedUserUpdates
	   , OverlappedIndexUsageStats.last_user_lookup AS OverlappedLastUserLookup
	   , OverlappedIndexUsageStats.last_user_scan AS OverlappedLastUserScan
	   , OverlappedIndexUsageStats.last_user_seek AS OverlappedLastUserSeek
	   , OverlappedIndexUsageStats.last_user_update AS OverlappedLastUserUpdate
	   , 'ALTER INDEX ' + QUOTENAME(iOverlapped.IndexName) + ' ON ' + QUOTENAME(iOverlapping.SchemaName) + '.' + QUOTENAME(iOverlapping.TableName) + ' DISABLE;' AS OverlappedDisableScript
FROM cteIndexes AS iOverlapping
	INNER JOIN ctePartitionStatsSummary AS OverlappingPartitionStatsSummary ON iOverlapping.ObjectId = OverlappingPartitionStatsSummary.ObjectId
																			   AND iOverlapping.IndexId = OverlappingPartitionStatsSummary.IndexId
	LEFT OUTER JOIN sys.dm_db_index_usage_stats AS OverlappingIndexUsageStats ON iOverlapping.ObjectId = OverlappingIndexUsageStats.object_id
																				 AND iOverlapping.IndexId = OverlappingIndexUsageStats.index_id
																				 AND OverlappingIndexUsageStats.database_id = DB_ID()
	INNER JOIN cteOverlappingIndexes ON iOverlapping.SchemaId = cteOverlappingIndexes.SchemaId
										AND iOverlapping.ObjectId = cteOverlappingIndexes.ObjectId
										AND iOverlapping.IndexId = cteOverlappingIndexes.OverlappingIndexId
	INNER JOIN cteIndexes AS iOverlapped ON cteOverlappingIndexes.SchemaId = iOverlapped.SchemaId
											AND cteOverlappingIndexes.ObjectId = iOverlapped.ObjectId
											AND cteOverlappingIndexes.OverlappedIndexId = iOverlapped.IndexId
	INNER JOIN ctePartitionStatsSummary AS OverlappedPartitionStatsSummary ON iOverlapped.ObjectId = OverlappedPartitionStatsSummary.ObjectId
																			  AND iOverlapped.IndexId = OverlappedPartitionStatsSummary.IndexId
	LEFT OUTER JOIN sys.dm_db_index_usage_stats AS OverlappedIndexUsageStats ON iOverlapped.ObjectId = OverlappedIndexUsageStats.object_id
																				AND iOverlapped.IndexId = OverlappedIndexUsageStats.index_id
																				AND OverlappedIndexUsageStats.database_id = DB_ID()
	CROSS APPLY (
	SELECT CASE
			   WHEN (
				   key_ordinal = 1
				   AND MaxPartitionOrdinal = 0
			   ) THEN N''
			   WHEN (
				   key_ordinal = 0
				   AND partition_ordinal >= 1
			   ) THEN N'*'
			   ELSE N', '
		   END + ColumnName + ' ' + CASE
										WHEN is_descending_key = 0 THEN N'ASC'
										ELSE N'DESC'
									END
	FROM #AllIndexes
	WHERE SchemaId = iOverlapping.SchemaId
		  AND ObjectId = iOverlapping.ObjectId
		  AND IndexId = iOverlapping.IndexId
		  AND is_included_column = 0
	ORDER BY key_ordinal
	FOR XML PATH('')
) AS OverlappingIndexColumns(ColumnNames)
	CROSS APPLY (
	SELECT ColumnName + N', '
	FROM #AllIndexes
	WHERE SchemaId = iOverlapping.SchemaId
		  AND ObjectId = iOverlapping.ObjectId
		  AND IndexId = iOverlapping.IndexId
		  AND is_included_column = 1
	ORDER BY ColumnName
	FOR XML PATH('')
) AS OverlappingIncludeColumns(ColumnNames)
	CROSS APPLY (
	SELECT CASE
			   WHEN (
				   key_ordinal = 1
				   AND MaxPartitionOrdinal = 0
			   ) THEN N''
			   WHEN (
				   key_ordinal = 0
				   AND partition_ordinal >= 1
			   ) THEN N'*'
			   ELSE N', '
		   END + ColumnName + N' ' + CASE
										 WHEN is_descending_key = 0 THEN N'ASC'
										 ELSE N'DESC'
									 END
	FROM #AllIndexes
	WHERE SchemaId = iOverlapped.SchemaId
		  AND ObjectId = iOverlapped.ObjectId
		  AND IndexId = iOverlapped.IndexId
		  AND is_included_column = 0
	ORDER BY key_ordinal
	FOR XML PATH('')
) AS OverlappedIndexColumns(ColumnNames)
	CROSS APPLY (
	SELECT ColumnName + N', '
	FROM #AllIndexes
	WHERE SchemaId = iOverlapped.SchemaId
		  AND ObjectId = iOverlapped.ObjectId
		  AND IndexId = iOverlapped.IndexId
		  AND is_included_column = 1
	ORDER BY ColumnName
	FOR XML PATH('')
) AS OverlappedIncludeColumns(ColumnNames)
WHERE cteOverlappingIndexes.OverlappingKeyOrdinalCount = cteOverlappingIndexes.OverlappedMaxKeyOrdinal
ORDER BY iOverlapping.SchemaName
		 , iOverlapping.TableName
		 , iOverlapping.IndexName
		 , iOverlapped.IndexName;



/* Cleanup temporary tables */
DROP TABLE #DbccShowStatistics;
DROP TABLE #StatsDensity;
DROP TABLE #MissingIndexColumns;
DROP TABLE #MissingIndexes;
DROP TABLE #AllIndexes;