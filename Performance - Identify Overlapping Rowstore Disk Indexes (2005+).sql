/*********************************************************************************************
Identify Overlapping Rowstore Disk Indexes v2.00 (2017-04-30)
(C) 2017, Kendal Van Dyke

Feedback: mailto:kendal.vandyke@gmail.com

License: 
	This query is free to download and use for personal, educational, and internal 
	corporate purposes, provided that this header is preserved. Redistribution or sale 
	of this query, in whole or in part, is prohibited without the author's express 
	written consent.
	
Note: 

*********************************************************************************************/

CREATE TABLE #Indexes (
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


INSERT INTO #Indexes
SELECT s.schema_id AS SchemaId
	   , t.object_id AS ObjectId
	   , i.index_id AS IndexId
	   , ic.index_column_id AS IndexColumnId
	   , s.name AS SchemaName
	   , t.name AS TableName
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
	   , MAX(ic.key_ordinal) OVER (PARTITION BY s.schema_id, t.object_id, i.index_id) AS MaxKeyOrdinal
	   , MAX(ic.partition_ordinal) OVER (PARTITION BY s.schema_id, t.object_id, i.index_id) AS MaxPartitionOrdinal
	   , COUNT(ic.index_column_id) OVER (PARTITION BY s.schema_id, t.object_id, i.index_id) AS TotalColumnCount
FROM sys.schemas AS s
	INNER JOIN sys.tables AS t ON s.schema_id = t.schema_id
	INNER JOIN sys.indexes AS i ON t.object_id = i.object_id
	INNER JOIN sys.index_columns AS ic ON t.object_id = ic.object_id
										  AND i.index_id = ic.index_id
	INNER JOIN sys.columns AS c ON t.object_id = c.object_id
								   AND ic.column_id = c.column_id
WHERE INDEXPROPERTY(t.object_id, i.name, 'IsStatistics') = 0
	  AND i.is_hypothetical = 0
	  AND i.type_desc <> 'heap'

/* -- For troubleshooting
		AND s.schema_id = 1
		AND t.object_id = 1455344249
		AND i.index_id IN ( 1, 2 )*/
;

CREATE STATISTICS IX_#Indexes_1 ON #Indexes (IndexColumnId) WITH FULLSCAN;

WITH cteIndexes AS (
	SELECT SchemaId
		   , ObjectId
		   , IndexId
		   , SchemaName
		   , TableName
		   , IndexName
		   , is_disabled
		   , is_unique
		   , MaxPartitionOrdinal
	FROM #Indexes
	GROUP BY SchemaId
			 , ObjectId
			 , IndexId
			 , SchemaName
			 , SchemaName
			 , TableName
			 , IndexName
			 , is_disabled
			 , is_unique
			 , MaxPartitionOrdinal
)
	 , ctePartitionStatsSummary AS (
	SELECT object_id AS ObjectId
		   , index_id AS IndexId
		   , SUM(row_count) AS [RowCount]
		   , SUM(used_page_count) AS UsedPageCount
		   , SUM(reserved_page_count) AS ReservedPageCount
	FROM sys.dm_db_partition_stats
	GROUP BY object_id
			 , index_id
)
	 /* 
		Candidates for overlapping/overlapped are on the same schema and table, not the same index, and the last key column of the indexes are the same 
		Exclude CLUSTERED indexes as overlapped candidates
		*/
	 , cteOverlapIndexCandidate AS (
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
	FROM #Indexes AS iLeft
		INNER JOIN #Indexes AS iRight ON iLeft.SchemaId = iRight.SchemaId
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
	 , cteOverlappingIndexes AS (
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
	FROM #Indexes AS iOverlapping
		INNER JOIN cteOverlapIndexCandidate ON iOverlapping.SchemaId = cteOverlapIndexCandidate.OverlappingSchemaId
											   AND iOverlapping.ObjectId = cteOverlapIndexCandidate.OverlappingObjectId
											   AND iOverlapping.IndexId = cteOverlapIndexCandidate.OverlappingIndexId
		INNER JOIN #Indexes AS iOverlapped ON cteOverlapIndexCandidate.OverlappedSchemaId = iOverlapped.SchemaId
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
SELECT DB_NAME() AS DatabaseName
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
			 WHEN (COALESCE(OverlappingIncludeColumns.ColumnNames, '') = COALESCE(
																		 OverlappedIncludeColumns.ColumnNames, ''
																		 )
			 ) THEN '*'
			 ELSE ''
		 END AS ExactIncludeColumnMatch
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
	   , 'ALTER INDEX ' + QUOTENAME(iOverlapped.IndexName) + ' ON ' + QUOTENAME(iOverlapping.SchemaName) + '.'
		 + QUOTENAME(iOverlapping.TableName) + ' DISABLE;' AS OverlappedDisableScript
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
	FROM #Indexes
	WHERE SchemaId = iOverlapping.SchemaId
		  AND ObjectId = iOverlapping.ObjectId
		  AND IndexId = iOverlapping.IndexId
		  AND is_included_column = 0
	ORDER BY key_ordinal
	FOR XML PATH('')
) AS OverlappingIndexColumns(ColumnNames)
	CROSS APPLY (
	SELECT ColumnName + N', '
	FROM #Indexes
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
	FROM #Indexes
	WHERE SchemaId = iOverlapped.SchemaId
		  AND ObjectId = iOverlapped.ObjectId
		  AND IndexId = iOverlapped.IndexId
		  AND is_included_column = 0
	ORDER BY key_ordinal
	FOR XML PATH('')
) AS OverlappedIndexColumns(ColumnNames)
	CROSS APPLY (
	SELECT ColumnName + N', '
	FROM #Indexes
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


DROP TABLE #Indexes;