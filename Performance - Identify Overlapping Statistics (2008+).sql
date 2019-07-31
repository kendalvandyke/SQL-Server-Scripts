/*********************************************************************************************
Identify Overlapping Statistics v1.00 (2015-10-15)
(C) 2015, Kendal Van Dyke

Feedback: mailto:kendal.vandyke@gmail.com

License: 
	This query is free to download and use for personal, educational, and internal 
	corporate purposes, provided that this header is preserved. Redistribution or sale 
	of this query, in whole or in part, is prohibited without the author's express 
	written consent.
	
Note: 

*********************************************************************************************/

/* For SQL 2008 and up - takes filtered indexes into consideration */
WITH cteAutostats (object_id, stats_id, name, has_filter, filter_definition, column_id) AS (
	SELECT ColumnStats.object_id
		   , ColumnStats.stats_id
		   , ColumnStats.name
		   , ColumnStats.has_filter
		   , ColumnStats.filter_definition
		   , StatsColumns.column_id
	FROM sys.stats AS ColumnStats
		INNER JOIN sys.stats_columns AS StatsColumns ON ColumnStats.object_id = StatsColumns.object_id
														AND ColumnStats.stats_id = StatsColumns.stats_id
	WHERE ColumnStats.auto_created = 1
		  AND StatsColumns.stats_column_id = 1
)
SELECT OBJECT_SCHEMA_NAME(ColumnStats.object_id) AS SchemaName
	   , OBJECT_NAME(ColumnStats.object_id) AS TableName
	   , ObjectColumns.name AS ColumnName
	   , ColumnStats.name AS Overlapped
	   , cteAutostats.name AS Overlapping
	   , 'DROP STATISTICS ' + QUOTENAME(OBJECT_SCHEMA_NAME(ColumnStats.object_id)) + '.'
		 + QUOTENAME(OBJECT_NAME(ColumnStats.object_id)) + '.' + QUOTENAME(cteAutostats.name) + ';' AS DropStatement
FROM sys.stats AS ColumnStats
	INNER JOIN sys.stats_columns AS StatsColumns ON ColumnStats.object_id = StatsColumns.object_id
													AND ColumnStats.stats_id = StatsColumns.stats_id
	INNER JOIN cteAutostats ON StatsColumns.object_id = cteAutostats.object_id
							   AND StatsColumns.column_id = cteAutostats.column_id
	INNER JOIN sys.columns AS ObjectColumns ON ColumnStats.object_id = ObjectColumns.object_id
											   AND StatsColumns.column_id = ObjectColumns.column_id
WHERE ColumnStats.auto_created = 0
	  AND StatsColumns.stats_column_id = 1
	  AND StatsColumns.stats_id != cteAutostats.stats_id
	  AND (
		  (
			  cteAutostats.has_filter = 1
			  AND ColumnStats.has_filter = 1
			  AND cteAutostats.filter_definition = ColumnStats.filter_definition
		  )
		  OR (
			  cteAutostats.has_filter = 0
			  AND ColumnStats.has_filter = 0
		  )
	  )
	  AND OBJECTPROPERTY(ColumnStats.object_id, 'IsMsShipped') = 0
ORDER BY OBJECT_SCHEMA_NAME(ColumnStats.object_id)
		 , OBJECT_NAME(ColumnStats.object_id)
		 , ObjectColumns.name;
GO