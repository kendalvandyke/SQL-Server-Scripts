-- Limited
-- Change "LIMITED" to "SAMPLED" to get ghost record counts
SELECT @@SERVERNAME AS [Server Name]
	 , DB_NAME() AS [Database Name]
	 , OBJECT_SCHEMA_NAME(i.OBJECT_ID) AS [Schema Name]
	 , OBJECT_NAME(i.OBJECT_ID) AS [Table Name]
	 , i.name AS [Index Name]
	 , ps.partition_number AS [Partition Number]
	 , i.is_primary_key AS [Is Primary Key]
	 , i.is_unique AS [Is Unique]
	 , ips.index_type_desc AS [Type]
	 , ips.alloc_unit_type_desc AS [Allocation Unit Type]
	 , p.data_compression_desc AS [Compression Type]
	 , ps.row_count AS [Row Count]
	 , ips.record_count AS [Record Count]
	 , ips.ghost_record_count AS [Ghost Record Count]
	 , ips.avg_fragmentation_in_percent AS [Avg Fragmentation Pct]
	 , ips.avg_page_space_used_in_percent AS [Avg Page Space Used Pct]
	 , ps.used_page_count AS [Used Page Count]
	 , (ps.used_page_count * 8) / POWER(1024.0, 1) AS [Used Page Count (MB)]
	 , (ps.used_page_count * 8) / POWER(1024.0, 2) AS [Used Page Count (GB)]
	 , ps.reserved_page_count AS [Reserved Page Count]
	 , (ps.reserved_page_count * 8) / POWER(1024.0, 1) AS [Reserved Page Count (MB)]
	 , (ps.reserved_page_count * 8) / POWER(1024.0, 2) AS [Reserved Page Count (GB)]
	 , ius.user_seeks AS [User Seeks]
	 , ius.user_scans AS [User Scans]
	 , ius.user_lookups AS [User Lookups]
	 , ius.user_updates AS [User Updates]
	 , ius.system_seeks AS [System Seeks]
	 , ius.system_scans AS [System Scans]
	 , ius.system_lookups AS [System Lookups]
	 , ius.system_updates AS [System Updates]
	 , CASE
		   WHEN (ius.user_seeks + ius.user_scans + ius.user_lookups + ius.user_updates) > 0 THEN
	 ( 1.0
	   - (CAST(ius.user_updates AS FLOAT)
		  / CAST((ius.user_seeks + ius.user_scans + ius.user_lookups + ius.user_updates) AS FLOAT)
		 )
	 )
		   ELSE 0
	   END AS [% Reads]
FROM sys.indexes AS [i]
	INNER JOIN sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') AS [ips] ON i.OBJECT_ID = ips.OBJECT_ID
																								AND i.index_id = ips.index_id
	INNER JOIN sys.dm_db_partition_stats AS [ps] ON i.OBJECT_ID = ps.OBJECT_ID
													AND i.index_id = ps.index_id
													AND ips.partition_number = ps.partition_number
	INNER JOIN sys.partitions AS p ON i.index_id = p.index_id
									  AND i.object_id = p.object_id
									  AND ps.partition_id = p.partition_id
	LEFT OUTER JOIN sys.dm_db_index_usage_stats AS [ius] ON i.object_id = ius.object_id
															AND i.index_id = ius.index_id
															AND ius.database_id = DB_ID()
ORDER BY 1
	   , 2
	   , 3
	   , 4
	   , 5;



-- No fragmentation details
SELECT @@SERVERNAME AS [Server Name]
	 , DB_NAME() AS [Database Name]
	 , OBJECT_SCHEMA_NAME(i.OBJECT_ID) AS [Schema Name]
	 , OBJECT_NAME(i.OBJECT_ID) AS [Table Name]
	 , i.name AS [Index Name]
	 , ps.partition_number AS [Partition Number]
	 , i.is_primary_key AS [Is Primary Key]
	 , i.is_unique AS [Is Unique]
	 , i.type_desc AS [Type]
	 , p.data_compression_desc AS [Compression Type]
	 , ps.row_count AS [Row Count]
	 , ps.used_page_count AS [Used Page Count]
	 , (ps.used_page_count * 8) / POWER(1024.0, 1) AS [Used Page Count (MB)]
	 , (ps.used_page_count * 8) / POWER(1024.0, 2) AS [Used Page Count (GB)]
	 , ps.reserved_page_count AS [Reserved Page Count]
	 , (ps.reserved_page_count * 8) / POWER(1024.0, 1) AS [Reserved Page Count (MB)]
	 , (ps.reserved_page_count * 8) / POWER(1024.0, 2) AS [Reserved Page Count (GB)]
	 , ius.user_seeks AS [User Seeks]
	 , ius.user_scans AS [User Scans]
	 , ius.user_lookups AS [User Lookups]
	 , ius.user_updates AS [User Updates]
	 , ius.system_seeks AS [System Seeks]
	 , ius.system_scans AS [System Scans]
	 , ius.system_lookups AS [System Lookups]
	 , ius.system_updates AS [System Updates]
	 , CASE
		   WHEN (ius.user_seeks + ius.user_scans + ius.user_lookups + ius.user_updates) > 0 THEN
	 ( 1.0
	   - (CAST(ius.user_updates AS FLOAT)
		  / CAST((ius.user_seeks + ius.user_scans + ius.user_lookups + ius.user_updates) AS FLOAT)
		 )
	 )
		   ELSE 0
	   END AS [% Reads]
FROM sys.indexes AS [i]
	INNER JOIN sys.dm_db_partition_stats AS [ps] ON i.OBJECT_ID = ps.OBJECT_ID
													AND i.index_id = ps.index_id
	INNER JOIN sys.partitions AS p ON i.index_id = p.index_id
									  AND i.object_id = p.object_id
									  AND ps.partition_id = p.partition_id
	LEFT OUTER JOIN sys.dm_db_index_usage_stats AS [ius] ON i.object_id = ius.object_id
															AND i.index_id = ius.index_id
															AND ius.database_id = DB_ID()
ORDER BY 1
	   , 2
	   , 3
	   , 4
	   , 5;




-- No fragmentation or partition level details
WITH ctePartitionStatsSummary AS (
	SELECT OBJECT_ID
		 , index_id
		 , SUM(row_count) AS row_count
		 , SUM(used_page_count) AS used_page_count
		 , SUM(reserved_page_count) AS reserved_page_count
		 , COUNT(*) AS partition_count
	FROM sys.dm_db_partition_stats
	GROUP BY OBJECT_ID
		   , index_id
)
SELECT @@SERVERNAME AS [Server Name]
	 , DB_NAME() AS [Database Name]
	 , OBJECT_SCHEMA_NAME(i.OBJECT_ID) AS [Schema Name]
	 , OBJECT_NAME(i.OBJECT_ID) AS [Table Name]
	 , i.name AS [Index Name]
	 , i.is_primary_key AS [Is Primary Key]
	 , i.is_unique AS [Is Unique]
	 , i.type_desc AS [Type]
	 , ps.partition_count AS [Partition Count]
	 , ps.row_count AS [Row Count]
	 , ps.used_page_count AS [Used Page Count]
	 , (ps.used_page_count * 8) / POWER(1024.0, 1) AS [Used Page Count (MB)]
	 , (ps.used_page_count * 8) / POWER(1024.0, 2) AS [Used Page Count (GB)]
	 , ps.reserved_page_count AS [Reserved Page Count]
	 , (ps.reserved_page_count * 8) / POWER(1024.0, 1) AS [Reserved Page Count (MB)]
	 , (ps.reserved_page_count * 8) / POWER(1024.0, 2) AS [Reserved Page Count (GB)]
	 , ius.user_seeks AS [User Seeks]
	 , ius.user_scans AS [User Scans]
	 , ius.user_lookups AS [User Lookups]
	 , ius.user_updates AS [User Updates]
	 , ius.system_seeks AS [System Seeks]
	 , ius.system_scans AS [System Scans]
	 , ius.system_lookups AS [System Lookups]
	 , ius.system_updates AS [System Updates]
	 , CASE
		   WHEN (ius.user_seeks + ius.user_scans + ius.user_lookups + ius.user_updates) > 0 THEN
	 ( 1.0
	   - (CAST(ius.user_updates AS FLOAT)
		  / CAST((ius.user_seeks + ius.user_scans + ius.user_lookups + ius.user_updates) AS FLOAT)
		 )
	 )
		   ELSE 0
	   END AS [% Reads]
FROM sys.indexes AS [i]
	INNER JOIN ctePartitionStatsSummary AS [ps] ON i.OBJECT_ID = ps.OBJECT_ID
												   AND i.index_id = ps.index_id
	LEFT OUTER JOIN sys.dm_db_index_usage_stats AS [ius] ON i.object_id = ius.object_id
															AND i.index_id = ius.index_id
															AND ius.database_id = DB_ID()
ORDER BY 1
	   , 2
	   , 3
	   , 4
	   , 5;


GO


/*
-- For 2000 compatibility
SELECT 'DBCC SHOWCONTIG(''Rpt_Feed_Item_Sku'',''' + NAME + ''')'
FROM sysindexes
WHERE id = OBJECT_ID('Rpt_Feed_Item_Sku')
ORDER BY NAME;
GO

DBCC SHOWCONTIG() WITH TABLERESULTS;
*/

--DBCC SQLPERF(LOGSPACE)
