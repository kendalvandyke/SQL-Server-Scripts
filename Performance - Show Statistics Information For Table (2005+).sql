WITH cteStatistics AS (
	SELECT sc.name AS [Schema Name]
		   , o.name AS [Object Name]
		   , sp.stats_id AS [Statistic ID]
		   , s.name AS [Statistic Name]
		   , sp.last_updated AS [Last Updated]
		   , sp.rows
		   , sp.rows_sampled
		   , sp.unfiltered_rows
		   , sp.modification_counter AS Modifications
	FROM sys.stats AS s
		OUTER APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) AS sp
		INNER JOIN sys.objects AS o ON s.object_id = o.object_id
		INNER JOIN sys.schemas AS sc ON o.schema_id = sc.schema_id
	WHERE o.is_ms_shipped = 0
)
SELECT [Schema Name]
	   , [Object Name]
	   , [Statistic ID]
	   , [Statistic Name]
	   , [Last Updated]
	   , rows
	   , rows_sampled
	   , unfiltered_rows
	   , Modifications
FROM cteStatistics
--WHERE o.name = 'object name'
ORDER BY [Schema Name]
		 , [Object Name]
		 , [Statistic ID];


-- Add some data about stats updates
WITH cteStatistics AS (
	SELECT sc.name AS [Schema Name]
		   , o.name AS [Object Name]
		   , o.type_desc AS [Object Type]
		   , sp.stats_id AS [Statistic ID]
		   , s.name AS [Statistic Name]
		   , sp.last_updated AS [Last Updated]
		   , DATEADD(DAY, 1, sp.last_updated) AS [LastUpdatedPlusOneDay]
		   , sp.rows
		   , sp.rows_sampled
		   , sp.unfiltered_rows
		   , sp.modification_counter
		   , CAST(sp.modification_counter AS FLOAT) / DATEDIFF(MINUTE, sp.last_updated, GETDATE()) AS [ModificationsPerMinute]
		   , DATEDIFF(MINUTE, sp.last_updated, GETDATE()) AS [MinutesSinceLastUpdate]
		   , CASE
				 WHEN sp.rows >= 500 THEN sp.rows * .20
				 ELSE 500
			 END AS [StaticThreshold]
		   , SQRT(1000 * sp.rows) AS [DynamicThreshold]
	FROM sys.stats AS s
		OUTER APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) AS sp
		INNER JOIN sys.objects AS o ON s.object_id = o.object_id
		INNER JOIN sys.schemas AS sc ON o.schema_id = sc.schema_id
	WHERE o.is_ms_shipped = 0
)
SELECT @@SERVERNAME AS [Instance Name]
	   , DB_NAME() AS [Database Name]
	   , [Schema Name]
	   , [Object Name]
	   , [Statistic ID]
	   , [Statistic Name]
	   , [Last Updated]
	   , [MinutesSinceLastUpdate] AS [Minutes Since Last Update]
	   , rows AS [Row Count]
	   , [StaticThreshold] AS [Rows Required To Change (Static)]
	   , [DynamicThreshold] AS [Rows Required To Change (Dynamic)]
	   , rows_sampled AS [Rows Sampled]
	   , (CAST(rows_sampled AS FLOAT) / rows) * 100.0 AS [Sample %]
	   , unfiltered_rows AS [Unfiltered Rows]
	   , modification_counter AS [Modification Count]
	   , [StaticThreshold] - modification_counter AS [Modifications Until Auto-Update (Static)]
	   , [DynamicThreshold] - modification_counter AS [Modifications Until Auto-Update (Dynamic)]
	   , CASE
			 WHEN [ModificationsPerMinute] > 0 THEN
				 DATEADD(MINUTE, (([StaticThreshold] - modification_counter) / [ModificationsPerMinute]), GETDATE())
			 ELSE NULL
		 END AS [Projected Auto-Update (Static)]
	   , CASE
			 WHEN [ModificationsPerMinute] > 0 THEN
				 DATEADD(MINUTE, (([DynamicThreshold] - modification_counter) / [ModificationsPerMinute]), GETDATE())
			 ELSE NULL
		 END AS [Projected Auto-Update (Dynamic)]
	   , CASE
			 WHEN [ModificationsPerMinute] <= 0 THEN 'No rows have changed'
			 WHEN (([DynamicThreshold] - modification_counter) / [ModificationsPerMinute]) < 0 THEN
				 'Update will have happened already'
			 WHEN DATEADD(MINUTE, (([DynamicThreshold] - modification_counter) / [ModificationsPerMinute]), GETDATE()) <= [LastUpdatedPlusOneDay] THEN
				 'Update likely within 24 hours of last update'
			 ELSE 'Update will NOT happen within next 24 hours'
		 END AS [Auto-Update Reason]
FROM cteStatistics
ORDER BY [Schema Name]
		 , [Object Name]
		 , [Statistic ID];