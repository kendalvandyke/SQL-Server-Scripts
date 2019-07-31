/*********************************************************************************************
Transactional Replication Toolkit: Show Logreader Agent Volume By Day

Description:
	Shows the Logreader Agent Volume By Day (interpolated using replication history)

	(C) 2012, Kendal Van Dyke (mailto:kendal.vandyke@gmail.com)

Version History:
	v1.00 (2013-01-29)

License: 
	This query is free to download and use for personal, educational, and internal 
	corporate purposes, provided that this header is preserved. Redistribution or sale 
	of this query, in whole or in part, is prohibited without the author's express 
	written consent.

Note: 
	Execute this query on the DISTRIBUTOR
	
*********************************************************************************************/
USE distribution	-- Change this if your distribution database has a different name
GO

SET TRANSACTION ISOLATION LEVEL REPEATABLE READ ;
GO

DECLARE @agent_history TABLE
    (
      agent_id INT ,
      delivered_commands INT ,
      start_time DATETIME ,
      duration INT
    ) ;

/*

-- dbo.MSlogreader_history.runstatus
1 = Start...punt
2 = Succeed...counts come from the most recent 3 or 4 history preceeding the 2 
3 = In progress...counts come from most recent record not followed by a 1,2,3,4,5
4 = Idle...counts come from most recent record not followed by a 1,2,3,4,5 
5 = Retry...counts come from this row 
6 = Fail...counts come from this row

*/


-- Get the latest value for agents in "In progress" (3) or "Idle" (4) status
-- Disregard entries with a more recent runstatus of 1, 2, 5, or 6
WITH    history_subset
          AS ( SELECT   history_running.agent_id ,
                        MAX(history_running.[timestamp]) AS [timestamp] ,
                        MAX(history_running.runstatus) AS [runstatus]
               FROM     dbo.MSlogreader_history AS [history_running]
               WHERE    history_running.runstatus IN ( 3, 4 )
                        AND ( history_running.comments NOT LIKE N'The process is running and is waiting%'
                              AND history_running.comments NOT LIKE N'<stats state%'
                              AND history_running.comments NOT LIKE N'The replication agent has not logged a progress message%'
                            )
                        AND NOT EXISTS ( SELECT agent_id
                                         FROM   dbo.MSlogreader_history
                                         WHERE  runstatus IN ( 1, 2, 5, 6 )
                                                AND agent_id = history_running.agent_id
                                                AND [timestamp] > history_running.[timestamp] )
               GROUP BY history_running.agent_id
               UNION ALL
               SELECT   history_running.agent_id ,
                        history_running.[timestamp] ,
                        history_running.runstatus
               FROM     dbo.MSlogreader_history AS [history_running]
               WHERE    history_running.runstatus IN ( 5 )
                        AND NOT EXISTS ( SELECT agent_id
                                         FROM   dbo.MSlogreader_history
                                         WHERE  runstatus IN ( 6 )
                                                AND agent_id = history_running.agent_id
                                                AND [timestamp] > history_running.[timestamp]
                                                AND xact_seqno = history_running.xact_seqno )
             )
    INSERT  INTO @agent_history
            ( agent_id ,
              delivered_commands ,
              start_time ,
              duration
            )
            SELECT  history_all.agent_id ,
                    history_all.delivered_commands ,
                    history_all.start_time ,
                    history_all.duration /*,
            history_subset.* */
            FROM    history_subset
                    INNER JOIN dbo.MSlogreader_history AS [history_all] ON history_subset.agent_id = history_all.agent_id
                                                              AND history_subset.[timestamp] = history_all.[timestamp]
            ORDER BY history_all.agent_id ,
                    history_subset.[timestamp] ;



-- Now grab the historical values for agents in "Succed" (2) or "Fail" (6) status
-- runstatus IN ( 2 , 6 ) will reset counts when agent fires up again
-- For (2,6) we need to grab the most recent (3,4,5) row before it to get accurate counts
WITH    history_stopped
          AS ( SELECT   agent_id ,
                        [timestamp] ,
                        xact_seqno ,
                        ROW_NUMBER() OVER ( PARTITION BY agent_id ORDER BY agent_id, [timestamp] ) AS [rownum]
               FROM     dbo.MSlogreader_history
               WHERE    runstatus IN ( 2, 6 )
                        AND ( comments NOT LIKE N'The process is running and is waiting%'
                              AND comments NOT LIKE N'<stats state%'
                              AND comments NOT LIKE N'The replication agent has not logged a progress message%'
                            )
             ),
        history_stopped_transformed
          AS ( SELECT   history_stopped.agent_id ,
                        history_stopped.xact_seqno ,
                        history_stopped_prev.xact_seqno AS [last_xact_seqno] ,
                        history_stopped.[timestamp] ,
                        COALESCE(history_stopped_prev.[timestamp], 0x0) AS [last_timestamp] ,
                        history_stopped.rownum ,
                        history_stopped_prev.rownum AS [last_rownum]
               FROM     history_stopped
                        LEFT OUTER JOIN history_stopped AS [history_stopped_prev] ON history_stopped.agent_id = history_stopped_prev.agent_id
                                                              AND history_stopped.rownum = ( history_stopped_prev.rownum
                                                              + 1 )
             ),
        history_subset
          AS ( SELECT   history_stopped_transformed.agent_id ,
                        history_stopped_transformed.rownum ,
                        history_stopped_transformed.xact_seqno ,
                        MAX(distribution_history.[timestamp]) AS [timestamp]
               FROM     history_stopped_transformed
                        INNER JOIN dbo.MSlogreader_history AS [distribution_history] ON history_stopped_transformed.agent_id = distribution_history.agent_id
                                                              AND history_stopped_transformed.xact_seqno = distribution_history.xact_seqno
                                                              AND history_stopped_transformed.[timestamp] > distribution_history.[timestamp]
                                                              AND history_stopped_transformed.[last_timestamp] < distribution_history.[timestamp]
               WHERE    [distribution_history].runstatus IN ( 3, 4, 5 )
               GROUP BY history_stopped_transformed.agent_id ,
                        history_stopped_transformed.rownum ,
                        history_stopped_transformed.xact_seqno
             )
    INSERT  INTO @agent_history
            ( agent_id ,
              delivered_commands ,
              start_time ,
              duration
					
            )
            SELECT  history_all.agent_id ,
                    history_all.delivered_commands ,
                    history_all.start_time ,
                    history_all.duration 
            FROM    history_subset
                    INNER JOIN dbo.MSlogreader_history AS [history_all] ON history_subset.agent_id = history_all.agent_id
                                                              AND history_subset.[timestamp] = history_all.[timestamp]
;

-- By publisher & publication
WITH    agent_summary
          AS ( SELECT   agent_id ,
                        MIN(start_time) AS [Min_Start_Time] ,
                        MAX(start_time) AS [Max_Start_Time] ,
                        SUM(delivered_commands) AS [Total_delivered_commands] ,
                        CONVERT(DECIMAL, SUM(delivered_commands)) / SUM(duration) AS [Avg_delivered_commands_per_second_running] ,
                        CONVERT(DECIMAL, SUM(delivered_commands)) / DATEDIFF(ss, MIN(start_time), GETDATE()) AS [Avg_delivered_commands_per_second_total] ,
                        SUM(duration) AS [Total_time_running],
                        DATEDIFF(ss, MIN(start_time), GETDATE()) AS [Total_time]
               FROM     @agent_history
               GROUP BY agent_id
             )
    SELECT  publishers.name AS [publisher] ,
			@@SERVERNAME AS [distributor] ,
            logreader_agents.publisher_db ,
            --logreader_agents.publication ,
            agent_summary.Min_Start_Time,
            agent_summary.Total_time_running,
			agent_summary.Total_time,
            agent_summary.Total_delivered_commands ,
            agent_summary.Avg_delivered_commands_per_second_running ,
            ( agent_summary.Avg_delivered_commands_per_second_running * 86400.0 ) AS [Avg_delivered_commands_per_day_running],
            agent_summary.Avg_delivered_commands_per_second_total ,
            ( agent_summary.Avg_delivered_commands_per_second_total * 86400.0 ) AS [Avg_delivered_commands_per_day_total]
    FROM    agent_summary
            INNER JOIN dbo.MSlogreader_agents AS [logreader_agents] ON agent_summary.agent_id = logreader_agents.id
            INNER JOIN sys.servers AS [publishers] ON logreader_agents.publisher_id = publishers.server_id
    ORDER BY publishers.name ,
            logreader_agents.publication ;

