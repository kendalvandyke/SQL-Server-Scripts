/*********************************************************************************************
Replication - Show Command Counts At Distributor v1.00 (2010-11-01)
(C) 2010, Kendal Van Dyke

Feedback: mailto:kendal.vandyke@gmail.com

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

/* Show total commands per article by pending\delivered */
WITH    cteDistributionAgents
          AS ( SELECT   agent_id ,
                        MAX(timestamp) AS max_timestamp
               FROM     dbo.MSdistribution_history
               GROUP BY agent_id
             ),
        cteDistributionAgent
          AS ( SELECT   agent_history.agent_id ,
                        agent_history.xact_seqno
               FROM     cteDistributionAgents
                        INNER JOIN dbo.MSdistribution_history AS agent_history ON cteDistributionAgents.agent_id = agent_history.agent_id
                                                              AND cteDistributionAgents.max_timestamp = agent_history.timestamp
             )
    SELECT  Subscriptions.publisher_db AS PublisherDB ,
            Subscribers.srvname AS Subscriber ,
            Articles.article ,
            SUM(CASE WHEN Commands.xact_seqno > cteDistributionAgent.xact_seqno
                     THEN 1
                     ELSE 0
                END) AS PendingCommands ,
            SUM(CASE WHEN Commands.xact_seqno <= cteDistributionAgent.xact_seqno
                     THEN 1
                     ELSE 0
                END) AS DeliveredCommands ,
            COUNT(*) AS TotalCommands
    FROM    dbo.MSrepl_commands AS Commands
            INNER JOIN dbo.MSsubscriptions AS Subscriptions ON Commands.publisher_database_id = Subscriptions.publisher_database_id
                                                              AND Commands.article_id = Subscriptions.article_id
            INNER JOIN cteDistributionAgent ON Subscriptions.agent_id = cteDistributionAgent.agent_id
            INNER JOIN dbo.MSarticles AS Articles ON Subscriptions.publisher_id = Articles.publisher_id
                                                     AND Subscriptions.publication_id = Articles.publication_id
                                                     AND Subscriptions.article_id = Articles.article_id
            INNER JOIN master.dbo.sysservers AS Subscribers ON Subscriptions.subscriber_id = Subscribers.srvid
    --WHERE   Subscriptions.publisher_id = 27
    GROUP BY Subscriptions.publisher_db ,
            Subscribers.srvname ,
            Articles.article
    ORDER BY COUNT(*) DESC ;
GO



	
/* Only show total commands per article regardless of pending\delivered */
SELECT  Subscriptions.publisher_db AS PublisherDB ,
        Subscribers.srvname AS Subscriber ,
        Articles.article ,
        COUNT(*) AS TotalCommands
FROM    dbo.MSrepl_commands AS Commands
        INNER JOIN dbo.MSsubscriptions AS Subscriptions ON Commands.publisher_database_id = Subscriptions.publisher_database_id
                                                           AND Commands.article_id = Subscriptions.article_id
        INNER JOIN dbo.MSarticles AS Articles ON Subscriptions.publisher_id = Articles.publisher_id
                                                 AND Subscriptions.publication_id = Articles.publication_id
                                                 AND Subscriptions.article_id = Articles.article_id
        INNER JOIN master.dbo.sysservers AS Subscribers ON Subscriptions.subscriber_id = Subscribers.srvid
--WHERE   Subscriptions.publisher_id = 27
GROUP BY Subscriptions.publisher_db ,
        Subscribers.srvname ,
        Articles.article
ORDER BY COUNT(*) DESC ;
