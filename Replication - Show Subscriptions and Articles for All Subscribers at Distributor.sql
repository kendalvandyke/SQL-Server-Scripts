/*********************************************************************************************
Transactional Replication Toolbox: Show Subscriptions and Articles for Subscribers at Distributor

Description:
	Show Transactional Publications and Subscriptions to articles at Distributor

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

	Add a WHERE clause to limit results to one publisher\subscriber\publication\etc
	
*********************************************************************************************/

USE distribution	-- Change this if your distribution database has a different name
GO

SELECT  publishers.srvname AS [Publisher] ,
        publications.publisher_db AS [Publisher DB] ,
        publications.publication AS [Publication] ,
		@@SERVERNAME AS [Distributor] ,
        subscribers.srvname AS [Subscriber] ,
        subscriptions.subscriber_db AS [Subscriber DB] ,
        articles.article AS [Article],
        profiles.profile_name AS [Agent Profile],
        profiles.description AS [Profile Description],
		profiles.def_profile AS [Is Default Profile]        
FROM    sys.sysservers AS publishers
        INNER JOIN distribution.dbo.MSarticles AS articles ON publishers.srvid = articles.publisher_id
        INNER JOIN distribution.dbo.MSpublications AS publications ON articles.publisher_id = publications.publisher_id
                                                              AND articles.publication_id = publications.publication_id
        INNER JOIN distribution.dbo.MSsubscriptions AS subscriptions ON articles.publisher_id = subscriptions.publisher_id
                                                              AND articles.publication_id = subscriptions.publication_id
                                                              AND articles.article_id = subscriptions.article_id
        INNER JOIN sys.sysservers AS subscribers ON subscriptions.subscriber_id = subscribers.srvid

        INNER JOIN distribution.dbo.MSdistribution_agents AS agents ON subscriptions.agent_id = agents.id
        INNER JOIN msdb.dbo.MSagent_profiles AS profiles ON agents.profile_id = profiles.profile_id

---- Limit results to subscriber 
--WHERE   subscribers.srvname LIKE '%'

---- Limit results to publisher and publication
--WHERE   publishers.srvname = '%'
--        --AND publications.publisher_db = ''

ORDER BY publishers.srvname ,
        publications.publication ,
        articles.article,
        subscribers.srvname


