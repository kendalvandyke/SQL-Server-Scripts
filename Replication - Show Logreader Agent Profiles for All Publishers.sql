/*********************************************************************************************
Replication - Show Logreader Agent Profiles For All Publications v1.00 (2010-11-01)
(C) 2010, Kendal Van Dyke

Feedback: mailto:kendal.vandyke@gmail.com

License: 
	This query is free to download and use for personal, educational, and internal 
	corporate purposes, provided that this header is preserved. Redistribution or sale 
	of this query, in whole or in part, is prohibited without the author's express 
	written consent.
	
Note: 
	This query will display the Logreader Agent Profiles used for each published DB
	Execute this query on the DISTRIBUTOR
	
*********************************************************************************************/

USE distribution	-- Change this if your distribution database has a different name
GO
--SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
--GO

SELECT  DISTINCT 
		publishers.srvname AS [Publisher] ,
        publications.publisher_db AS [Publisher DB] ,
		agents.id AS [Agent ID],
		agents.job_id AS [Agent Job ID],
        profiles.profile_name AS [Agent Profile],
        profiles.description AS [Profile Description],
		profiles.def_profile AS [Is Default Profile]
		
FROM    sys.sysservers AS publishers
        INNER JOIN dbo.MSarticles AS articles ON publishers.srvid = articles.publisher_id
        INNER JOIN dbo.MSpublications AS publications ON articles.publisher_id = publications.publisher_id
                                                              AND articles.publication_id = publications.publication_id
        INNER JOIN dbo.MSlogreader_agents AS agents ON publications.publisher_id = agents.publisher_id
																	AND publications.publisher_db = agents.publisher_db
        INNER JOIN msdb.dbo.MSagent_profiles AS profiles ON agents.profile_id = profiles.profile_id
ORDER BY publishers.srvname