/*********************************************************************************************
Transactional Replication Toolbox: Show Articles and Columns for All Publications

Description:
	Shows articles and columns for each article for all transactional publications

	(C) 2013, Kendal Van Dyke (mailto:kendal.vandyke@gmail.com)

Version History:
	v1.00 (2013-01-29)

License: 
	This query is free to download and use for personal, educational, and internal 
	corporate purposes, provided that this header is preserved. Redistribution or sale 
	of this query, in whole or in part, is prohibited without the author's express 
	written consent.

Note: 
	Execute this query in the published database on the PUBLISHER

	Because this query uses FOR XML PATH('') it requires SQL 2005 or higher
	
*********************************************************************************************/

SELECT 
	syspublications.name AS "Publication", 
	sysarticles.name AS "Article", 
	STUFF(
		(
			SELECT ', ' + syscolumns.name AS [text()]
			FROM sysarticlecolumns WITH (NOLOCK)
				INNER JOIN syscolumns WITH (NOLOCK) ON sysarticlecolumns.colid = syscolumns.colorder
			WHERE sysarticlecolumns.artid = sysarticles.artid
				AND sysarticles.objid = syscolumns.id
			ORDER BY syscolumns.colorder
			FOR XML PATH('')
		), 1, 2, ''
	) AS "Columns"
FROM syspublications WITH (NOLOCK)
	INNER JOIN sysarticles WITH (NOLOCK) ON syspublications.pubid = sysarticles.pubid
ORDER BY syspublications.name, sysarticles.name
