-- Execute in published database on publisher to generate scripts which enable\diable DDL replication
-- Value = 0 indicates DO NOT replicate DDL changes
-- Value = 1 indicates DO replicate DDL changes
SELECT 'exec sp_changepublication @publication = N''' + name + ''', @property = N''replicate_ddl'', @value = N''0'''
FROM syspublications WITH (NOLOCK)
ORDER BY name
