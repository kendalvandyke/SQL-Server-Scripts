/*********************************************************************************************
Security - Script Database Level Permissions (2005+)

Description:
	Scripts database permissions (including objects) for a specific user or all database users

	(C) 2014, Kendal Van Dyke (mailto:kendal.vandyke@gmail.com)

Version History:
	v1.00 (2014-02-03)

License: 
	This query is free to download and use for personal, educational, and internal 
	corporate purposes, provided that this header is preserved. Redistribution or sale 
	of this query, in whole or in part, is prohibited without the author's express 
	written consent.

Note:
	Execute this query in the database that you wish to script permissions for
	
*********************************************************************************************/
SET NOCOUNT ON

DECLARE	@user SYSNAME;

-- Set this to NULL for all users
-- SET @user = 'DOMAIN\USER';
SET @user = NULL;

SELECT	'USE ' + QUOTENAME(DB_NAME()) + ';' AS '--Database Context';


-- Logins
SELECT	'CREATE USER [' + usr.name + '] FOR LOGIN [' + susr.name + ']'
		+ CASE WHEN usr.TYPE = 'S'
			   THEN ' WITH DEFAULT_SCHEMA=[' + usr.default_schema_name + '];'
			   ELSE ';'
		  END AS ' --Logins'
FROM	sys.database_permissions AS perm
		INNER JOIN sys.database_principals AS usr ON perm.grantee_principal_id = usr.principal_id
		INNER JOIN sys.server_principals AS susr ON usr.sid = susr.sid
WHERE	perm.permission_name = 'CONNECT'
		AND susr.sid != 0x01
		AND usr.NAME = COALESCE(@user, usr.NAME);


-- Role Members
SELECT	'EXEC sp_addrolemember @rolename = ' + QUOTENAME(usr.name, '''')
		+ ', @membername = ' + QUOTENAME(usr2.name, '''') + ';' AS '--Role Memberships'
FROM	sys.database_principals AS usr
		INNER JOIN sys.database_role_members AS rm ON usr.principal_id = rm.role_principal_id
		INNER JOIN sys.database_principals AS usr2 ON rm.member_principal_id = usr2.principal_id
WHERE	/*usr2.is_fixed_role = 0*/
		usr2.sid != 0x01
		AND usr2.NAME = COALESCE(@user, usr2.NAME)
ORDER BY rm.role_principal_id ASC;


-- Object permissions
WITH	cteObject ( [major_id], [name], [class_desc], [class_name] )
		  AS ( SELECT	obj.[object_id] AS [major_id] ,
						QUOTENAME(SCHEMA_NAME(obj.schema_id)) + '.'
						+ QUOTENAME(obj.name) AS [major_name] ,
						'OBJECT_OR_COLUMN' AS [class_desc] ,
						'OBJECT' AS [class_name]
			   FROM		sys.all_objects AS obj
			   UNION ALL
			   SELECT	assembly_id AS [major_id] ,
						QUOTENAME(name) COLLATE database_default ,
						'ASSEMBLY' AS [class_desc] ,
						'ASSEMBLY' AS [class_name]
			   FROM		sys.assemblies
			   UNION ALL
			   SELECT	asymmetric_key_id AS [major_id] ,
						QUOTENAME(name) COLLATE database_default ,
						'ASYMMETRIC KEY' AS [class_desc] ,
						'ASYMMETRIC_KEY' AS [class_name]
			   FROM		sys.asymmetric_keys
			   UNION ALL
			   SELECT	certificate_id AS [major_id] ,
						QUOTENAME(name) COLLATE database_default ,
						'CERTIFICATE' AS [class_desc] ,
						'CERTIFICATE' AS [class_name]
			   FROM		sys.certificates
			   UNION ALL
			   SELECT	principal_id AS [major_id] ,
						QUOTENAME(name) COLLATE database_default ,
						'DATABASE_PRINCIPAL' AS [class_desc] ,
						CASE type_desc
						  WHEN 'APPLICATION_ROLE' THEN 'APPLICATION ROLE'
						  WHEN 'DATABASE_ROLE' THEN 'ROLE'
						  ELSE 'USER'
						END AS [class_name]
			   FROM		sys.database_principals
			   UNION ALL
			   SELECT	fulltext_catalog_id AS [major_id] ,
						QUOTENAME(name) COLLATE database_default ,
						'FULLTEXT_CATALOG' AS [class_desc] ,
						'FULLTEXT CATALOG' AS [class_name]
			   FROM		sys.fulltext_catalogs
			   UNION ALL
			   SELECT	stoplist_id AS [major_id] ,
						QUOTENAME(name) COLLATE database_default ,
						'FULLTEXT_STOPLIST' AS [class_desc] ,
						'FULLTEXT STOPLIST' AS [class_name]
			   FROM		sys.fulltext_stoplists
			   UNION ALL
			   SELECT	message_type_id AS [major_id] ,
						QUOTENAME(name) COLLATE database_default ,
						'MESSAGE_TYPE' AS [class_desc] ,
						'MESSAGE TYPE' AS [class_name]
			   FROM		sys.service_message_types
			   UNION ALL
			   SELECT	remote_service_binding_id AS [major_id] ,
						QUOTENAME(name) COLLATE database_default ,
						'REMOTE_SERVICE_BINDING' AS [class_desc] ,
						'REMOTE SERVICE BINDING' AS [class_name]
			   FROM		sys.remote_service_bindings
			   UNION ALL
			   SELECT	route_id AS [major_id] ,
						QUOTENAME(name) COLLATE database_default ,
						'ROUTE' AS [class_desc] ,
						'ROUTE' AS [class_name]
			   FROM		sys.routes
			   UNION ALL
			   SELECT	[schema_id] AS [major_id] ,
						QUOTENAME(name) COLLATE database_default ,
						'SCHEMA' AS [class_desc] ,
						'SCHEMA' AS [class_name]
			   FROM		sys.schemas
			   UNION ALL
			   SELECT	service_id AS [major_id] ,
						QUOTENAME(name) COLLATE database_default ,
						'SERVICE' AS [class_desc] ,
						'SERVICE' AS [class_name]
			   FROM		sys.services
			   UNION ALL
			   SELECT	service_contract_id AS [major_id] ,
						QUOTENAME(name) COLLATE database_default ,
						'SERVICE_CONTRACT' AS [class_desc] ,
						'CONTRACT' AS [class_name]
			   FROM		sys.service_contracts
			   UNION ALL
			   SELECT	symmetric_key_id AS [major_id] ,
						QUOTENAME(name) COLLATE database_default ,
						'SYMMETRIC_KEY' AS [class_desc] ,
						'SYMMETRIC KEY' AS [class_name]
			   FROM		sys.symmetric_keys
			   UNION ALL
			   SELECT	user_type_id AS [major_id] ,
						QUOTENAME(name) COLLATE database_default ,
						'TYPE' AS [class_desc] ,
						'TYPE' AS [class_name]
			   FROM		sys.types
			   UNION ALL
			   SELECT	xml_collection_id AS [major_id] ,
						QUOTENAME(SCHEMA_NAME(schema_id)) + '.'
						+ QUOTENAME(name) COLLATE database_default ,
						'XML_SCHEMA_COLLECTION' AS [class_desc] ,
						'XML SCHEMA COLLECTION' AS [class_name]
			   FROM		sys.xml_schema_collections
			 )
	SELECT	CASE WHEN perm.state <> 'W' THEN perm.state_desc
				 ELSE 'GRANT'
			END + SPACE(1) + perm.permission_name + ' ON ' + obj.class_name
			+ ' :: ' + obj.name + CASE WHEN cl.column_id IS NULL THEN SPACE(0)
									   ELSE ' (' + QUOTENAME(cl.name) + ')'
								  END + ' TO ' + QUOTENAME(usr.name) COLLATE database_default
			+ CASE WHEN perm.state <> 'W' THEN ';'
				   ELSE ' WITH GRANT OPTION;'
			  END AS '--Object Level Permissions'
	FROM	sys.database_permissions AS perm
			INNER JOIN cteObject AS obj ON perm.major_id = obj.major_id
										   AND perm.class_desc = obj.class_desc
			INNER JOIN sys.database_principals AS usr ON perm.grantee_principal_id = usr.principal_id
			LEFT JOIN sys.all_columns AS cl ON perm.minor_id = cl.column_id
											   AND perm.major_id = cl.[object_id]
											   AND perm.class_desc = 'OBJECT_OR_COLUMN'
	WHERE	usr.is_fixed_role = 0
			AND usr.sid != 0x01
			AND usr.NAME = COALESCE(@user, usr.NAME)
	ORDER BY perm.permission_name ASC ,
			perm.state_desc ASC;


-- Database permissions (non-object specific)
SELECT	CASE WHEN perm.state <> 'W' THEN perm.state_desc
			 ELSE 'GRANT'
		END + SPACE(1) + perm.permission_name + ' TO ' + QUOTENAME(usr.name) COLLATE database_default
		+ CASE WHEN perm.state <> 'W' THEN ';'
			   ELSE ' WITH GRANT OPTION;'
		  END AS ' --Database Level Permissions'
FROM	sys.database_permissions AS perm
		INNER JOIN sys.database_principals AS usr ON perm.grantee_principal_id = usr.principal_id
WHERE	perm.major_id = 0
		AND usr.NAME != 'dbo'
		AND usr.NAME = COALESCE(@user, usr.NAME)
ORDER BY perm.permission_name ASC ,
		perm.state_desc ASC;