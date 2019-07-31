SET NOCOUNT ON

DECLARE @user sysname

-- Set this to NULL for all users
SET @user = NULL

-- Role Members
SELECT usr2.name AS [User],
	usr.name AS [Role]
FROM    sys.database_principals AS usr
        INNER JOIN sys.database_role_members AS rm ON usr.principal_id = rm.role_principal_id
        INNER JOIN sys.database_principals AS usr2 ON rm.member_principal_id = usr2.principal_id
WHERE   usr.is_fixed_role = 0
	AND usr2.NAME = COALESCE(@user, usr2.NAME)
ORDER BY usr2.name, rm.role_principal_id ASC



-- Object level permissions
SELECT usr.NAME AS [User],
QUOTENAME(USER_NAME(obj.schema_id)) + '.' + QUOTENAME(obj.name)
        + CASE WHEN cl.column_id IS NULL THEN SPACE(0)
               ELSE '(' + QUOTENAME(cl.name) + ')'
          END AS [Object],
          perm.permission_name,
CASE WHEN perm.state <> 'W' THEN perm.state_desc
             ELSE 'GRANT'
       END AS [State]
FROM    sys.database_permissions AS perm
        INNER JOIN sys.objects AS obj ON perm.major_id = obj.[object_id]
        INNER JOIN sys.database_principals AS usr ON perm.grantee_principal_id = usr.principal_id
        LEFT JOIN sys.columns AS cl ON cl.column_id = perm.minor_id
                                       AND cl.[object_id] = perm.major_id
WHERE   usr.is_fixed_role = 0
        AND usr.sid != 0x01
        AND usr.NAME = COALESCE(@user, usr.NAME)
ORDER BY usr.NAME,
		perm.permission_name ASC,
        perm.state_desc ASC,
        QUOTENAME(USER_NAME(obj.schema_id)),
        obj.name



-- Database permissions
--SELECT  CASE WHEN perm.state <> 'W' THEN perm.state_desc
--             ELSE 'GRANT'
--        END + SPACE(1) + perm.permission_name + SPACE(1) + + 'TO'
--        + SPACE(1) + QUOTENAME(usr.name) COLLATE database_default
--        + CASE WHEN perm.state <> 'W' THEN SPACE(0)
--               ELSE SPACE(1) + 'WITH GRANT OPTION'
--          END AS '--Database Level Permissions'

SELECT 
	DB_NAME() AS [Database],
	srv_logins.name AS [Server Login],
	usr.name AS [DB User],
	perm.permission_name AS [Permission],
	perm.state_desc AS [Permission State]
FROM    sys.database_permissions AS perm
        INNER JOIN sys.database_principals AS usr ON perm.grantee_principal_id = usr.principal_id
        INNER JOIN sys.sql_logins AS srv_logins ON usr.sid = srv_logins.sid
WHERE   /*usr.name = @OldUser
        AND */
        perm.major_id = 0
        AND usr.NAME != 'dbo'
        AND usr.NAME = COALESCE(@user, usr.NAME)
ORDER BY srv_logins.name,
		usr.name,
		perm.permission_name ASC,
        perm.state_desc ASC

