-- This query is designed to help you find hangups that will prevent you from deleting a server login
-- and database users associated with the login.

-- You need to have SYSADMIN rights in order for this script to work properly

-- Enter the login name you want to delete here, then execute the script. 
--DECLARE @PrincipalName SYSNAME = 'DOMAIN\Davidsta';
--DECLARE @PrincipalName SYSNAME = 'DOMAIN\SteveMc';
--DECLARE @PrincipalName SYSNAME = 'DOMAIN\WadeJo';
--DECLARE @PrincipalName SYSNAME = 'DOMAIN\DanielPr';

DECLARE @PrincipalName SYSNAME = 'testuser';

-- Here's what you'll see:
-- Query 0: Server login for user
-- Query 1: Server login to database user mapping
-- Query 2: Schemas owned by the database users
-- Query 3: Object counts for each schema owned by the database users
-- Query 4: Objects for each schema owned by the database users
-- Query 5: SQL Agent Jobs owned by the server login

-- This query does NOT delete the logins or users, nor does it fix any of the hangups. 
-- The HelperScript column in the query results shows a suggested query to run to fix anything that needs fixin'
-- Note that it's a *suggested* query and may not be what you actually want to run depending on the results
-- Finally, you'll want to execute the suggested queries from last resultset to first


-- Questions about how this works? Something missing or not quite working right?
-- Contact Kendal Van Dyke at kendal.vandyke@upsearch.com



-- Commence Magic from this point forward!
CREATE TABLE #OwnedSchema
    (
      DatabaseName SYSNAME ,
      DatabaseId INT ,
      OwnerName SYSNAME ,
      OwnerId INT ,
      OwnerSid VARBINARY(85) ,
      SchemaName SYSNAME ,
      SchemaId INT ,
      SchemaPrincipalType SYSNAME
    );

CREATE TABLE #OwnedSchemaObject
    (
      DatabaseName SYSNAME ,
      DatabaseId INT ,
      OwnerName SYSNAME ,
      OwnerId INT ,
      SchemaName SYSNAME ,
      SchemaId INT ,
      ObjectName SYSNAME ,
      ObjectType SYSNAME
    );

CREATE TABLE #UserAccess
    (
      DatabaseName SYSNAME ,
      UserName SYSNAME ,
      UserSid VARBINARY(85)
    )


-- Dynamic SQL run against each DB
DECLARE @Sql NVARCHAR(2000) = '
use [?];

DECLARE @PrincipalName SYSNAME = ''' + @PrincipalName
    + ''';


INSERT  INTO #UserAccess
( DatabaseName ,
    UserName ,
    UserSid
)
SELECT  DB_NAME() ,
        dp.name ,
        dp.sid
FROM    sys.database_principals AS dp
        LEFT OUTER JOIN sys.server_principals AS sp ON dp.sid = sp.sid
WHERE   dp.name = @PrincipalName
        OR sp.name = @PrincipalName;


INSERT  INTO #OwnedSchema
( DatabaseName ,
    DatabaseId ,
    OwnerName ,
    OwnerId ,
    OwnerSid ,
    SchemaName ,
    SchemaId ,
    SchemaPrincipalType
)
SELECT  DB_NAME() ,
        DB_ID() ,
        dp.name ,
        dp.principal_id ,
        dp.sid ,
        s.name ,
        s.schema_id ,
        dp2.type_desc
FROM    sys.schemas AS s
        INNER JOIN sys.database_principals AS dp ON s.principal_id = dp.principal_id
        INNER JOIN sys.database_principals AS dp2 ON s.schema_id = dp2.principal_id
        LEFT OUTER JOIN sys.server_principals AS sp ON dp.sid = sp.sid
WHERE   dp.name = @PrincipalName
        OR sp.name = @PrincipalName;

INSERT  INTO #OwnedSchemaObject
( DatabaseName ,
    DatabaseId ,
    SchemaName ,
    SchemaId ,
    OwnerName ,
    OwnerId ,
    ObjectName ,
    ObjectType
)
SELECT  DB_NAME() ,
        DB_ID() ,
        s.name ,
        s.schema_id ,
        dp.name ,
        dp.principal_id ,
        o.name ,
        o.type_desc
FROM    sys.objects AS o
        INNER JOIN sys.schemas AS s ON o.schema_id = s.schema_id
        INNER JOIN sys.database_principals AS dp ON s.principal_id = dp.principal_id
        LEFT OUTER JOIN sys.server_principals AS sp ON dp.sid = sp.sid
WHERE   dp.name = @PrincipalName
        OR sp.name = @PrincipalName;
';

EXECUTE sp_MSforeachdb @Sql;


-- Query 0: 
-- Server login for user
-- If this is empty but subsequent queries show databases users then we've got orphaned users
SELECT  @@SERVERNAME AS 'ServerName' ,
        name AS 'LoginName' ,
        type_desc AS 'LoginType' ,
        is_disabled AS 'IsDisabled' ,
        '-- Server login' AS 'Description' ,
        '--DROP LOGIN [' + name + '];' AS 'HelperScript'
FROM    sys.server_principals
WHERE   name = @PrincipalName;


-- Query 1:
-- Server login to database user mapping
-- Database users without a server login are orphaned users
-- This also uncovers logins mapped to users that don't have a matching name
SELECT  @@SERVERNAME AS 'ServerName' ,
        sp.name AS 'ServerLogin' ,
        ua.DatabaseName ,
        ua.UserName AS 'DatabaseUser' ,
        '-- Server login to database user mapping' AS 'Description' ,
        '--USE [' + ua.DatabaseName + ']; DROP USER [' + ua.UserName + '];' AS 'HelperScript'
FROM    #UserAccess AS ua
        LEFT OUTER JOIN sys.server_principals AS sp ON ua.UserSid = sp.sid
ORDER BY sp.name ,
        ua.DatabaseName;



-- Query 2: 
-- Schemas owned by the database users
-- These need to be assigned a new owner (e.g. for fixed roles) before the user can be dropped
SELECT  @@SERVERNAME AS 'ServerName' ,
        DatabaseName ,
        OwnerName ,
        SchemaName ,
        SchemaPrincipalType ,
        '-- Schemas owned by the database users' AS 'Description' ,
        CASE WHEN ( SchemaName = 'dbo' )
             THEN '--USE [' + DatabaseName
                  + ']; execute sp_changedbowner @loginame = ''sa'''
             WHEN ( SchemaPrincipalType = 'DATABASE_ROLE' )
             THEN '--USE [' + DatabaseName
                  + ']; ALTER AUTHORIZATION ON SCHEMA::[' + SchemaName
                  + '] TO [' + SchemaName + ']'
             ELSE '--See query 3 results'
        END AS 'HelperScript'
FROM    #OwnedSchema;


-- Query 3:
-- Object counts for each schema owned by the database users
-- User Schemas that have have no objects associated with them (ObjectCount = 0) can likely be dropped
-- Schema that have objects associated with them need to be assigned a new owner or drop all objects first
WITH    cteOwnedSchemaObject ( DatabaseId, OwnerId, SchemaId, ObjectCount )
          AS ( SELECT   DatabaseId ,
                        OwnerId ,
                        SchemaId ,
                        COUNT(*) AS ObjectCount
               FROM     #OwnedSchemaObject
               GROUP BY DatabaseId ,
                        OwnerId ,
                        SchemaId
             )
    SELECT  @@SERVERNAME AS 'ServerName' ,
            OwnedSchema.DatabaseName AS 'DatabaseName' ,
            OwnedSchema.OwnerName AS 'OwnerName' ,
            OwnedSchema.SchemaName AS 'SchemaName' ,
            OwnedSchema.SchemaPrincipalType AS 'SchemaPrincipalType' ,
            COALESCE(cteOwnedSchemaObject.ObjectCount, 0) AS 'ObjectCount' ,
            '-- Object counts for each schema owned by the database users' AS 'Description' ,
            CASE WHEN ( SchemaName = 'dbo' )
                 THEN '--USE [' + DatabaseName
                      + ']; execute sp_changedbowner @loginame = ''sa'''
                 WHEN ( COALESCE(cteOwnedSchemaObject.ObjectCount, 0) = 0 )
                      AND ( ( OwnedSchema.OwnerName = OwnedSchema.SchemaName )
                            OR ( ( OwnedSchema.SchemaPrincipalType = 'WINDOWS_USER' )
                                 AND ( RIGHT(OwnedSchema.OwnerName,
                                             LEN(OwnedSchema.SchemaName) + 1) = ( '\'
                                                              + OwnedSchema.SchemaName ) )
                               )
                          )
                 THEN '--USE [' + OwnedSchema.DatabaseName
                      + ']; DROP SCHEMA [' + OwnedSchema.SchemaName + '];'
                 ELSE '--USE [' + OwnedSchema.DatabaseName
                      + ']; ALTER AUTHORIZATION ON SCHEMA::['
                      + OwnedSchema.SchemaName
                      + '] TO [<database user, sysname, dbo>];'
            END AS 'HelperScript'
    FROM    #OwnedSchema AS OwnedSchema
            LEFT OUTER JOIN cteOwnedSchemaObject ON OwnedSchema.DatabaseId = cteOwnedSchemaObject.DatabaseId
                                                    AND OwnedSchema.OwnerId = cteOwnedSchemaObject.OwnerId
                                                    AND OwnedSchema.SchemaId = cteOwnedSchemaObject.SchemaId
    WHERE   OwnedSchema.SchemaPrincipalType != 'DATABASE_ROLE';


-- Query 4:
-- Objects for each schema owned by the database users
SELECT  @@SERVERNAME AS 'ServerName' ,
        DatabaseName ,
        OwnerName ,
        SchemaName ,
        ObjectName ,
        ObjectType ,
        '-- Objects for each schema owned by the database users' AS 'Description'
FROM    #OwnedSchemaObject;


-- Query 5:
-- SQL Agent Jobs owned by the server login
-- Change these or the job will fail to run when the server login has been deleted
SELECT  @@SERVERNAME AS 'ServerName' ,
        sj.name AS 'JobName' ,
        '-- SQL Agent Jobs owned by the server login' AS 'Description' ,
        'execute msdb.dbo.sp_update_job @job_id = '''
        + CONVERT(CHAR(36), sj.job_id) + ''', @owner_login_name=N''sa'';' AS 'HelperScript'
FROM    msdb.dbo.sysjobs AS sj
        INNER JOIN sys.server_principals AS sp ON sj.owner_sid = sp.sid
WHERE   sp.name = @PrincipalName;


DROP TABLE #OwnedSchema;
DROP TABLE #OwnedSchemaObject;
DROP TABLE #UserAccess;

GO
