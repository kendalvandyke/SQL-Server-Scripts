-- Impersonate domain login
EXECUTE AS LOGIN = 'DOMAIN\account'

-- Server permissions
SELECT *
FROM fn_my_permissions(NULL, 'SERVER');

-- Datbase permissions
SELECT *
FROM fn_my_permissions(NULL, 'DATABASE');

REVERT;

/* https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/xp-logininfo-transact-sql */
EXECUTE xp_logininfo @acctname = 'DOMAIN\account'
	, @option = 'all';
