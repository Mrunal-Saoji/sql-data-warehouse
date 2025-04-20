/*
=============================================================
Drop and Recreate Schema bronze, silver and gold
=============================================================
WARNING:
	Running this script will drop the schema if it exits.
	All data in the schema will be permanently deleted. Proceed with caution
	and ensure you have proper backups before running the script.
*/


DROP SCHEMA IF EXISTS bronze;
CREATE SCHEMA bronze;

DROP SCHEMA IF EXISTS silver;
CREATE SCHEMA silver;

DROP SCHEMA IF EXISTS gold;
CREATE SCHEMA gold;
