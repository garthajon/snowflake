-- create database for sharing data from the producer snowflake account
create database PRODUCER_SHARE_COMPASS

-- firstly create the integration for an AWS S3 bucket/ storage bucket which I have created in AWS
-- the ARN role in the intergration must be the same as the one used in the AWS IAM S3 bucket policy
-- https://us-east-1.console.aws.amazon.com/iam/home?region=eu-north-1#/roles/details/snowflake_access?section=trust_relationships
-- so review the identity and access management (IAM) policy for the S3 bucket policy details
-- note that the STORAGE_ALLOWED_LOCATIONS is not an ARN but the S3 bucket path
-- also note that ARN refers to the Amazon Resource Name which is a unique identifier for AWS resources



CREATE OR REPLACE STORAGE INTEGRATION my_s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::692859908729:role/snowflake_access'
STORAGE_ALLOWED_LOCATIONS = ('s3://garthsfirstbucket/');
--SHOW INTEGRATIONS;

--Note: a storage integration in Snowflake is a global account-level object, 
--not a database or schema object. You will not see it listed under any
-- database or schema in the Snowflake Snowsight GUI

-- next we get the details of the storage integration we have just created in order to
-- extract the unique ids which we will need to enter into the AWS policy details
-- specifically (from the details of the storage integration) we need two values/parameters:
-- STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
-- and these values will need to be entered into the Trust Relationships section of the AWS IAM role
-- policy under the Trust Relationships tab

DESC STORAGE INTEGRATION my_s3_integration;

-- now create the stage which will connect to the AWS S3 bucket
-- note the use of the integration in creating the stage to crate the connection to the AWS S3 bucket
-- also note that unlike the storage integration, the stage is a database object

CREATE OR REPLACE STAGE my_s3_stage
STORAGE_INTEGRATION = my_s3_integration
URL = 's3://garthsfirstbucket/'
FILE_FORMAT = (TYPE = PARQUET);

-- now we can list the contents of the stage to see what is in the S3 bucket
LIST @my_s3_stage;

--  now we can begin importing parquet files from the AWS S3 bucket into Snowflake


-- *************populate data from the Snowflake stage which was created to connect to the AWS S3 bucket
-- note that the following datasets are from the COMPASS data model (GP data) the data model for which may be found here:
-- https://wiki.voror.co.uk/index.php?title=Compass_2.1_Schema_Documentation

-- creating a file format for the parquet files is key because it prevents corrupt characters from stopping the import
-- without the parquet file format, the import will fail if there are any invalid characters in the parquet file

CREATE OR REPLACE FILE FORMAT my_parquet_format TYPE = parquet, REPLACE_INVALID_CHARACTERS = TRUE, BINARY_AS_TEXT = FALSE;

-- begin **********************************patient_contact-inserts make table and copy into *****************

BEGIN
    -- Infer the schema
    CREATE OR REPLACE TEMP TABLE inferred_schema AS 
    SELECT * FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@my_s3_stage/patient_contact-inserts.parquet',
            FILE_FORMAT => 'my_parquet_format'
        )
    );

    -- Generate the CREATE TABLE statement dynamically
    LET create_stmt STRING;

    SELECT 'CREATE OR REPLACE TABLE PATIENT_CONTACT (' ||
           LISTAGG(column_name || ' ' || upper(type), ', ') 
           || ');'
    INTO create_stmt
    FROM inferred_schema;

    -- Execute the CREATE TABLE statement
    EXECUTE IMMEDIATE create_stmt;
END;


DECLARE copy_query STRING;
-- VARIABLE DECLARATION BETWEEN BEGIN AND END USES LET
-- VARIABLE ASSIGNMENT WITHIN BEGIN AND END USES :=
BEGIN  
    LET col_list STRING;
    SELECT LISTAGG('"' || UPPER(column_name) || '"', ', ') INTO :col_list
    FROM TABLE(INFER_SCHEMA(
        LOCATION => '@my_s3_stage/patient_contact-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    )); 
    copy_query := 'COPY INTO patient_contact (' || col_list || ') FROM (SELECT ';
    LET select_list STRING;
    SELECT LISTAGG('$1:' || column_name || '::' || type, ', ') INTO :select_list
    FROM TABLE(INFER_SCHEMA(
        LOCATION => '@my_s3_stage/patient_contact-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    ));  
    copy_query := copy_query || select_list || ' FROM @my_s3_stage/patient_contact-inserts.parquet) FILE_FORMAT = (FORMAT_NAME = ''my_parquet_format'');';
EXECUTE IMMEDIATE :copy_query;
END;
-- RETURN :copy_query;
-- EXECUTE IMMEDIATE :copy_query;

-- end **********************************patient_contact-inserts make table and copy into *****************

-- create a database role which will be used in the Row level access policy for a specific research project
-- to restrict access on the share producer side to specific rows
CREATE OR REPLACE DATABASE ROLE DB_ROLE_PROJECT1;

-- grant permissions to the database role
GRANT USAGE ON DATABASE PRODUCER_SHARE_COMPASS TO DATABASE ROLE DB_ROLE_PROJECT1;
GRANT USAGE ON SCHEMA PRODUCER_SHARE_COMPASS.PUBLIC TO DATABASE ROLE DB_ROLE_PROJECT1;
GRANT SELECT ON TABLE PRODUCER_SHARE_COMPASS.PUBLIC.PATIENT_CONTACT TO DATABASE ROLE DB_ROLE_PROJECT1;
GRANT DATABASE ROLE DB_ROLE_PROJECT1 TO USER GARTHJON;

-- in the row level access policy as applied here the is_role_in_session() function is key
-- because that function will tell you which database_role the query is currently running under
-- so then the RLAC is applied for that role
-- thus the check below asks whether the following database_role is being used: DB_ROLE_PROJECT1
-- if it is then the policy applied for that role 

CREATE OR REPLACE ROW ACCESS POLICY PROJECT1_PHONE_ACCESS_POLICY
AS (USE_CONCEPT_ID STRING) RETURNS BOOLEAN ->
  CASE
    WHEN IS_DATABASE_ROLE_IN_SESSION('DB_ROLE_PROJECT1') AND USE_CONCEPT_ID = 'HomePhone' THEN TRUE
    ELSE FALSE
  END;
-- the RLAC function evaluates for each row and only permits the user querying under the DB role DB_ROLE_PROJECT1 to see rows
-- where the function evaluates to true, in this case only where USE_CONCEPT_ID = 'HomePhone'

-- CHANGED THE ACCESS POLICY BECAUSE THE DATABASE ROLE DOES NOT COMMUTE ACROSS THE SHARE

ALTER ROW ACCESS POLICY PROJECT1_PHONE_ACCESS_POLICY
SET BODY ->
  CASE
    WHEN CURRENT_ACCOUNT() = 'KV29405' AND CURRENT_USER = 'GARTHJON2' AND USE_CONCEPT_ID = 'HomePhone' THEN TRUE
    ELSE FALSE
  END;

-- THE CURRENT ACCOUNT VARIABLE DIDN'T SEEM TO WORK SO RESTRICTING ON USER

ALTER ROW ACCESS POLICY PROJECT1_PHONE_ACCESS_POLICY
SET BODY ->
  CASE
    WHEN  CURRENT_USER IN('GARTHJON2','GARTHJON')  AND USE_CONCEPT_ID = 'HomePhone' THEN TRUE
    ELSE FALSE
  END;

  -- testing by removing user restriction

  ALTER ROW ACCESS POLICY PROJECT1_PHONE_ACCESS_POLICY
SET BODY ->
  CASE
    WHEN  USE_CONCEPT_ID = 'HomePhone' THEN TRUE
    ELSE FALSE
  END;

  -- reverting to the original policy

  ALTER ROW ACCESS POLICY PROJECT1_PHONE_ACCESS_POLICY
SET BODY ->
  CASE
    WHEN IS_DATABASE_ROLE_IN_SESSION('DB_ROLE_PROJECT1') AND USE_CONCEPT_ID = 'HomePhone' THEN TRUE
    ELSE FALSE
  END;

  
ALTER TABLE PATIENT_CONTACT ADD ROW ACCESS POLICY PROJECT1_PHONE_ACCESS_POLICY ON (USE_CONCEPT_ID);

DESCRIBE ROW ACCESS POLICY PROJECT1_PHONE_ACCESS_POLICY;

-- CHECK SELECTION ONLY RETURN ROWS WHERE THE USE_CONCEPT_ID IS 'HomePhone'
SELECT * FROM PATIENT_CONTACT
SELECT COUNT(*) AS COUNTALL, USE_CONCEPT_ID FROM PATIENT_CONTACT
GROUP BY USE_CONCEPT_ID

-- *** *****On the producer side - create and configure the share - begin ********

-- now create the share from the producer snowflake account
-- this share will be used to share the data with the consumer snowflake account
-- the share will include the PATIENT_CONTACT table and the database role DB_ROLE_PROJECT1
-- the share will also include the row level access policy which will restrict access to the PATIENT_CONTACT table
-- to only those rows where the USE_CONCEPT_ID is 'HomePhone' for the DB_ROLE_PROJECT1 role
DROP SHARE IF EXISTS PROJECT1_SHARE;


CREATE OR REPLACE SHARE  PROJECT1_SHARE;
-- grant usage commands are used to add objects to the share
-- the share will include the PATIENT_CONTACT table and the database role DB_ROLE_PROJECT1  
GRANT USAGE ON DATABASE PRODUCER_SHARE_COMPASS TO SHARE PROJECT1_SHARE;
GRANT USAGE ON SCHEMA PRODUCER_SHARE_COMPASS.PUBLIC TO SHARE PROJECT1_SHARE;
-- recall that the PATIENT_CONTACT table has a row level access policy applied to it
-- so when the table is shared, the row level access policy will also be shared
GRANT SELECT ON TABLE PRODUCER_SHARE_COMPASS.PUBLIC.PATIENT_CONTACT TO SHARE PROJECT1_SHARE;
GRANT SELECT ON TABLE PRODUCER_SHARE_COMPASS.PUBLIC.PATIENT_CONTACT2 TO SHARE PROJECT1_SHARE;

-- explicit grant of the database role to the share (on the producer side)
GRANT DATABASE ROLE DB_ROLE_PROJECT1 TO SHARE PROJECT1_SHARE;
-- note that roles are not part of a share privilege model
-- so the database role DB_ROLE_PROJECT1 is not part of the share
-- therefore for the consumer to make use of the database role DB_ROLE_PROJECT1
-- the consumer will need to create the role with the same name in their own consumer account
-- and only then will the database role (and therefore the row level access policy) be applied
-- grant access to the share to the consumer snowflake account
-- accounts are in the same region as the producer snowflake account
-- so no need to specify the region

-- also note AND THIS IS VERY IMPORTANT that the database role must be granted explicit access permission
-- to the share on the producer side or else the consumer will not be able to use the database role
-- and the row level access policy will not work

--whereas the defined database role (which is used  in the access policy) is not 'live'
 --and therefore actioning across the share, the row level policy itself is live and 
 --actioning across the share, but in order for a user on the consumer side to be
 -- subject to the row level access policy the consumer side has to create a database role
-- of the same name and allocate the user to that database role (as the database role
 -- from the producer side although referenced in the policy is not active across the share) 
 -- note the specific account id that you need here is called the account locator
 -- to find the account locator, go to the account settings
 -- or simply run this command in Snowsight: SELECT CURRENT_ACCOUNT();
ALTER SHARE PROJECT1_SHARE ADD ACCOUNTS = KV29405;
-- check details of which privileges have been granted to the share
SHOW GRANTS TO SHARE PROJECT1_SHARE;

-- *** *****On the producer side - create and configure the share - end ********

-- *** *****On the consumer side - create and configure the share - start ********
-- To accept a Snowflake data share on the consumer side, you typically use SQL commands to create a database
--  from the share that has been made available to your account

CREATE OR REPLACE DATABASE PRODUCER_SHARE_COMPASS
FROM SHARE CT32643.PROJECT1_SHARE;
-- note the reference to the producer account locator id: 'CT32643'

-- querying the PATIENT_CONTACT table from the share using an admin role, in spite of the role i'm using being an admin role
-- I cannot see the rows because of the row level access policy applied to the PATIENT_CONTACT table
-- so I need to create roles on the consumer side
-- as I cannot create a database role on the share
-- i create a normal role on the consumer side with the same name as the database role on the producer side
CREATE OR REPLACE ROLE DB_ROLE_PROJECT1;
-- get the details of the current user
--select current_user()
-- grant the role to the current user
GRANT ROLE DB_ROLE_PROJECT1 TO USER GARTHJON2;

-- When you create a database from a share, it's considered imported, and Snowflake restricts how you manage privileges on it.
-- To grant access to a specific role, use:

GRANT IMPORTED PRIVILEGES ON DATABASE PRODUCER_SHARE_COMPASS TO ROLE DB_ROLE_PROJECT1;

-- troubleshooting access to the PATIENT_CONTACT table

-- look at the permissions for role DB_ROLE_PROJECT1 ON the consumer side

SHOW GRANTS TO ROLE DB_ROLE_PROJECT1;

-- RUN THIS ON THE PRODUCER SIDE OF THE SHARE
-- TO SHOW THE DETAIL OF WHICH ROW ACCESS POLICIES ARE IN PLACE IN A DATABASE/TABLE
SHOW ROW ACCESS POLICIES
DESC ROW ACCESS POLICY PROJECT1_PHONE_ACCESS_POLICY


-- exploring permission in the account for roles

-- TEST USING A ROLE WHICH DOES NOT EXPLICITLY HAVE ACCESS TO THE SHARE/DATABASE ROLE ACROSS THE SHARE
USE ROLE db_role_project1;
select top 10 * from information_schema.applicable_roles

USE ROLE db_role_project1;
select top 10 * from information_schema.usage_privileges


SELECT COUNT(*) AS COUNTALL, USE_CONCEPT_ID FROM PATIENT_CONTACT
GROUP BY USE_CONCEPT_ID

SHOW GRANTS TO ROLE SNOWFLAKE_LEARNING_ROLE;

-- Check privileges granted to a specific role
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE NAME = 'SNOWFLAKE_LEARNING_ROLE';

-- Check privileges granted to a specific role
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE NAME != 'SNOWFLAKE_LEARNING_ROLE';
and GRANTED_TO = 'ROLE'
-- GRANTEE RECEIVES THE PRIVILEDGE
ORDER BY GRANTEE_NAME


-- Check privileges granted to a specific role
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE 
--
--NAME != 'SNOWFLAKE_LEARNING_ROLE'
--AND GRANTED_TO = 'ACCOUNTADMIN'
GRANTED_TO = 'ROLE'
AND 
GRANTEE_NAME = 'SNOWFLAKE_LEARNING_ROLE'
-- GRANTEE RECEIVES THE PRIVILEDGE
ORDER BY GRANTEE_NAME


-- Check privileges granted to a specific role
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE 
--
--NAME != 'SNOWFLAKE_LEARNING_ROLE'
--AND GRANTED_TO = 'ACCOUNTADMIN'
GRANTED_TO = 'ROLE'
AND 
GRANTEE_NAME = 'DB_ROLE_PROJECT1'
-- GRANTEE RECEIVES THE PRIVILEDGE
ORDER BY GRANTEE_NAME


  
  select top 10 * from 
  SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS
  where role = 'SNOWFLAKE_LEARNING_ROLE'

  SELECT top 10 *
FROM SNOWFLAKE.ACCOUNT_USAGE.ROLES
WHERE name = 'SNOWFLAKE_LEARNING_ROLE';

SHOW ROLES;

SHOW GRANTS TO ROLE DB_ROLE_PROJECT1;

SHOW GRANTS TO ROLE SNOWFLAKE_LEARNING_ROLE;

SHOW DATABASE ROLES IN PRODUCER_SHARE_COMPASS


-- ***** EVERY NEW ROLE IN SNOWFLAKE AUTOMATICALLY INHERITS THE PERMISSIONS OF THE PUBLIC ROLE
-- ***** THIS COULD CAUSE PROBLEMS IF THE PUBLIC ROLE HAS HIGH ADMINISTRATIVE PRIVILEGES
-- ALSO THE PUBLIC ROLE BY DEFAULT CAN SEE SHARES    
-- SO WE WILL NEED TO RUN A SCRIPT WHICH RESTRICTS THE PUBLIC ROLE 

-- Check privileges granted to  the public role 
-- we can that the databases 'snowflake' and 'snowflake_learning_db'
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE NAME = 'PUBLIC';


-- checking the permissions on the default free account snowflake sample share (for free sample data in snowflake on signing up)
-- does user public have permissions to that share

-- it is generally a good idea to restrict what the PUBLIC role can access in Snowflake—especially
 -- in environments where security, data governance, and least privilege principles are important


SELECT TOP 10 *
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE GRANTED_ON IN ('DATABASE', 'SCHEMA', 'TABLE')
  AND NAME = 'SNOWFLAKE_SAMPLE_DATA'
  -- THE GRANTEE (ie. whom the permission has been granted to) is public
  AND GRANTEE_NAME = 'PUBLIC'

  -- actions for next time 12/08/2025
  -- 1) restrict the scope of the public role
  -- 2) create a new user/role after restricting the public role
  -- 3) assign user (me)  to the new role
  -- 4) test whether the new role can access the share prior to explicitly granting access to the share
  -- 5) test whether the new role once access is granted to the share can also access the row level access policy
  --    hopefully the user will only be able to access the row level access policy if the user is assigned to the database role
  --    which is used in the row level access policy























