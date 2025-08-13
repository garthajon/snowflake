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
    WHEN IS_DATABASE_ROLE_IN_SESSION('DB_ROLE_PROJECT2') AND USE_CONCEPT_ID = 'HomePhone' THEN TRUE
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

USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE DATABASE ROLE DB_ROLE_PROJECT2;
DROP SHARE IF EXISTS PROJECT1_SHARE;
CREATE OR REPLACE SHARE  PROJECT1_SHARE;
-- rather than granting access to table and schema directly to the share
-- we grant the database role access to the table and schema
GRANT SELECT ON TABLE PRODUCER_SHARE_COMPASS.PUBLIC.PATIENT_CONTACT TO DATABASE ROLE DB_ROLE_PROJECT2;
GRANT USAGE ON  SCHEMA PRODUCER_SHARE_COMPASS.PUBLIC TO DATABASE ROLE DB_ROLE_PROJECT2;
GRANT USAGE ON DATABASE PRODUCER_SHARE_COMPASS TO SHARE  PROJECT1_SHARE;
-- having granted the database role access to the table and schema
-- we now grant the database role to the share meaning that a user on the consumer side
-- can use the database role to access the table and schema
-- but that access will only be permitted if one of user's primary function roles
--  is assigned this 'commuted' secondary database role from the producer side
GRANT DATABASE ROLE DB_ROLE_PROJECT2 TO SHARE PROJECT1_SHARE;
ALTER SHARE PROJECT1_SHARE ADD ACCOUNTS = KV29405;


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

-- TESTING WHICH ROLE HAS ACCESS TO THE POLICY
-- RECALL THAT ONLY PRIMARY FUNCTIONAL ROLES WHICH WE HAVE EXPLICITLY GRANTED THE SECONDARY DATABASE ROLE TO SHOULD BE ABLE ACCESS THE TABLE WITH THR ROW LEVEL 
-- ACCESS POLICY ON IT

  -- recall that this is the row level access policy definition (on the producer side of the share) which is limiting access to table Patient_Contact
  -- to those primary functional roles who have been granted permission to the secondary database role which applicable/commuted across the share
  -- note that by default an admin role WILL have access to the secondary database role which has been commuted across the share
  -- and that makes sense because admin it this role which will need to grant access to other roles/users
  -- i say that an admin user can 'see' the secondary database role or 'access it' along with the phrase 'the secondary database is active for the admin role'
  -- synomously: these phrases mean the same thing, but snowflake and copilot tend to speak of a secondary database role being active against a primary role (or not)

  ALTER ROW ACCESS POLICY PROJECT1_PHONE_ACCESS_POLICY
SET BODY ->
  CASE
    WHEN IS_DATABASE_ROLE_IN_SESSION('DB_ROLE_PROJECT2') AND USE_CONCEPT_ID = 'HomePhone' THEN TRUE
    ELSE FALSE
  END;


-- ************IMPORTANT - WE NEED TO REMOVE ACCESS TO THE DATABASE ROLE FOR ALL ROLES AGAINST THE CURRENT USER USING REVOKE ON THE DATABASE ROLE*************************
-- *** THIS IS ALL ON THE CONSUMER SIDE OF THE SHARE
-- list all available roles against the current user
show roles;

-- using an account admin revoke permissions from the database role for all possible primary functional roles against the user
-- for a user who has both admin and non admin roles (as the case here with my user in the free test account)
-- we need to explicitly revoke permissions from the database role secondary role from ALL primary roles and restart snowflake - log out/log back in
-- because the fact of my test user account having both admin and non admin primary roles, it seems that all primary roles by default with such a 'mixed' role account
-- have access to the secondary database role which has been commuted across the share
-- which means (by defaault) that all primary roles against this account will be able to 'see' the shared database role and thus prositively trigger the row level access policy
use role accountadmin;
REVOKE DATABASE ROLE DB_ROLE_PROJECT2 FROM ROLE ACCOUNTADMIN;
REVOKE DATABASE ROLE DB_ROLE_PROJECT2 FROM ROLE  DB_ROLE_PROJECT1;
REVOKE DATABASE ROLE DB_ROLE_PROJECT2 FROM ROLE  ORGADMIN;
REVOKE DATABASE ROLE DB_ROLE_PROJECT2 FROM ROLE  PUBLIC;
REVOKE DATABASE ROLE DB_ROLE_PROJECT2 FROM ROLE  RESEARCH_ROLE;
REVOKE DATABASE ROLE DB_ROLE_PROJECT2 FROM ROLE  RESEARCH_ROLE2;
REVOKE DATABASE ROLE DB_ROLE_PROJECT2 FROM ROLE  SECURITYADMIN;
REVOKE DATABASE ROLE DB_ROLE_PROJECT2 FROM ROLE  SNOWFLAKE_LEARNING_ROLE;
REVOKE DATABASE ROLE DB_ROLE_PROJECT2 FROM ROLE  SYSADMIN;
REVOKE DATABASE ROLE DB_ROLE_PROJECT2 FROM ROLE  USERADMIN;

-- check current primary role which is active in this session, most permissions should be directed off this primary role
select current_role()

-- change primary role to be an account admin (which will automatically have permissions on the database role - a secondary role)
use role accountadmin;
-- check is database role  active in the current session (as a secondary role)
SELECT IS_DATABASE_ROLE_IN_SESSION('DB_ROLE_PROJECT2');
SELECT COUNT(*) AS COUNTALL, USE_CONCEPT_ID FROM PATIENT_CONTACT
GROUP BY USE_CONCEPT_ID

-- change primary role to be a role which does not have admin permissions 
use role db_role_project1;
-- check is database role  active in the current session (as a secondary role)
SELECT IS_DATABASE_ROLE_IN_SESSION('DB_ROLE_PROJECT2');
SELECT COUNT(*) AS COUNTALL, USE_CONCEPT_ID FROM PATIENT_CONTACT
GROUP BY USE_CONCEPT_ID


-- change primary role to be a role which does not have admin permissions 
use role research_role2;
-- check is database role  active in the current session (as a secondary role)
SELECT IS_DATABASE_ROLE_IN_SESSION('DB_ROLE_PROJECT2');
SELECT COUNT(*) AS COUNTALL, USE_CONCEPT_ID FROM PATIENT_CONTACT
GROUP BY USE_CONCEPT_ID

--***granting permission to the secondary database role which has been commuted across the share from the producer account****
--***for an existing prinary functional role which does not currently have access to that secondary database role
--*** and therefore this  secondary database role will never currently be 'active' in session for this primary functional role
--*** but if we grant permission to this secondary database role to this primary functional role
--*** the secondary database role should become active and then under that primary functional role the user should then be able to 'see'
--*** the share and the RLAC (row level access policy) should also apply for this primary functional role in addition to the primary functional
--*** admin role

-- change primary role to be a role which does not have admin permissions 
use role db_role_project1;
-- check is database role  active in the current session (as a secondary role)
SELECT IS_DATABASE_ROLE_IN_SESSION('DB_ROLE_PROJECT2');
-- next check whether or not we can query the 'shared' dataset which has had the role level access policy applied to it
SELECT COUNT(*) AS COUNTALL, USE_CONCEPT_ID FROM PATIENT_CONTACT
GROUP BY USE_CONCEPT_ID

-- [switch to admin role to grant  permission to functional role which does not have access to the share currently]
use role accountadmin;
use database producer_share_compass;
-- grant the commuted secondary database role (via the share) to teh primary functional role which cannot currently access that
-- secondary secondary database role
GRANT DATABASE ROLE DB_ROLE_PROJECT2 TO ROLE  DB_ROLE_PROJECT1;

-- now you should see that the functional which did not have permission to the secondary database role via the share now has permission
use role db_role_project1;
USE WAREHOUSE snowflake_learning_wh;
-- check is database role  active in the current session (as a secondary role)
SELECT IS_DATABASE_ROLE_IN_SESSION('DB_ROLE_PROJECT2');
-- next check whether or not we can query the 'shared' dataset which has had the role level access policy applied to it
SELECT COUNT(*) AS COUNTALL, USE_CONCEPT_ID FROM PATIENT_CONTACT
GROUP BY USE_CONCEPT_ID


























