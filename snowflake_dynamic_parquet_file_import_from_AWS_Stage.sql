-- *************populate data from the Snowflake stage which was created to connect to the AWS S3 bucket
-- note that the following datasets are from the COMPASS data model (GP data) the data model for which may be found here:
-- https://wiki.voror.co.uk/index.php?title=Compass_2.1_Schema_Documentation

CREATE OR REPLACE FILE FORMAT my_parquet_format TYPE = parquet, REPLACE_INVALID_CHARACTERS = TRUE, BINARY_AS_TEXT = FALSE;

-- list the contents of the stage which have already been created previously (the connection to the amazon S3 web bucket)

-- LIST @my_s3_stage;

-- begin **********************************allergy_intolerance-inserts make table and copy into *****************

BEGIN
    -- Infer the schema
    CREATE OR REPLACE TEMP TABLE inferred_schema AS 
    SELECT * FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@my_s3_stage/allergy_intolerance-inserts.parquet',
            FILE_FORMAT => 'my_parquet_format'
        )
    );

    -- Generate the CREATE TABLE statement dynamically
    LET create_stmt STRING;
    
    SELECT 'CREATE OR REPLACE TABLE ALLERGY_INTOLERANCE (' ||
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
        LOCATION => '@my_s3_stage/allergy_intolerance-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    )); 
    copy_query := 'COPY INTO allergy_intolerance (' || col_list || ') FROM (SELECT ';
    LET select_list STRING;
    SELECT LISTAGG('$1:' || column_name || '::' || type, ', ') INTO :select_list
    FROM TABLE(INFER_SCHEMA(
        LOCATION => '@my_s3_stage/allergy_intolerance-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    ));  
    copy_query := copy_query || select_list || ' FROM @my_s3_stage/allergy_intolerance-inserts.parquet) FILE_FORMAT = (FORMAT_NAME = ''my_parquet_format'');';
EXECUTE IMMEDIATE :copy_query;
END;
-- RETURN :copy_query;
-- EXECUTE IMMEDIATE :copy_query;


-- end **********************************allergy_intolerance-inserts make table and copy into *****************

-- begin **********************************appointment-inserts make table and copy into *****************

BEGIN
    -- Infer the schema
    CREATE OR REPLACE TEMP TABLE inferred_schema AS 
    SELECT * FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@my_s3_stage/appointment-inserts.parquet',
            FILE_FORMAT => 'my_parquet_format'
        )
    );

    -- Generate the CREATE TABLE statement dynamically
    LET create_stmt STRING;
    
    SELECT 'CREATE OR REPLACE TABLE APPOINTMENT (' ||
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
        LOCATION => '@my_s3_stage/appointment-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    )); 
    copy_query := 'COPY INTO appointment (' || col_list || ') FROM (SELECT ';
    LET select_list STRING;
    SELECT LISTAGG('$1:' || column_name || '::' || type, ', ') INTO :select_list
    FROM TABLE(INFER_SCHEMA(
        LOCATION => '@my_s3_stage/appointment-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    ));  
    copy_query := copy_query || select_list || ' FROM @my_s3_stage/appointment-inserts.parquet) FILE_FORMAT = (FORMAT_NAME = ''my_parquet_format'');';
EXECUTE IMMEDIATE :copy_query;
END;
-- RETURN :copy_query;
-- EXECUTE IMMEDIATE :copy_query;


-- end **********************************appointment-inserts make table and copy into *****************

-- begin **********************************diagnostic_order-inserts make table and copy into *****************

BEGIN
    -- Infer the schema
    CREATE OR REPLACE TEMP TABLE inferred_schema AS 
    SELECT * FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@my_s3_stage/diagnostic_order-inserts.parquet',
            FILE_FORMAT => 'my_parquet_format'
        )
    );

    -- Generate the CREATE TABLE statement dynamically
    LET create_stmt STRING;
    
    SELECT 'CREATE OR REPLACE TABLE DIAGNOSTIC_ORDER (' ||
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
        LOCATION => '@my_s3_stage/diagnostic_order-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    )); 
    copy_query := 'COPY INTO diagnostic_order (' || col_list || ') FROM (SELECT ';
    LET select_list STRING;
    SELECT LISTAGG('$1:' || column_name || '::' || type, ', ') INTO :select_list
    FROM TABLE(INFER_SCHEMA(
        LOCATION => '@my_s3_stage/diagnostic_order-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    ));  
    copy_query := copy_query || select_list || ' FROM @my_s3_stage/diagnostic_order-inserts.parquet) FILE_FORMAT = (FORMAT_NAME = ''my_parquet_format'');';
EXECUTE IMMEDIATE :copy_query;
END;
-- RETURN :copy_query;
-- EXECUTE IMMEDIATE :copy_query;


-- end **********************************diagnostic_order-inserts make table and copy into *****************


-- begin **********************************encounter-inserts make table and copy into *****************

BEGIN
    -- Infer the schema
    CREATE OR REPLACE TEMP TABLE inferred_schema AS 
    SELECT * FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@my_s3_stage/encounter-inserts.parquet',
            FILE_FORMAT => 'my_parquet_format'
        )
    );

    -- Generate the CREATE TABLE statement dynamically
    LET create_stmt STRING;
    
    SELECT 'CREATE OR REPLACE TABLE ENCOUNTER (' ||
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
        LOCATION => '@my_s3_stage/encounter-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    )); 
    copy_query := 'COPY INTO encounter (' || col_list || ') FROM (SELECT ';
    LET select_list STRING;
    SELECT LISTAGG('$1:' || column_name || '::' || type, ', ') INTO :select_list
    FROM TABLE(INFER_SCHEMA(
        LOCATION => '@my_s3_stage/encounter-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    ));  
    copy_query := copy_query || select_list || ' FROM @my_s3_stage/encounter-inserts.parquet) FILE_FORMAT = (FORMAT_NAME = ''my_parquet_format'');';
EXECUTE IMMEDIATE :copy_query;
END;
-- RETURN :copy_query;
-- EXECUTE IMMEDIATE :copy_query;


-- end **********************************encounter-inserts make table and copy into *****************


-- begin **********************************episode_of_care-inserts make table and copy into *****************

BEGIN
    -- Infer the schema
    CREATE OR REPLACE TEMP TABLE inferred_schema AS 
    SELECT * FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@my_s3_stage/episode_of_care-inserts.parquet',
            FILE_FORMAT => 'my_parquet_format'
        )
    );

    -- Generate the CREATE TABLE statement dynamically
    LET create_stmt STRING;
    
    SELECT 'CREATE OR REPLACE TABLE EPISODE_OF_CARE (' ||
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
        LOCATION => '@my_s3_stage/episode_of_care-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    )); 
    copy_query := 'COPY INTO episode_of_care (' || col_list || ') FROM (SELECT ';
    LET select_list STRING;
    SELECT LISTAGG('$1:' || column_name || '::' || type, ', ') INTO :select_list
    FROM TABLE(INFER_SCHEMA(
        LOCATION => '@my_s3_stage/episode_of_care-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    ));  
    copy_query := copy_query || select_list || ' FROM @my_s3_stage/episode_of_care-inserts.parquet) FILE_FORMAT = (FORMAT_NAME = ''my_parquet_format'');';
EXECUTE IMMEDIATE :copy_query;
END;
-- RETURN :copy_query;
-- EXECUTE IMMEDIATE :copy_query;


-- end **********************************episode_of_care-inserts make table and copy into *****************


-- begin **********************************flag-inserts make table and copy into *****************

BEGIN
    -- Infer the schema
    CREATE OR REPLACE TEMP TABLE inferred_schema AS 
    SELECT * FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@my_s3_stage/flag-inserts.parquet',
            FILE_FORMAT => 'my_parquet_format'
        )
    );

    -- Generate the CREATE TABLE statement dynamically
    LET create_stmt STRING;
    
    SELECT 'CREATE OR REPLACE TABLE FLAG (' ||
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
        LOCATION => '@my_s3_stage/flag-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    )); 
    copy_query := 'COPY INTO flag (' || col_list || ') FROM (SELECT ';
    LET select_list STRING;
    SELECT LISTAGG('$1:' || column_name || '::' || type, ', ') INTO :select_list
    FROM TABLE(INFER_SCHEMA(
        LOCATION => '@my_s3_stage/flag-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    ));  
    copy_query := copy_query || select_list || ' FROM @my_s3_stage/flag-inserts.parquet) FILE_FORMAT = (FORMAT_NAME = ''my_parquet_format'');';
EXECUTE IMMEDIATE :copy_query;
END;
-- RETURN :copy_query;
-- EXECUTE IMMEDIATE :copy_query;


-- end **********************************flag-inserts make table and copy into *****************


-- begin **********************************location-inserts make table and copy into *****************

BEGIN
    -- Infer the schema
    CREATE OR REPLACE TEMP TABLE inferred_schema AS 
    SELECT * FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@my_s3_stage/location-inserts.parquet',
            FILE_FORMAT => 'my_parquet_format'
        )
    );

    -- Generate the CREATE TABLE statement dynamically
    LET create_stmt STRING;
    
    SELECT 'CREATE OR REPLACE TABLE LOCATION (' ||
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
        LOCATION => '@my_s3_stage/location-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    )); 
    copy_query := 'COPY INTO location (' || col_list || ') FROM (SELECT ';
    LET select_list STRING;
    SELECT LISTAGG('$1:' || column_name || '::' || type, ', ') INTO :select_list
    FROM TABLE(INFER_SCHEMA(
        LOCATION => '@my_s3_stage/location-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    ));  
    copy_query := copy_query || select_list || ' FROM @my_s3_stage/location-inserts.parquet) FILE_FORMAT = (FORMAT_NAME = ''my_parquet_format'');';
EXECUTE IMMEDIATE :copy_query;
END;
-- RETURN :copy_query;
-- EXECUTE IMMEDIATE :copy_query;


-- end **********************************location-inserts make table and copy into *****************


-- begin **********************************medication_order-inserts make table and copy into *****************

BEGIN
    -- Infer the schema
    CREATE OR REPLACE TEMP TABLE inferred_schema AS 
    SELECT * FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@my_s3_stage/medication_order-inserts.parquet',
            FILE_FORMAT => 'my_parquet_format'
        )
    );

    -- Generate the CREATE TABLE statement dynamically
    LET create_stmt STRING;

    SELECT 'CREATE OR REPLACE TABLE MEDICATION_ORDER (' ||
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
        LOCATION => '@my_s3_stage/medication_order-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    )); 
    copy_query := 'COPY INTO medication_order (' || col_list || ') FROM (SELECT ';
    LET select_list STRING;
    SELECT LISTAGG('$1:' || column_name || '::' || type, ', ') INTO :select_list
    FROM TABLE(INFER_SCHEMA(
        LOCATION => '@my_s3_stage/medication_order-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    ));  
    copy_query := copy_query || select_list || ' FROM @my_s3_stage/medication_order-inserts.parquet) FILE_FORMAT = (FORMAT_NAME = ''my_parquet_format'');';
EXECUTE IMMEDIATE :copy_query;
END;
-- RETURN :copy_query;
-- EXECUTE IMMEDIATE :copy_query;


-- end **********************************medication_order-inserts make table and copy into *****************


-- begin **********************************medication_statement-inserts make table and copy into *****************

BEGIN
    -- Infer the schema
    CREATE OR REPLACE TEMP TABLE inferred_schema AS 
    SELECT * FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@my_s3_stage/medication_statement-inserts.parquet',
            FILE_FORMAT => 'my_parquet_format'
        )
    );

    -- Generate the CREATE TABLE statement dynamically
    LET create_stmt STRING;

    SELECT 'CREATE OR REPLACE TABLE MEDICATION_STATEMENT (' ||
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
        LOCATION => '@my_s3_stage/medication_statement-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    )); 
    copy_query := 'COPY INTO medication_statement (' || col_list || ') FROM (SELECT ';
    LET select_list STRING;
    SELECT LISTAGG('$1:' || column_name || '::' || type, ', ') INTO :select_list
    FROM TABLE(INFER_SCHEMA(
        LOCATION => '@my_s3_stage/medication_statement-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    ));  
    copy_query := copy_query || select_list || ' FROM @my_s3_stage/medication_statement-inserts.parquet) FILE_FORMAT = (FORMAT_NAME = ''my_parquet_format'');';
EXECUTE IMMEDIATE :copy_query;
END;
-- RETURN :copy_query;
-- EXECUTE IMMEDIATE :copy_query;


-- end **********************************medication_statement-inserts make table and copy into *****************


-- begin **********************************observation-inserts make table and copy into *****************

BEGIN
    -- Infer the schema
    CREATE OR REPLACE TEMP TABLE inferred_schema AS 
    SELECT * FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@my_s3_stage/observation-inserts.parquet',
            FILE_FORMAT => 'my_parquet_format'
        )
    );

    -- Generate the CREATE TABLE statement dynamically
    LET create_stmt STRING;

    SELECT 'CREATE OR REPLACE TABLE OBSERVATION (' ||
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
        LOCATION => '@my_s3_stage/observation-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    )); 
    copy_query := 'COPY INTO observation (' || col_list || ') FROM (SELECT ';
    LET select_list STRING;
    SELECT LISTAGG('$1:' || column_name || '::' || type, ', ') INTO :select_list
    FROM TABLE(INFER_SCHEMA(
        LOCATION => '@my_s3_stage/observation-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    ));  
    copy_query := copy_query || select_list || ' FROM @my_s3_stage/observation-inserts.parquet) FILE_FORMAT = (FORMAT_NAME = ''my_parquet_format'');';
EXECUTE IMMEDIATE :copy_query;
END;
-- RETURN :copy_query;
-- EXECUTE IMMEDIATE :copy_query;


-- end **********************************observation-inserts make table and copy into *****************

-- begin **********************************organisation-inserts make table and copy into *****************

BEGIN
    -- Infer the schema
    CREATE OR REPLACE TEMP TABLE inferred_schema AS 
    SELECT * FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@my_s3_stage/organisation-inserts.parquet',
            FILE_FORMAT => 'my_parquet_format'
        )
    );

    -- Generate the CREATE TABLE statement dynamically
    LET create_stmt STRING;

    SELECT 'CREATE OR REPLACE TABLE ORGANISATION (' ||
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
        LOCATION => '@my_s3_stage/organisation-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    )); 
    copy_query := 'COPY INTO organisation (' || col_list || ') FROM (SELECT ';
    LET select_list STRING;
    SELECT LISTAGG('$1:' || column_name || '::' || type, ', ') INTO :select_list
    FROM TABLE(INFER_SCHEMA(
        LOCATION => '@my_s3_stage/organisation-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    ));  
    copy_query := copy_query || select_list || ' FROM @my_s3_stage/organisation-inserts.parquet) FILE_FORMAT = (FORMAT_NAME = ''my_parquet_format'');';
EXECUTE IMMEDIATE :copy_query;
END;
-- RETURN :copy_query;
-- EXECUTE IMMEDIATE :copy_query;


-- end **********************************organisation-inserts make table and copy into *****************


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


-- begin *****patient_registered_practitioner_in_role-inserts make table and copy into --------*****************

BEGIN
    -- Infer the schema
    CREATE OR REPLACE TEMP TABLE inferred_schema AS 
    SELECT * FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@my_s3_stage/patient_registered_practitioner_in_role-inserts.parquet',
            FILE_FORMAT => 'my_parquet_format'
        )
    );

    -- Generate the CREATE TABLE statement dynamically
    LET create_stmt STRING;

    SELECT 'CREATE OR REPLACE TABLE PATIENT_REGISTERED_PRACTITIONER_IN_ROLE (' ||
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
        LOCATION => '@my_s3_stage/patient_registered_practitioner_in_role-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    )); 
    copy_query := 'COPY INTO patient_registered_practitioner_in_role (' || col_list || ') FROM (SELECT ';
    LET select_list STRING;
    SELECT LISTAGG('$1:' || column_name || '::' || type, ', ') INTO :select_list
    FROM TABLE(INFER_SCHEMA(
        LOCATION => '@my_s3_stage/patient_registered_practitioner_in_role-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    ));  
    copy_query := copy_query || select_list || ' FROM @my_s3_stage/patient_registered_practitioner_in_role-inserts.parquet) FILE_FORMAT = (FORMAT_NAME = ''my_parquet_format'');';
EXECUTE IMMEDIATE :copy_query;
END;
-- RETURN :copy_query;
-- EXECUTE IMMEDIATE :copy_query;


-- end *******************patient_registered_practitioner_in_role-inserts make table and copy into *****************

-- begin *****procedure_request-inserts make table and copy into --------*****************

BEGIN
    -- Infer the schema
    CREATE OR REPLACE TEMP TABLE inferred_schema AS 
    SELECT * FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@my_s3_stage/procedure_request-inserts.parquet',
            FILE_FORMAT => 'my_parquet_format'
        )
    );

    -- Generate the CREATE TABLE statement dynamically
    LET create_stmt STRING;

    SELECT 'CREATE OR REPLACE TABLE PROCEDURE_REQUEST (' ||
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
        LOCATION => '@my_s3_stage/procedure_request-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    )); 
    copy_query := 'COPY INTO procedure_request (' || col_list || ') FROM (SELECT ';
    LET select_list STRING;
    SELECT LISTAGG('$1:' || column_name || '::' || type, ', ') INTO :select_list
    FROM TABLE(INFER_SCHEMA(
        LOCATION => '@my_s3_stage/procedure_request-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    ));  
    copy_query := copy_query || select_list || ' FROM @my_s3_stage/procedure_request-inserts.parquet) FILE_FORMAT = (FORMAT_NAME = ''my_parquet_format'');';
EXECUTE IMMEDIATE :copy_query;
END;
-- RETURN :copy_query;
-- EXECUTE IMMEDIATE :copy_query;


-- end *******************procedure_request-inserts make table and copy into *****************

-- begin *****referral_request-inserts make table and copy into --------*****************

BEGIN
    -- Infer the schema
    CREATE OR REPLACE TEMP TABLE inferred_schema AS 
    SELECT * FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@my_s3_stage/referral_request-inserts.parquet',
            FILE_FORMAT => 'my_parquet_format'
        )
    );

    -- Generate the CREATE TABLE statement dynamically
    LET create_stmt STRING;

    SELECT 'CREATE OR REPLACE TABLE REFERRAL_REQUEST (' ||
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
        LOCATION => '@my_s3_stage/referral_request-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    )); 
    copy_query := 'COPY INTO referral_request (' || col_list || ') FROM (SELECT ';
    LET select_list STRING;
    SELECT LISTAGG('$1:' || column_name || '::' || type, ', ') INTO :select_list
    FROM TABLE(INFER_SCHEMA(
        LOCATION => '@my_s3_stage/referral_request-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    ));  
    copy_query := copy_query || select_list || ' FROM @my_s3_stage/referral_request-inserts.parquet) FILE_FORMAT = (FORMAT_NAME = ''my_parquet_format'');';
EXECUTE IMMEDIATE :copy_query;
END;
-- RETURN :copy_query;
-- EXECUTE IMMEDIATE :copy_query;


-- end *******************referral_request-inserts make table and copy into *****************


-- begin *****schedule_practitioner_in_role-inserts make table and copy into --------*****************

BEGIN
    -- Infer the schema
    CREATE OR REPLACE TEMP TABLE inferred_schema AS 
    SELECT * FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@my_s3_stage/schedule_practitioner_in_role-inserts.parquet',
            FILE_FORMAT => 'my_parquet_format'
        )
    );

    -- Generate the CREATE TABLE statement dynamically
    LET create_stmt STRING;

    SELECT 'CREATE OR REPLACE TABLE SCHEDULE_PRACTITIONER_IN_ROLE (' ||
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
        LOCATION => '@my_s3_stage/schedule_practitioner_in_role-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    )); 
    copy_query := 'COPY INTO schedule_practitioner_in_role (' || col_list || ') FROM (SELECT ';
    LET select_list STRING;
    SELECT LISTAGG('$1:' || column_name || '::' || type, ', ') INTO :select_list
    FROM TABLE(INFER_SCHEMA(
        LOCATION => '@my_s3_stage/schedule_practitioner_in_role-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    ));  
    copy_query := copy_query || select_list || ' FROM @my_s3_stage/schedule_practitioner_in_role-inserts.parquet) FILE_FORMAT = (FORMAT_NAME = ''my_parquet_format'');';
EXECUTE IMMEDIATE :copy_query;
END;
-- RETURN :copy_query;
-- EXECUTE IMMEDIATE :copy_query;


-- end *******************schedule_practitioner_in_role-inserts make table and copy into *****************


-- begin *****schedule-inserts make table and copy into --------*****************

BEGIN
    -- Infer the schema
    CREATE OR REPLACE TEMP TABLE inferred_schema AS 
    SELECT * FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@my_s3_stage/schedule-inserts.parquet',
            FILE_FORMAT => 'my_parquet_format'
        )
    );

    -- Generate the CREATE TABLE statement dynamically
    LET create_stmt STRING;

    SELECT 'CREATE OR REPLACE TABLE SCHEDULE (' ||
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
        LOCATION => '@my_s3_stage/schedule-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    )); 
    copy_query := 'COPY INTO schedule (' || col_list || ') FROM (SELECT ';
    LET select_list STRING;
    SELECT LISTAGG('$1:' || column_name || '::' || type, ', ') INTO :select_list
    FROM TABLE(INFER_SCHEMA(
        LOCATION => '@my_s3_stage/schedule-inserts.parquet',
        FILE_FORMAT => 'my_parquet_format'
    ));  
    copy_query := copy_query || select_list || ' FROM @my_s3_stage/schedule-inserts.parquet) FILE_FORMAT = (FORMAT_NAME = ''my_parquet_format'');';
EXECUTE IMMEDIATE :copy_query;
END;
-- RETURN :copy_query;
-- EXECUTE IMMEDIATE :copy_query;


-- end *******************schedule-inserts make table and copy into *****************
