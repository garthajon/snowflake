-- Testing credit used per query for a table with a row level access policy on it vs a similar table without

-- on producer side of the share
-- grant permissions to a 2nd version of the patient contact table which does not have a 
-- row level access policy on it (permission is granted to a database role)
-- as access to objects is still being controlled via a database role across the share
-- which is considered a best practice
-- also note that a primary (functional) role is granted permission to the database role on the consumer side
-- (this has happened already - we're just amended access rights to this additional table)
GRANT SELECT ON TABLE PRODUCER_SHARE_COMPASS.PUBLIC.PATIENT_CONTACT2 TO DATABASE ROLE DB_ROLE_PROJECT2;
-- run this on the producer side to check which row level access policies have been applied
-- note that this will not work on the consumer side of the share
use role accountadmin;
SHOW ROW ACCESS POLICIES;
-- also the following describes the row level access policies applied to the table
DESCRIBE ROW ACCESS POLICY PROJECT1_PHONE_ACCESS_POLICY;

-- on the consumer side of the share we start to review the differences between tables with and without row level access policies
-- group by query for table with row level access

use role db_role_project1;
SELECT COUNT(*) AS COUNTALL, USE_CONCEPT_ID FROM PATIENT_CONTACT
GROUP BY USE_CONCEPT_ID

-- group by query for table without row level access
use role db_role_project1;
use warehouse snowflake_learning_wh;
SELECT COUNT(*) AS COUNTALL, USE_CONCEPT_ID FROM PATIENT_CONTACT2
GROUP BY USE_CONCEPT_ID

 -- tag the queries such that the query history can be used to compare the credit usage
-- group by query for table with row level access
use role db_role_project1;
ALTER SESSION SET QUERY_TAG = 'PATIENT_CONTACT WITH RLAC';
SELECT COUNT(*) AS COUNTALL, USE_CONCEPT_ID FROM PATIENT_CONTACT
GROUP BY USE_CONCEPT_ID;
-- RESET TAGGING TO BLANK
ALTER SESSION SET QUERY_TAG = '';

-- group by query for table without row level access
use role db_role_project1;
use warehouse snowflake_learning_wh;
ALTER SESSION SET QUERY_TAG = 'PATIENT_CONTACT WITHOUT RLAC';
SELECT COUNT(*) AS COUNTALL, USE_CONCEPT_ID FROM PATIENT_CONTACT2
GROUP BY USE_CONCEPT_ID;

-- RESET TAGGING TO BLANK
ALTER SESSION SET QUERY_TAG = '';

-- RESET TAGGING TO BLANK
ALTER SESSION SET QUERY_TAG = '';

-- A MORE COMPLEX QUERY TO TEST CREDIT USAGE (WITH A JOIN)

-- group by query for table with row level access
use role db_role_project1;
use warehouse snowflake_learning_wh;
ALTER SESSION SET QUERY_TAG = 'PATIENT_CONTACT WITH RLAC';
SELECT COUNT(*) AS COUNTALL, PATIENT_CONTACT.USE_CONCEPT_ID FROM PATIENT_CONTACT
LEFT JOIN PATIENT_CONTACT AS PAT2 ON PATIENT_CONTACT.LDS_RECORD_ID = PAT2.LDS_RECORD_ID 
full outer join  PATIENT_CONTACT AS PAT3 ON PATIENT_CONTACT.LDS_RECORD_ID = PAT3.LDS_RECORD_ID 
and pat3.use_concept_id = 'HomePhone' and pat3.lds_is_deleted = 'FALSE'
full outer join  PATIENT_CONTACT AS PAT4 ON PATIENT_CONTACT.LDS_RECORD_ID = PAT4.LDS_RECORD_ID 
and pat4.use_concept_id = 'HomePhone' and pat4.lds_is_deleted = 'FALSE'
RIGHT outer join  PATIENT_CONTACT AS PAT5 ON PATIENT_CONTACT.LDS_RECORD_ID = PAT5.LDS_RECORD_ID 
and pat5.use_concept_id = 'HomePhone' and pat5.lds_is_deleted = 'FALSE'
INNER JOIN   PATIENT_CONTACT AS PAT6 ON PATIENT_CONTACT.LDS_RECORD_ID = PAT6.LDS_RECORD_ID 
AND (SUBSTR(PAT6.LDS_BUSINESS_KEY,2,1) = 'A' OR SUBSTR(PAT6.LDS_BUSINESS_KEY,2,1) = 'D'  OR SUBSTR(PAT6.LDS_BUSINESS_KEY,2,1) = 'F')
inner join 
(select distinct pat1.lds_record_id as inner_id from PATIENT_CONTACT as pat1,PATIENT_CONTACT as pat2 where SUBSTR(PAT1.LDS_BUSINESS_KEY,2,1) = 'A' 
and  (SUBSTR(PAT2.LDS_BUSINESS_KEY,3,1) <> 'B' OR SUBSTR(PAT2.LDS_BUSINESS_KEY,3,1) <> 'X' OR SUBSTR(PAT2.LDS_BUSINESS_KEY,3,1) <> 'Y')) as inner_cartesian_final
on inner_cartesian_final.inner_id = PATIENT_CONTACT.LDS_RECORD_ID 
GROUP BY PATIENT_CONTACT.USE_CONCEPT_ID;
-- RESET TAGGING TO BLANK
ALTER SESSION SET QUERY_TAG = '';

-- group by query for table without row level access
use role db_role_project1;
use warehouse snowflake_learning_wh;
ALTER SESSION SET QUERY_TAG = 'PATIENT_CONTACT2 WITHOUT RLAC';
SELECT COUNT(*) AS COUNTALL, PATIENT_CONTACT2.USE_CONCEPT_ID FROM PATIENT_CONTACT2
LEFT JOIN PATIENT_CONTACT2 AS PAT2 ON PATIENT_CONTACT2.LDS_RECORD_ID = PAT2.LDS_RECORD_ID 
full outer join  PATIENT_CONTACT2 AS PAT3 ON PATIENT_CONTACT2.LDS_RECORD_ID = PAT3.LDS_RECORD_ID 
and pat3.use_concept_id = 'HomePhone' and pat3.lds_is_deleted = 'FALSE'
full outer join  PATIENT_CONTACT2 AS PAT4 ON PATIENT_CONTACT2.LDS_RECORD_ID = PAT4.LDS_RECORD_ID 
and pat4.use_concept_id = 'HomePhone' and pat4.lds_is_deleted = 'FALSE'
RIGHT outer join  PATIENT_CONTACT2 AS PAT5 ON PATIENT_CONTACT2.LDS_RECORD_ID = PAT5.LDS_RECORD_ID 
and pat5.use_concept_id = 'HomePhone' and pat5.lds_is_deleted = 'FALSE'
INNER JOIN   PATIENT_CONTACT2 AS PAT6 ON PATIENT_CONTACT2.LDS_RECORD_ID = PAT6.LDS_RECORD_ID 
AND (SUBSTR(PAT6.LDS_BUSINESS_KEY,2,1) = 'A' OR SUBSTR(PAT6.LDS_BUSINESS_KEY,2,1) = 'D'  OR SUBSTR(PAT6.LDS_BUSINESS_KEY,2,1) = 'F')
inner join 
(select distinct pat1.lds_record_id as inner_id from PATIENT_CONTACT2 as pat1,PATIENT_CONTACT2 as pat2 where SUBSTR(PAT1.LDS_BUSINESS_KEY,2,1) = 'A' 
and  (SUBSTR(PAT2.LDS_BUSINESS_KEY,3,1) <> 'B' OR SUBSTR(PAT2.LDS_BUSINESS_KEY,3,1) <> 'X' OR SUBSTR(PAT2.LDS_BUSINESS_KEY,3,1) <> 'Y')) as inner_cartesian_final
on inner_cartesian_final.inner_id = PATIENT_CONTACT2.LDS_RECORD_ID 
GROUP BY PATIENT_CONTACT2.USE_CONCEPT_ID;

-- RESET TAGGING TO BLANK
ALTER SESSION SET QUERY_TAG = '';


-- RESET TAGGING TO BLANK
ALTER SESSION SET QUERY_TAG = '';

-- A MORE COMPLEX QUERY TO TEST CREDIT USAGE (WITH A JOIN)

-- group by query for table with row level access
use role db_role_project1;
use warehouse snowflake_learning_wh;
ALTER SESSION SET QUERY_TAG = 'OBSERVATIONS WITH RLAC';
SELECT COUNT(*) AS COUNTALL, OBSERVATION.ORGANIZATION_ID FROM OBSERVATION
LEFT JOIN OBSERVATION AS OBS2 ON OBSERVATION.LDS_RECORD_ID = OBS2.LDS_RECORD_ID 
full outer join  OBSERVATION AS OBS3 ON OBSERVATION.LDS_RECORD_ID = OBS3.LDS_RECORD_ID 
and OBS3.ORGANIZATION_ID = 'A00005' and OBS3.lds_is_deleted = 'FALSE'
full outer join  OBSERVATION AS OBS4 ON OBSERVATION.LDS_RECORD_ID = OBS4.LDS_RECORD_ID 
and OBS4.ORGANIZATION_ID = 'A00005' and OBS4.lds_is_deleted = 'FALSE'
RIGHT outer join  OBSERVATION AS OBS5 ON OBSERVATION.LDS_RECORD_ID = OBS5.LDS_RECORD_ID 
and OBS5.ORGANIZATION_ID = 'A00005' and OBS5.lds_is_deleted = 'FALSE'
INNER JOIN   OBSERVATION AS OBS6 ON OBSERVATION.LDS_RECORD_ID = OBS6.LDS_RECORD_ID 
AND (SUBSTR(OBS6.LDS_BUSINESS_KEY,2,1) = 'A' OR SUBSTR(OBS6.LDS_BUSINESS_KEY,2,1) = 'D'  OR SUBSTR(OBS6.LDS_BUSINESS_KEY,2,1) = 'F')
inner join 
(select distinct OBS1.lds_record_id as inner_id from OBSERVATION as OBS1,OBSERVATION as OBS2 where SUBSTR(OBS1.LDS_BUSINESS_KEY,2,1) = 'A' 
and  (SUBSTR(OBS2.LDS_BUSINESS_KEY,3,1) <> 'B' OR SUBSTR(OBS2.LDS_BUSINESS_KEY,3,1) <> 'X' OR SUBSTR(OBS2.LDS_BUSINESS_KEY,3,1) <> 'Y')) as inner_cartesian_final
on inner_cartesian_final.inner_id = OBSERVATION.LDS_RECORD_ID 
GROUP BY OBSERVATION.ORGANIZATION_ID;
-- RESET TAGGING TO BLANK
ALTER SESSION SET QUERY_TAG = '';

-- group by query for table without row level access
use role db_role_project1;
use warehouse snowflake_learning_wh;
ALTER SESSION SET QUERY_TAG = 'OBSERVATIONS WITHOUT RLAC';
SELECT COUNT(*) AS COUNTALL, OBSERVATION2.ORGANIZATION_ID FROM OBSERVATION2
LEFT JOIN OBSERVATION2 AS OBS2 ON OBSERVATION2.LDS_RECORD_ID = OBS2.LDS_RECORD_ID 
full outer join  OBSERVATION2 AS OBS3 ON OBSERVATION2.LDS_RECORD_ID = OBS3.LDS_RECORD_ID 
and OBS3.ORGANIZATION_ID = 'A00005' and OBS3.lds_is_deleted = 'FALSE'
full outer join  OBSERVATION2 AS OBS4 ON OBSERVATION2.LDS_RECORD_ID = OBS4.LDS_RECORD_ID 
and OBS4.ORGANIZATION_ID = 'A00005' and OBS4.lds_is_deleted = 'FALSE'
RIGHT outer join  OBSERVATION2 AS OBS5 ON OBSERVATION2.LDS_RECORD_ID = OBS5.LDS_RECORD_ID 
and OBS5.ORGANIZATION_ID = 'A00005' and OBS5.lds_is_deleted = 'FALSE'
INNER JOIN   OBSERVATION2 AS OBS6 ON OBSERVATION2.LDS_RECORD_ID = OBS6.LDS_RECORD_ID 
AND (SUBSTR(OBS6.LDS_BUSINESS_KEY,2,1) = 'A' OR SUBSTR(OBS6.LDS_BUSINESS_KEY,2,1) = 'D'  OR SUBSTR(OBS6.LDS_BUSINESS_KEY,2,1) = 'F')
inner join 
(select distinct OBS1.lds_record_id as inner_id from OBSERVATION2 as OBS1,OBSERVATION2 as OBS2 where SUBSTR(OBS1.LDS_BUSINESS_KEY,2,1) = 'A' 
and  (SUBSTR(OBS2.LDS_BUSINESS_KEY,3,1) <> 'B' OR SUBSTR(OBS2.LDS_BUSINESS_KEY,3,1) <> 'X' OR SUBSTR(OBS2.LDS_BUSINESS_KEY,3,1) <> 'Y')) as inner_cartesian_final
on inner_cartesian_final.inner_id = OBSERVATION2.LDS_RECORD_ID 
GROUP BY OBSERVATION2.ORGANIZATION_ID;
-- RESET TAGGING TO BLANK
ALTER SESSION SET QUERY_TAG = '';


-- Now we can run a query to check the credits used by these queries

use role accountadmin;
use warehouse snowflake_learning_wh;
WITH query_costs AS (
SELECT DISTINCT
q.execution_status,
Q.COMPILATION_TIME,
Q.EXECUTION_TIME,
Q.CREDITS_USED_CLOUD_SERVICES,
--q.error_code,
Q.QUERY_TYPE,
q.query_id,
q.query_text,
q.start_time,
q.end_time,
q.query_tag,
w.credits_used_compute AS compute_credits
FROM
snowflake.account_usage.query_history q
JOIN
snowflake.account_usage.warehouse_metering_history w
ON
q.warehouse_id = w.warehouse_id
WHERE
q.start_time >= DATEADD(DAY, -2, CURRENT_DATE) -- Last 2 days
)


SELECT 
query_id,
START_TIME,
query_text,
query_tag,
execution_status,
--error_code,
MAX(compute_credits) AS MAX_COMPUTE_CREDITS,
COMPILATION_TIME,
EXECUTION_TIME,
CREDITS_USED_CLOUD_SERVICES,
TIMESTAMPDIFF('milliseconds', start_time, end_time) AS execution_time_milliseconds
FROM
query_costs
where (query_tag like '%OBSERVATIONS%' OR query_tag like '%PATIENT_CONTACT%')
AND QUERY_TEXT LIKE '%COUNT%'
AND QUERY_TEXT  LIKE '%inner_cartesian_final%'
AND execution_status = 'SUCCESS'
AND QUERY_TYPE = 'SELECT'
GROUP BY 
query_id,
START_TIME,
query_text,
query_tag,
execution_status,
COMPILATION_TIME,
EXECUTION_TIME,
CREDITS_USED_CLOUD_SERVICES,
TIMESTAMPDIFF('milliseconds', start_time, end_time)
ORDER BY
QUERY_TAG DESC,
START_TIME DESC
LIMIT 20;

-- RESET TAGGING TO BLANK
ALTER SESSION SET QUERY_TAG = '';

-- A MORE COMPLEX QUERY TO TEST CREDIT USAGE (WITH A JOIN)

-- group by query for table with row level access
use role db_role_project1;
use warehouse snowflake_learning_wh;
ALTER SESSION SET QUERY_TAG = 'OBSERVATIONS WITH RLAC';
SELECT COUNT(*) AS COUNTALL, OBSERVATION.ORGANIZATION_ID FROM OBSERVATION
LEFT JOIN OBSERVATION AS OBS2 ON OBSERVATION.LDS_RECORD_ID = OBS2.LDS_RECORD_ID 
full outer join  OBSERVATION AS OBS3 ON OBSERVATION.LDS_RECORD_ID = OBS3.LDS_RECORD_ID 
and OBS3.ORGANIZATION_ID = 'A00005' and OBS3.lds_is_deleted = 'FALSE'
full outer join  OBSERVATION AS OBS4 ON OBSERVATION.LDS_RECORD_ID = OBS4.LDS_RECORD_ID 
and OBS4.ORGANIZATION_ID = 'A00005' and OBS4.lds_is_deleted = 'FALSE'
RIGHT outer join  OBSERVATION AS OBS5 ON OBSERVATION.LDS_RECORD_ID = OBS5.LDS_RECORD_ID 
and OBS5.ORGANIZATION_ID = 'A00005' and OBS5.lds_is_deleted = 'FALSE'
INNER JOIN   OBSERVATION AS OBS6 ON OBSERVATION.LDS_RECORD_ID = OBS6.LDS_RECORD_ID 
AND (SUBSTR(OBS6.LDS_BUSINESS_KEY,2,1) = 'A' OR SUBSTR(OBS6.LDS_BUSINESS_KEY,2,1) = 'D'  OR SUBSTR(OBS6.LDS_BUSINESS_KEY,2,1) = 'F')
FULL OUTER join 
(select distinct OBS1.lds_record_id as inner_id from OBSERVATION as OBS1,OBSERVATION as OBS2 where SUBSTR(OBS1.LDS_BUSINESS_KEY,2,1) = 'A' 
and  (SUBSTR(OBS2.LDS_BUSINESS_KEY,3,1) <> 'B' OR SUBSTR(OBS2.LDS_BUSINESS_KEY,3,1) <> 'X' OR SUBSTR(OBS2.LDS_BUSINESS_KEY,3,1) <> 'Y')) as inner_cartesian_final
on inner_cartesian_final.inner_id = OBSERVATION.LDS_RECORD_ID 
FULL OUTER join 
(select distinct OBS1.lds_record_id as inner_id from OBSERVATION as OBS1,OBSERVATION as OBS2 where SUBSTR(OBS1.LDS_BUSINESS_KEY,2,1) = 'P' 
and  (SUBSTR(OBS2.LDS_BUSINESS_KEY,3,1) = 'Q' OR SUBSTR(OBS2.LDS_BUSINESS_KEY,3,1) = 'R' OR SUBSTR(OBS2.LDS_BUSINESS_KEY,3,1) <> 'T')) as inner_cartesian_final2
on inner_cartesian_final2.inner_id = OBSERVATION.LDS_RECORD_ID 
FULL OUTER join 
(select distinct OBS1.lds_record_id as inner_id from OBSERVATION as OBS1,OBSERVATION as OBS2 where SUBSTR(OBS1.LDS_BUSINESS_KEY,2,1) = 'S' 
and  (SUBSTR(OBS2.LDS_BUSINESS_KEY,3,1) = 'W' OR SUBSTR(OBS2.LDS_BUSINESS_KEY,3,1) = 'R' OR SUBSTR(OBS2.LDS_BUSINESS_KEY,3,1) = 'Y')) as inner_cartesian_final3
on inner_cartesian_final3.inner_id = OBSERVATION.LDS_RECORD_ID 
GROUP BY OBSERVATION.ORGANIZATION_ID;
-- RESET TAGGING TO BLANK
ALTER SESSION SET QUERY_TAG = '';

-- group by query for table without row level access
use role db_role_project1;
use warehouse snowflake_learning_wh;
ALTER SESSION SET QUERY_TAG = 'OBSERVATIONS WITHOUT RLAC';
SELECT COUNT(*) AS COUNTALL, OBSERVATION2.ORGANIZATION_ID FROM OBSERVATION2
LEFT JOIN OBSERVATION2 AS OBS2 ON OBSERVATION2.LDS_RECORD_ID = OBS2.LDS_RECORD_ID 
full outer join  OBSERVATION2 AS OBS3 ON OBSERVATION2.LDS_RECORD_ID = OBS3.LDS_RECORD_ID 
and OBS3.ORGANIZATION_ID = 'A00005' and OBS3.lds_is_deleted = 'FALSE'
full outer join  OBSERVATION2 AS OBS4 ON OBSERVATION2.LDS_RECORD_ID = OBS4.LDS_RECORD_ID 
and OBS4.ORGANIZATION_ID = 'A00005' and OBS4.lds_is_deleted = 'FALSE'
RIGHT outer join  OBSERVATION2 AS OBS5 ON OBSERVATION2.LDS_RECORD_ID = OBS5.LDS_RECORD_ID 
and OBS5.ORGANIZATION_ID = 'A00005' and OBS5.lds_is_deleted = 'FALSE'
INNER JOIN   OBSERVATION2 AS OBS6 ON OBSERVATION2.LDS_RECORD_ID = OBS6.LDS_RECORD_ID 
AND (SUBSTR(OBS6.LDS_BUSINESS_KEY,2,1) = 'A' OR SUBSTR(OBS6.LDS_BUSINESS_KEY,2,1) = 'D'  OR SUBSTR(OBS6.LDS_BUSINESS_KEY,2,1) = 'F')
FULL OUTER join 
(select distinct OBS1.lds_record_id as inner_id from OBSERVATION2 as OBS1,OBSERVATION2 as OBS2 where SUBSTR(OBS1.LDS_BUSINESS_KEY,2,1) = 'A' 
and  (SUBSTR(OBS2.LDS_BUSINESS_KEY,3,1) <> 'B' OR SUBSTR(OBS2.LDS_BUSINESS_KEY,3,1) <> 'X' OR SUBSTR(OBS2.LDS_BUSINESS_KEY,3,1) <> 'Y')) as inner_cartesian_final
on inner_cartesian_final.inner_id = OBSERVATION2.LDS_RECORD_ID 
FULL OUTER join 
(select distinct OBS1.lds_record_id as inner_id from OBSERVATION2 as OBS1,OBSERVATION2 as OBS2 where SUBSTR(OBS1.LDS_BUSINESS_KEY,2,1) = 'P' 
and  (SUBSTR(OBS2.LDS_BUSINESS_KEY,3,1) = 'Q' OR SUBSTR(OBS2.LDS_BUSINESS_KEY,3,1) = 'R' OR SUBSTR(OBS2.LDS_BUSINESS_KEY,3,1) <> 'T')) as inner_cartesian_final2
on inner_cartesian_final2.inner_id = OBSERVATION2.LDS_RECORD_ID 
FULL OUTER join 
(select distinct OBS1.lds_record_id as inner_id from OBSERVATION2 as OBS1,OBSERVATION as OBS2 where SUBSTR(OBS1.LDS_BUSINESS_KEY,2,1) = 'S' 
and  (SUBSTR(OBS2.LDS_BUSINESS_KEY,3,1) = 'W' OR SUBSTR(OBS2.LDS_BUSINESS_KEY,3,1) = 'R' OR SUBSTR(OBS2.LDS_BUSINESS_KEY,3,1) = 'Y')) as inner_cartesian_final3
on inner_cartesian_final3.inner_id = OBSERVATION2.LDS_RECORD_ID 
GROUP BY OBSERVATION2.ORGANIZATION_ID;
-- RESET TAGGING TO BLANK
ALTER SESSION SET QUERY_TAG = '';


-- Now we can run a query to check the credits used by these queries

use role accountadmin;
use warehouse snowflake_learning_wh;
WITH query_costs AS (
SELECT DISTINCT
q.execution_status,
Q.COMPILATION_TIME,
Q.EXECUTION_TIME,
Q.CREDITS_USED_CLOUD_SERVICES,
--q.error_code,
Q.QUERY_TYPE,
q.query_id,
q.query_text,
q.start_time,
q.end_time,
q.query_tag,
w.credits_used_compute AS compute_credits
FROM
snowflake.account_usage.query_history q
JOIN
snowflake.account_usage.warehouse_metering_history w
ON
q.warehouse_id = w.warehouse_id
WHERE
q.start_time >= DATEADD(DAY, -2, CURRENT_DATE) -- Last 2 days
)


SELECT 
query_id,
START_TIME,
query_text,
query_tag,
execution_status,
--error_code,
MAX(compute_credits) AS MAX_COMPUTE_CREDITS,
COMPILATION_TIME,
EXECUTION_TIME,
CREDITS_USED_CLOUD_SERVICES,
TIMESTAMPDIFF('milliseconds', start_time, end_time) AS execution_time_milliseconds
FROM
query_costs
where (query_tag like '%OBSERVATIONS%' OR query_tag like '%PATIENT_CONTACT%')
AND QUERY_TEXT LIKE '%COUNT%'
AND QUERY_TEXT  LIKE '%inner_cartesian_final%'
AND execution_status = 'SUCCESS'
AND QUERY_TYPE = 'SELECT'
GROUP BY 
query_id,
START_TIME,
query_text,
query_tag,
execution_status,
COMPILATION_TIME,
EXECUTION_TIME,
CREDITS_USED_CLOUD_SERVICES,
TIMESTAMPDIFF('milliseconds', start_time, end_time)
ORDER BY
QUERY_TAG DESC,
START_TIME DESC
LIMIT 20;


SELECT TOP 10 
query_id,
START_TIME,
query_text,
query_tag,
execution_status,
--error_code,
COMPILATION_TIME,
EXECUTION_TIME,
CREDITS_USED_CLOUD_SERVICES
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
where (query_tag like '%OBSERVATIONS%' OR query_tag like '%PATIENT_CONTACT%')
AND QUERY_TEXT LIKE '%COUNT%'
AND QUERY_TEXT  LIKE '%inner_cartesian_final%'
AND execution_status = 'SUCCESS'
ORDER BY

START_TIME ASC
--AND QUERY_TYPE = 'SELECT'






