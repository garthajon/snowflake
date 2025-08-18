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

-- A MORE COMPLEX QUERY TO TEST CREDIT USAGE (WITH A JOIN)

-- group by query for table with row level access
use role db_role_project1;
ALTER SESSION SET QUERY_TAG = 'PATIENT_CONTACT WITH RLAC';
SELECT COUNT(*) AS COUNTALL, PATIENT_CONTACT.USE_CONCEPT_ID FROM PATIENT_CONTACT
LEFT JOIN PATIENT_CONTACT AS PAT2 ON PATIENT_CONTACT.LDS_RECORD_ID = PAT2.LDS_RECORD_ID 
GROUP BY PATIENT_CONTACT.USE_CONCEPT_ID;
-- RESET TAGGING TO BLANK
ALTER SESSION SET QUERY_TAG = '';

-- group by query for table without row level access
use role db_role_project1;
use warehouse snowflake_learning_wh;
ALTER SESSION SET QUERY_TAG = 'PATIENT_CONTACT WITHOUT RLAC';
SELECT COUNT(*) AS COUNTALL, PATIENT_CONTACT2.USE_CONCEPT_ID FROM PATIENT_CONTACT2
LEFT JOIN PATIENT_CONTACT2 AS PAT2 ON PATIENT_CONTACT2.LDS_RECORD_ID = PAT2.LDS_RECORD_ID 
GROUP BY PATIENT_CONTACT2.USE_CONCEPT_ID;

-- RESET TAGGING TO BLANK
ALTER SESSION SET QUERY_TAG = '';

-- Now we can run a query to check the credits used by these queries

use role accountadmin;
WITH query_costs AS (
SELECT
q.execution_status,
--q.error_code,
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
query_text,
query_tag,
execution_status,
--error_code,
SUM(compute_credits) AS TOTAL_CREDITS,
TIMESTAMPDIFF('milliseconds', start_time, end_time) AS execution_time_milliseconds
FROM
query_costs
where query_tag like '%PATIENT_CONTACT%'
AND QUERY_TEXT LIKE '%COUNT%'
--AND QUERY_TEXT NOT LIKE '%LEFT JOIN%'
AND execution_status = 'SUCCESS'
GROUP BY 
query_id,
query_text,
query_tag,
TIMESTAMPDIFF('milliseconds', start_time, end_time),
execution_status
--error_code
ORDER BY
QUERY_TAG DESC
LIMIT 20;






