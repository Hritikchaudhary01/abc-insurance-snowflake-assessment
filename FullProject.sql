CREATE DATABASE ABC_INSURANCE_1;
CREATE SCHEMA RAW;
CREATE SCHEMA VALIDATED;
CREATE SCHEMA CURATED;
CREATE SCHEMA SECURITY;

use schema raw;

CREATE OR REPLACE TABLE RAW.POLICIES_RAW (
policy_number VARCHAR,
customer_id VARCHAR,
first_name VARCHAR,
last_name VARCHAR,
ssn VARCHAR,
email VARCHAR,
phone VARCHAR,
address VARCHAR,
city VARCHAR,
state VARCHAR,
zip VARCHAR,
policy_type VARCHAR,
effective_date VARCHAR,
expiration_date VARCHAR,
annual_premium VARCHAR,
payment_frequency VARCHAR,
renewal_flag VARCHAR,
policy_status VARCHAR,
marital_status VARCHAR,
agent_id VARCHAR,
agency_name VARCHAR,
agency_region VARCHAR,

-- Metadata columns for auditing
file_name VARCHAR,
load_timestamp TIMESTAMP
);

CREATE OR REPLACE TABLE RAW.CLAIMS_RAW (
raw_data VARIANT,

-- Metadata columns
file_name VARCHAR,
load_timestamp TIMESTAMP
);

CREATE OR REPLACE FILE FORMAT RAW.POLICY_CSV_FORMAT
TYPE = CSV
FIELD_DELIMITER = ','
PARSE_HEADER = TRUE 
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
EMPTY_FIELD_AS_NULL = TRUE
TRIM_SPACE = TRUE
NULL_IF = ('NULL','null','');

CREATE OR REPLACE FILE FORMAT RAW.CLAIM_JSON_FORMAT
TYPE = JSON;

CREATE OR REPLACE STAGE RAW.CLAIM_STAGE
FILE_FORMAT = RAW.CLAIM_JSON_FORMAT;

CREATE OR REPLACE STAGE RAW.POLICY_STAGE
FILE_FORMAT = RAW.POLICY_CSV_FORMAT;

list@claim_stage;

copy into temp_policies
from @policy_stage/raw_layer_incremental_data.csv
file_format=RAW.POLICY_CSV_FORMAT
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

INSERT INTO RAW.POLICIES_RAW
SELECT
    *,
    'METADATA$FILENAME',   -- or METADATA later if needed
    CURRENT_TIMESTAMP
FROM TEMP_POLICIES;

select * from  RAW.claims_raw;

create temp table temp_table like raw.policies_raw;

CREATE OR REPLACE TABLE TEMP_POLICIES (
    policy_number VARCHAR,
    customer_id VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    ssn VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state VARCHAR,
    zip VARCHAR,
    policy_type VARCHAR,
    effective_date VARCHAR,
    expiration_date VARCHAR,
    annual_premium VARCHAR,
    payment_frequency VARCHAR,
    renewal_flag VARCHAR,
    policy_status VARCHAR,
    marital_status VARCHAR,
    agent_id VARCHAR,
    agency_name VARCHAR,
    agency_region VARCHAR
);


COPY INTO RAW.CLAIMS_RAW
(
raw_data,
file_name,
load_timestamp
)
FROM
(
SELECT
$1,
METADATA$FILENAME,
CURRENT_TIMESTAMP
FROM @claim_stage/increment.json
)
FILE_FORMAT = RAW.CLAIM_JSON_FORMAT;

use schema validated;

CREATE OR REPLACE TABLE VALIDATED.POLICIES (
    policy_number VARCHAR NOT NULL,
    customer_id VARCHAR NOT NULL,
    first_name VARCHAR,
    last_name VARCHAR,
    ssn VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state VARCHAR,
    zip VARCHAR,
    policy_type VARCHAR,
    effective_date DATE,
    expiration_date DATE,
    annual_premium NUMBER(12,2),
    payment_frequency VARCHAR,
    renewal_flag VARCHAR,
    policy_status VARCHAR,
    marital_status VARCHAR,
    agent_id VARCHAR,
    agency_name VARCHAR,
    agency_region VARCHAR,

    -- metadata columns
    source_file VARCHAR,
    load_timestamp TIMESTAMP,

    CONSTRAINT pk_policy PRIMARY KEY (policy_number)
);

CREATE OR REPLACE TABLE VALIDATED.CLAIMS (
    claim_id VARCHAR NOT NULL,
    policy_number VARCHAR NOT NULL,
    customer_id VARCHAR NOT NULL,
    incident_date DATE,
    fnol_datetime TIMESTAMP,
    claim_status VARCHAR,
    report_channel VARCHAR,
    total_incurred NUMBER(12,2),
    total_paid NUMBER(12,2),
    city VARCHAR,
    state VARCHAR,
    zip varchar,
    claim_type varchar,

    -- metadata columns
    source_file VARCHAR,
    load_timestamp TIMESTAMP,

    CONSTRAINT pk_claim PRIMARY KEY (claim_id),

    CONSTRAINT fk_claim_policy
        FOREIGN KEY (policy_number)
        REFERENCES VALIDATED.POLICIES(policy_number)
);

CREATE OR REPLACE FUNCTION VALIDATED.VALIDATE_POLICY_RECORD(
policy_number STRING,
customer_id STRING,
email STRING,
ssn STRING,
annual_premium STRING,
effective_date STRING,
expiration_date STRING
)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$

policy_number IS NOT NULL
AND customer_id IS NOT NULL

/* Email validation */
AND (
email IS NULL 
OR REGEXP_LIKE(email,'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
)

/* SSN validation */
AND (
ssn IS NULL
OR REGEXP_LIKE(ssn,'^[0-9-]+$')
)

/* Premium validation */
AND TRY_TO_NUMBER(REPLACE(NULLIF(annual_premium,''),',','')) > 0

/* Date validation */
AND TRY_TO_DATE(effective_date) IS NOT NULL
AND TRY_TO_DATE(expiration_date) IS NOT NULL
AND TRY_TO_DATE(effective_date) < TRY_TO_DATE(expiration_date)

$$;

CREATE OR REPLACE PROCEDURE ABC_INSURANCE_1.VALIDATED.LOAD_POLICIES()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$

BEGIN

MERGE INTO VALIDATED.POLICIES T
USING (

    SELECT
        policy_number,
        customer_id,
        first_name,
        last_name,
        ssn,
        email,
        phone,
        address,
        city,
        state,
        zip,
        policy_type,

        TRY_TO_DATE(effective_date) AS effective_date,
        TRY_TO_DATE(expiration_date) AS expiration_date,

        /* FIXED PREMIUM CONVERSION */
        TRY_TO_NUMBER(
            REPLACE(NULLIF(annual_premium,''),',','')
        ) AS annual_premium,

        payment_frequency,
        renewal_flag,
        policy_status,
        marital_status,
        agent_id,
        agency_name,
        agency_region,

        file_name,
        load_timestamp

    FROM RAW.POLICIES_RAW_STREAM

    WHERE VALIDATED.VALIDATE_POLICY_RECORD(
        policy_number,
        customer_id,
        email,
        ssn,
        annual_premium,
        effective_date,
        expiration_date
    )

) S

ON T.policy_number = S.policy_number


WHEN MATCHED THEN UPDATE SET

    T.customer_id = S.customer_id,
    T.first_name = S.first_name,
    T.last_name = S.last_name,
    T.ssn = S.ssn,
    T.email = S.email,
    T.phone = S.phone,
    T.address = S.address,
    T.city = S.city,
    T.state = S.state,
    T.zip = S.zip,
    T.policy_type = S.policy_type,
    T.effective_date = S.effective_date,
    T.expiration_date = S.expiration_date,
    T.annual_premium = S.annual_premium,
    T.payment_frequency = S.payment_frequency,
    T.renewal_flag = S.renewal_flag,
    T.policy_status = S.policy_status,
    T.marital_status = S.marital_status,
    T.agent_id = S.agent_id,
    T.agency_name = S.agency_name,
    T.agency_region = S.agency_region,
    T.source_file = S.file_name,
    T.load_timestamp = S.load_timestamp


WHEN NOT MATCHED THEN INSERT (

    policy_number,
    customer_id,
    first_name,
    last_name,
    ssn,
    email,
    phone,
    address,
    city,
    state,
    zip,
    policy_type,
    effective_date,
    expiration_date,
    annual_premium,
    payment_frequency,
    renewal_flag,
    policy_status,
    marital_status,
    agent_id,
    agency_name,
    agency_region,
    source_file,
    load_timestamp

)

VALUES (

    S.policy_number,
    S.customer_id,
    S.first_name,
    S.last_name,
    S.ssn,
    S.email,
    S.phone,
    S.address,
    S.city,
    S.state,
    S.zip,
    S.policy_type,
    S.effective_date,
    S.expiration_date,
    S.annual_premium,
    S.payment_frequency,
    S.renewal_flag,
    S.policy_status,
    S.marital_status,
    S.agent_id,
    S.agency_name,
    S.agency_region,
    S.file_name,
    S.load_timestamp

);

RETURN 'POLICY LOAD COMPLETED';

END;

$$;

CALL ABC_INSURANCE_1.VALIDATED.LOAD_POLICIES();

select * from VALIDATED.POLICIES;

CREATE OR REPLACE FUNCTION VALIDATED.VALIDATE_CLAIM_RECORD(
    claim_id STRING,
    policy_number STRING,
    customer_id STRING,
    incident_date STRING,
    fnol_datetime STRING,
    total_incurred STRING,
    total_paid STRING
)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$

/* Mandatory fields */
claim_id IS NOT NULL
AND policy_number IS NOT NULL
AND customer_id IS NOT NULL

/* Policy must exist in policy table */
AND EXISTS (
    SELECT 1
    FROM VALIDATED.POLICIES P
    WHERE P.policy_number = policy_number
)

/* Date validation */
AND TRY_TO_DATE(incident_date) IS NOT NULL
AND TRY_TO_TIMESTAMP(fnol_datetime) IS NOT NULL

/* Amount validation with comma handling */
AND TRY_TO_NUMBER(REPLACE(total_incurred,',','')) >= 0
AND TRY_TO_NUMBER(REPLACE(total_paid,',','')) >= 0

$$;

CREATE OR REPLACE PROCEDURE VALIDATED.LOAD_CLAIMS()
RETURNS STRING
LANGUAGE SQL
AS
$$

BEGIN

MERGE INTO VALIDATED.CLAIMS T
USING (

SELECT

raw_data:claim_id::STRING AS claim_id,
raw_data:policy_number::STRING AS policy_number,
raw_data:customer_id::STRING AS customer_id,

/* Address fields */
raw_data:address.city::STRING AS city,
raw_data:address.state::STRING AS state,
raw_data:address.zip::STRING AS zip,
raw_data:claim_type:: STRING AS claim_type,

/* Status fields */
raw_data:status::STRING AS claim_status,
raw_data:report_channel::STRING AS report_channel,

/* Date conversion */
TRY_TO_DATE(raw_data:incident_date::STRING) AS incident_date,
TRY_TO_TIMESTAMP(raw_data:fnol_datetime::STRING) AS fnol_datetime,

/* Amount conversion */
TRY_TO_NUMBER(REPLACE(NULLIF(raw_data:total_incurred::STRING,''),',','')) AS total_incurred,
TRY_TO_NUMBER(REPLACE(NULLIF(raw_data:total_paid::STRING,''),',','')) AS total_paid,

file_name,
load_timestamp

FROM RAW.CLAIMS_RAW_STREAM

WHERE VALIDATED.VALIDATE_CLAIM_RECORD(

raw_data:claim_id::STRING,
raw_data:policy_number::STRING,
raw_data:customer_id::STRING,
raw_data:incident_date::STRING,
raw_data:fnol_datetime::STRING,
raw_data:total_incurred::STRING,
raw_data:total_paid::STRING

)

) S

ON T.claim_id = S.claim_id

WHEN MATCHED THEN UPDATE SET

T.policy_number = S.policy_number,
T.customer_id = S.customer_id,
T.city = S.city,
T.state = S.state,
T.zip = S.zip,
T.claim_type=S.claim_type,
T.claim_status = S.claim_status,
T.report_channel = S.report_channel,
T.incident_date = S.incident_date,
T.fnol_datetime = S.fnol_datetime,
T.total_incurred = S.total_incurred,
T.total_paid = S.total_paid,
T.source_file = S.file_name,
T.load_timestamp = S.load_timestamp

WHEN NOT MATCHED THEN INSERT (

claim_id,
policy_number,
customer_id,
city,
state,
zip,
claim_type,
claim_status,
report_channel,
incident_date,
fnol_datetime,
total_incurred,
total_paid,
source_file,
load_timestamp

)

VALUES (

S.claim_id,
S.policy_number,
S.customer_id,
S.city,
S.state,
S.zip,
S.claim_type,
S.claim_status,
S.report_channel,
S.incident_date,
S.fnol_datetime,
S.total_incurred,
S.total_paid,
S.file_name,
S.load_timestamp

);

RETURN 'CLAIMS LOAD COMPLETED';

END;

$$;

CALL VALIDATED.LOAD_CLAIMS();

select * from validated.claims;
select * from validated.policies;


CREATE OR REPLACE TABLE CURATED.DIM_CUSTOMER (
    customer_key NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    customer_id VARCHAR NOT NULL,
    first_name VARCHAR,
    last_name VARCHAR,
    ssn VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state VARCHAR,
    zip VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_key)
);


CREATE OR REPLACE TABLE CURATED.DIM_AGENT (
    agent_key NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    agent_id VARCHAR NOT NULL,
    agency_name VARCHAR,
    agency_region VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_dim_agent PRIMARY KEY (agent_key)
);


CREATE OR REPLACE TABLE CURATED.DIM_POLICY (
    policy_key NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    policy_number VARCHAR NOT NULL,
    policy_type VARCHAR,
    payment_frequency VARCHAR,
    renewal_flag VARCHAR,
    policy_status VARCHAR,
    marital_status VARCHAR,
    effective_date DATE,
    expiration_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_dim_policy PRIMARY KEY (policy_key)
);

CREATE OR REPLACE TABLE CURATED.FACT_POLICY (
    policy_fact_key NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    policy_number VARCHAR NOT NULL,
    customer_key NUMBER NOT NULL,
    agent_key NUMBER NOT NULL,
    policy_key NUMBER NOT NULL,
    annual_premium NUMBER(12,2),
    effective_date DATE,
    expiration_date DATE,
    active_flag NUMBER(1,0),
    invalid_contact_flag NUMBER(1,0),
    premium_anomaly_flag NUMBER(1,0),
    invalid_term_flag NUMBER(1,0),
    source_file VARCHAR,
    load_timestamp TIMESTAMP,
    CONSTRAINT pk_fact_policy PRIMARY KEY (policy_fact_key)
);

CREATE OR REPLACE TABLE CURATED.FACT_CLAIM (
    claim_fact_key NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    claim_id VARCHAR NOT NULL,
    policy_number VARCHAR NOT NULL,
    customer_key NUMBER,
    policy_key NUMBER,
    claim_type VARCHAR,
    incident_date DATE,
    fnol_datetime TIMESTAMP,
    claim_status VARCHAR,
    report_channel VARCHAR,
    city VARCHAR,
    state VARCHAR,
    zip VARCHAR,
    total_incurred NUMBER(12,2),
    total_paid NUMBER(12,2),
    claim_severity NUMBER(12,2),
    closed_claim_flag NUMBER(1,0),
    source_file VARCHAR,
    load_timestamp TIMESTAMP,
    CONSTRAINT pk_fact_claim PRIMARY KEY (claim_fact_key)
);


CREATE OR REPLACE PROCEDURE CURATED.SP_LOAD_DIM_CUSTOMER()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

MERGE INTO CURATED.DIM_CUSTOMER T
USING (
    SELECT
        customer_id,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        ssn,
        LOWER(TRIM(email)) AS email,
        REGEXP_REPLACE(phone, '[^0-9]', '') AS phone,
        TRIM(address) AS address,
        TRIM(city) AS city,
        UPPER(TRIM(state)) AS state,
        TRIM(zip) AS zip
    FROM VALIDATED.POLICIES
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY load_timestamp DESC
    ) = 1
) S
ON T.customer_id = S.customer_id
WHEN MATCHED THEN UPDATE SET
    T.first_name = S.first_name,
    T.last_name = S.last_name,
    T.ssn = S.ssn,
    T.email = S.email,
    T.phone = S.phone,
    T.address = S.address,
    T.city = S.city,
    T.state = S.state,
    T.zip = S.zip,
    T.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    customer_id, first_name, last_name, ssn, email, phone,
    address, city, state, zip, created_at, updated_at
)
VALUES (
    S.customer_id, S.first_name, S.last_name, S.ssn, S.email, S.phone,
    S.address, S.city, S.state, S.zip, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

RETURN 'DIM_CUSTOMER loaded successfully';

END;
$$;


CREATE OR REPLACE PROCEDURE CURATED.SP_LOAD_DIM_AGENT()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

MERGE INTO CURATED.DIM_AGENT T
USING (
    SELECT
        agent_id,
        TRIM(agency_name) AS agency_name,
        UPPER(TRIM(agency_region)) AS agency_region
    FROM VALIDATED.POLICIES
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY agent_id
        ORDER BY load_timestamp DESC
    ) = 1
) S
ON T.agent_id = S.agent_id
WHEN MATCHED THEN UPDATE SET
    T.agency_name = S.agency_name,
    T.agency_region = S.agency_region,
    T.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    agent_id, agency_name, agency_region, created_at, updated_at
)
VALUES (
    S.agent_id, S.agency_name, S.agency_region, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

RETURN 'DIM_AGENT loaded successfully';

END;
$$;


CREATE OR REPLACE PROCEDURE CURATED.SP_LOAD_DIM_POLICY()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

MERGE INTO CURATED.DIM_POLICY T
USING (
    SELECT
        policy_number,
        UPPER(TRIM(policy_type)) AS policy_type,
        UPPER(TRIM(payment_frequency)) AS payment_frequency,
        UPPER(TRIM(renewal_flag)) AS renewal_flag,
        UPPER(TRIM(policy_status)) AS policy_status,
        UPPER(TRIM(marital_status)) AS marital_status,
        effective_date,
        expiration_date
    FROM VALIDATED.POLICIES
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY policy_number
        ORDER BY load_timestamp DESC
    ) = 1
) S
ON T.policy_number = S.policy_number
WHEN MATCHED THEN UPDATE SET
    T.policy_type = S.policy_type,
    T.payment_frequency = S.payment_frequency,
    T.renewal_flag = S.renewal_flag,
    T.policy_status = S.policy_status,
    T.marital_status = S.marital_status,
    T.effective_date = S.effective_date,
    T.expiration_date = S.expiration_date,
    T.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    policy_number, policy_type, payment_frequency, renewal_flag,
    policy_status, marital_status, effective_date, expiration_date,
    created_at, updated_at
)
VALUES (
    S.policy_number, S.policy_type, S.payment_frequency, S.renewal_flag,
    S.policy_status, S.marital_status, S.effective_date, S.expiration_date,
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

RETURN 'DIM_POLICY loaded successfully';

END;
$$;

CREATE OR REPLACE PROCEDURE CURATED.SP_LOAD_FACT_POLICY()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

MERGE INTO CURATED.FACT_POLICY T
USING (
    SELECT
        P.policy_number,
        C.customer_key,
        A.agent_key,
        D.policy_key,
        P.annual_premium,
        P.effective_date,
        P.expiration_date,
        CASE
            WHEN UPPER(TRIM(P.policy_status)) = 'ACTIVE' THEN 1
            ELSE 0
        END AS active_flag,
        CASE
            WHEN P.email IS NULL OR TRIM(P.email) = ''
              OR P.phone IS NULL OR TRIM(P.phone) = ''
              OR P.zip IS NULL OR TRIM(P.zip) = ''
            THEN 1 ELSE 0
        END AS invalid_contact_flag,
        CASE
            WHEN P.annual_premium < 0 OR P.annual_premium > 100000 THEN 1
            ELSE 0
        END AS premium_anomaly_flag,
        CASE
            WHEN P.effective_date IS NULL
              OR P.expiration_date IS NULL
              OR P.effective_date >= P.expiration_date
            THEN 1 ELSE 0
        END AS invalid_term_flag,
        P.source_file,
        P.load_timestamp
    FROM VALIDATED.POLICIES P
    JOIN CURATED.DIM_CUSTOMER C
      ON P.customer_id = C.customer_id
    JOIN CURATED.DIM_AGENT A
      ON P.agent_id = A.agent_id
    JOIN CURATED.DIM_POLICY D
      ON P.policy_number = D.policy_number
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY P.policy_number
        ORDER BY P.load_timestamp DESC
    ) = 1
) S
ON T.policy_number = S.policy_number
WHEN MATCHED THEN UPDATE SET
    T.customer_key = S.customer_key,
    T.agent_key = S.agent_key,
    T.policy_key = S.policy_key,
    T.annual_premium = S.annual_premium,
    T.effective_date = S.effective_date,
    T.expiration_date = S.expiration_date,
    T.active_flag = S.active_flag,
    T.invalid_contact_flag = S.invalid_contact_flag,
    T.premium_anomaly_flag = S.premium_anomaly_flag,
    T.invalid_term_flag = S.invalid_term_flag,
    T.source_file = S.source_file,
    T.load_timestamp = S.load_timestamp
WHEN NOT MATCHED THEN INSERT (
    policy_number, customer_key, agent_key, policy_key,
    annual_premium, effective_date, expiration_date,
    active_flag, invalid_contact_flag, premium_anomaly_flag,
    invalid_term_flag, source_file, load_timestamp
)
VALUES (
    S.policy_number, S.customer_key, S.agent_key, S.policy_key,
    S.annual_premium, S.effective_date, S.expiration_date,
    S.active_flag, S.invalid_contact_flag, S.premium_anomaly_flag,
    S.invalid_term_flag, S.source_file, S.load_timestamp
);

RETURN 'FACT_POLICY loaded successfully';

END;
$$;


CREATE OR REPLACE PROCEDURE CURATED.SP_LOAD_FACT_CLAIM()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

MERGE INTO CURATED.FACT_CLAIM T
USING (
    SELECT
        CL.claim_id,
        CL.policy_number,
        C.customer_key,
        D.policy_key,
        UPPER(TRIM(CL.claim_type)) AS claim_type,
        CL.incident_date,
        CL.fnol_datetime,
        UPPER(TRIM(CL.claim_status)) AS claim_status,
        UPPER(TRIM(CL.report_channel)) AS report_channel,
        TRIM(CL.city) AS city,
        UPPER(TRIM(CL.state)) AS state,
        TRIM(CL.zip) AS zip,
        CL.total_incurred,
        CL.total_paid,
        CL.total_incurred AS claim_severity,
        CASE
            WHEN UPPER(TRIM(CL.claim_status)) IN ('CLOSED', 'SETTLED', 'PAID') THEN 1
            ELSE 0
        END AS closed_claim_flag,
        CL.source_file,
        CL.load_timestamp
    FROM VALIDATED.CLAIMS CL
    LEFT JOIN CURATED.DIM_CUSTOMER C
      ON CL.customer_id = C.customer_id
    LEFT JOIN CURATED.DIM_POLICY D
      ON CL.policy_number = D.policy_number
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CL.claim_id
        ORDER BY CL.load_timestamp DESC
    ) = 1
) S
ON T.claim_id = S.claim_id
WHEN MATCHED THEN UPDATE SET
    T.policy_number = S.policy_number,
    T.customer_key = S.customer_key,
    T.policy_key = S.policy_key,
    T.claim_type = S.claim_type,
    T.incident_date = S.incident_date,
    T.fnol_datetime = S.fnol_datetime,
    T.claim_status = S.claim_status,
    T.report_channel = S.report_channel,
    T.city = S.city,
    T.state = S.state,
    T.zip = S.zip,
    T.total_incurred = S.total_incurred,
    T.total_paid = S.total_paid,
    T.claim_severity = S.claim_severity,
    T.closed_claim_flag = S.closed_claim_flag,
    T.source_file = S.source_file,
    T.load_timestamp = S.load_timestamp
WHEN NOT MATCHED THEN INSERT (
    claim_id, policy_number, customer_key, policy_key, claim_type,
    incident_date, fnol_datetime, claim_status, report_channel,
    city, state, zip, total_incurred, total_paid,
    claim_severity, closed_claim_flag, source_file, load_timestamp
)
VALUES (
    S.claim_id, S.policy_number, S.customer_key, S.policy_key, S.claim_type,
    S.incident_date, S.fnol_datetime, S.claim_status, S.report_channel,
    S.city, S.state, S.zip, S.total_incurred, S.total_paid,
    S.claim_severity, S.closed_claim_flag, S.source_file, S.load_timestamp
);

RETURN 'FACT_CLAIM loaded successfully';

END;
$$;

CREATE OR REPLACE PROCEDURE CURATED.SP_LOAD_CURATED_MASTER()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

CALL CURATED.SP_LOAD_DIM_CUSTOMER();
CALL CURATED.SP_LOAD_DIM_AGENT();
CALL CURATED.SP_LOAD_DIM_POLICY();
CALL CURATED.SP_LOAD_FACT_POLICY();
CALL CURATED.SP_LOAD_FACT_CLAIM();

RETURN 'CURATED layer master load completed successfully';

END;
$$;

CALL CURATED.SP_LOAD_CURATED_MASTER();


select * from curated.dim_customer;
select * from curated.dim_agent;
select * from curated.dim_policy;

select * from curated.fact_claim;
select * from curated.fact_policy;


CREATE OR REPLACE STREAM RAW.POLICIES_RAW_STREAM
ON TABLE RAW.POLICIES_RAW;

CREATE OR REPLACE STREAM RAW.CLAIMS_RAW_STREAM
ON TABLE RAW.CLAIMS_RAW;

CREATE OR REPLACE STREAM VALIDATED.POLICIES_STREAM
ON TABLE VALIDATED.POLICIES;

CREATE OR REPLACE STREAM VALIDATED.CLAIMS_STREAM
ON TABLE VALIDATED.CLAIMS;


-- task for policies raw stream
CREATE OR REPLACE TASK RAW.TSK_RAW_POLICIES_STREAM
  WAREHOUSE = compute_wh
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('RAW.POLICIES_RAW_STREAM')
AS
  CALL VALIDATED.LOAD_POLICIES();

-- task for claims raw stream
CREATE OR REPLACE TASK RAW.TSK_RAW_CLAIMS_STREAM
  WAREHOUSE = compute_wh
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('RAW.CLAIMS_RAW_STREAM')
AS
  CALL VALIDATED.LOAD_CLAIMS();


ALTER TASK RAW.TSK_RAW_POLICIES_STREAM RESUME;
ALTER TASK RAW.TSK_RAW_CLAIMS_STREAM RESUME;

ALTER TASK RAW.TSK_RAW_POLICIES_STREAM SUSPEND;
ALTER TASK RAW.TSK_RAW_CLAIMS_STREAM SUSPEND;

SHOW TASKS IN SCHEMA RAW;

select * from raw.policies_raw_stream limit 10;
select * from raw.claims_raw_stream limit 10;

select * from validated.policies_stream;
select * from validated.claims_stream;

CREATE OR REPLACE TASK CURATED.TSK_VALIDATED_TO_CURATED
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'
  WHEN
    SYSTEM$STREAM_HAS_DATA('VALIDATED.POLICIES_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('VALIDATED.CLAIMS_STREAM')
AS
  CALL CURATED.SP_LOAD_CURATED_MASTER();

  ALTER TASK CURATED.TSK_VALIDATED_TO_CURATED suspend;

  SHOW TASKS IN SCHEMA CURATED;

  ALTER TASK RAW.TSK_RAW_POLICIES_STREAM SUSPEND;

  CREATE OR REPLACE VIEW CURATED.VW_REQ1_AGENT_CONTACT_DATA_QUALITY AS
SELECT
    A.agent_id,
    A.agency_name,
    A.agency_region,
    COUNT(*) AS total_policies,
    SUM(F.invalid_contact_flag) AS invalid_policies,
    ROUND(100.0 * SUM(F.invalid_contact_flag) / NULLIF(COUNT(*), 0), 2) AS invalid_policy_pct
FROM CURATED.FACT_POLICY F
JOIN CURATED.DIM_AGENT A
  ON F.agent_key = A.agent_key
GROUP BY
    A.agent_id,
    A.agency_name,
    A.agency_region;

    CREATE OR REPLACE VIEW CURATED.VW_REQ2_POLICY_AMOUNT_TERM_VALIDATION_BY_STATE AS
SELECT
    C.state,
    COUNT(*) AS total_policies,
    SUM(F.premium_anomaly_flag) AS premium_anomaly_count,
    SUM(F.invalid_term_flag) AS invalid_term_count,
    SUM(CASE
            WHEN F.premium_anomaly_flag = 1 OR F.invalid_term_flag = 1 THEN 1
            ELSE 0
        END) AS invalid_policy_count,
    ROUND(
        100.0 * SUM(CASE
                        WHEN F.premium_anomaly_flag = 1 OR F.invalid_term_flag = 1 THEN 1
                        ELSE 0
                    END) / NULLIF(COUNT(*), 0), 2
    ) AS invalid_policy_pct
FROM CURATED.FACT_POLICY F
JOIN CURATED.DIM_CUSTOMER C
  ON F.customer_key = C.customer_key
GROUP BY C.state;

CREATE OR REPLACE VIEW CURATED.VW_REQ3_POLICY_CLAIM_MATCHING_COVERAGE AS
WITH ACTIVE_POLICIES AS (
    SELECT policy_number
    FROM CURATED.FACT_POLICY
    WHERE active_flag = 1
),
POLICIES_WITH_CLAIMS AS (
    SELECT DISTINCT policy_number
    FROM CURATED.FACT_CLAIM
    WHERE policy_number IS NOT NULL
)
SELECT
    COUNT(AP.policy_number) AS total_active_policies,
    COUNT(PWC.policy_number) AS active_policies_with_claim,
    ROUND(
        100.0 * COUNT(PWC.policy_number) / NULLIF(COUNT(AP.policy_number), 0), 2
    ) AS matching_coverage_pct
FROM ACTIVE_POLICIES AP
LEFT JOIN POLICIES_WITH_CLAIMS PWC
  ON AP.policy_number = PWC.policy_number;

  CREATE OR REPLACE VIEW CURATED.VW_REQ4_CITY_PREMIUM_BENCHMARKING AS
WITH CITY_PREMIUM AS (
    SELECT
        C.state,
        C.city,
        AVG(F.annual_premium) AS city_avg_premium
    FROM CURATED.FACT_POLICY F
    JOIN CURATED.DIM_CUSTOMER C
      ON F.customer_key = C.customer_key
    GROUP BY C.state, C.city
),
STATE_PREMIUM AS (
    SELECT
        C.state,
        AVG(F.annual_premium) AS state_avg_premium
    FROM CURATED.FACT_POLICY F
    JOIN CURATED.DIM_CUSTOMER C
      ON F.customer_key = C.customer_key
    GROUP BY C.state
)
SELECT
    CP.state,
    CP.city,
    ROUND(CP.city_avg_premium, 2) AS city_avg_premium,
    ROUND(SP.state_avg_premium, 2) AS state_avg_premium,
    ROUND(CP.city_avg_premium - SP.state_avg_premium, 2) AS premium_diff_from_state_avg,
    CASE
        WHEN CP.city_avg_premium > SP.state_avg_premium THEN 'ABOVE_STATE_AVG'
        WHEN CP.city_avg_premium < SP.state_avg_premium THEN 'BELOW_STATE_AVG'
        ELSE 'EQUAL_STATE_AVG'
    END AS comparison_to_state_avg
FROM CITY_PREMIUM CP
JOIN STATE_PREMIUM SP
  ON CP.state = SP.state;


  CREATE OR REPLACE VIEW CURATED.VW_REQ5_TOP_CITIES_BY_CLAIM_SEVERITY AS
WITH CITY_SEVERITY AS (
    SELECT
        state,
        city,
        AVG(claim_severity) AS avg_closed_claim_severity
    FROM CURATED.FACT_CLAIM
    WHERE closed_claim_flag = 1
      AND state IS NOT NULL
      AND city IS NOT NULL
    GROUP BY state, city
),
RANKED AS (
    SELECT
        state,
        city,
        ROUND(avg_closed_claim_severity, 2) AS avg_closed_claim_severity,
        ROW_NUMBER() OVER (
            PARTITION BY state
            ORDER BY avg_closed_claim_severity DESC
        ) AS city_rank
    FROM CITY_SEVERITY
)
SELECT
    state,
    city,
    avg_closed_claim_severity,
    city_rank
FROM RANKED
WHERE city_rank <= 5;


CREATE OR REPLACE VIEW CURATED.VW_KPI_06_CROSS_SELL_AUTO_HOME_BY_REGION AS
WITH CUSTOMER_POLICY_TYPES AS (
    SELECT
        FP.customer_key,
        A.agency_region,
        MAX(CASE WHEN DP.policy_type = 'AUTO' THEN 1 ELSE 0 END) AS has_auto,
        MAX(CASE WHEN DP.policy_type = 'HOME' THEN 1 ELSE 0 END) AS has_home
    FROM CURATED.FACT_POLICY FP
    JOIN CURATED.DIM_POLICY DP
      ON FP.policy_key = DP.policy_key
    JOIN CURATED.DIM_AGENT A
      ON FP.agent_key = A.agent_key
    GROUP BY
        FP.customer_key,
        A.agency_region
)
SELECT
    agency_region,
    COUNT(*) AS total_customers,
    SUM(CASE WHEN has_auto = 1 AND has_home = 1 THEN 1 ELSE 0 END) AS auto_home_cross_sell_customers,
    ROUND(
        100.0 * SUM(CASE WHEN has_auto = 1 AND has_home = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        2
    ) AS auto_home_cross_sell_pct
FROM CUSTOMER_POLICY_TYPES
GROUP BY agency_region;

CREATE OR REPLACE VIEW CURATED.VW_KPI_07_30_DAY_ONBOARDING_ATTACHMENT_RATE AS
WITH POLICY_DATES AS (
    SELECT
        customer_key,
        policy_number,
        effective_date,
        MIN(effective_date) OVER (PARTITION BY customer_key) AS first_policy_date
    FROM CURATED.FACT_POLICY
),
CUSTOMER_ATTACHMENT AS (
    SELECT
        customer_key,
        first_policy_date,
        MAX(
            CASE
                WHEN effective_date > first_policy_date
                 AND effective_date <= DATEADD(DAY, 30, first_policy_date)
                THEN 1 ELSE 0
            END
        ) AS attached_within_30_days
    FROM POLICY_DATES
    GROUP BY
        customer_key,
        first_policy_date
)
SELECT
    COUNT(*) AS total_customers,
    SUM(attached_within_30_days) AS customers_attached_within_30_days,
    ROUND(
        100.0 * SUM(attached_within_30_days) / NULLIF(COUNT(*), 0),
        2
    ) AS attachment_rate_30_days_pct
FROM CUSTOMER_ATTACHMENT;


CREATE OR REPLACE VIEW CURATED.VW_KPI_08_POLICY_ONLY_VS_CLAIM_ONLY_CUSTOMERS AS
WITH POLICY_CUSTOMERS AS (
    SELECT DISTINCT customer_key
    FROM CURATED.FACT_POLICY
),
CLAIM_CUSTOMERS AS (
    SELECT DISTINCT customer_key
    FROM CURATED.FACT_CLAIM
    WHERE customer_key IS NOT NULL
)
SELECT
    CASE
        WHEN P.customer_key IS NOT NULL AND C.customer_key IS NOT NULL THEN 'BOTH_POLICY_AND_CLAIM'
        WHEN P.customer_key IS NOT NULL AND C.customer_key IS NULL THEN 'POLICY_ONLY'
        WHEN P.customer_key IS NULL AND C.customer_key IS NOT NULL THEN 'CLAIM_ONLY'
    END AS customer_segment,
    COUNT(*) AS customer_count
FROM POLICY_CUSTOMERS P
FULL OUTER JOIN CLAIM_CUSTOMERS C
  ON P.customer_key = C.customer_key
GROUP BY 1;

use schema security;

CREATE OR REPLACE ROLE R_ADMIN;
CREATE OR REPLACE ROLE R_ANALYTICS;
CREATE OR REPLACE ROLE R_REGIONAL_ANALYST;

SELECT CURRENT_USER();

-- example
GRANT ROLE R_ADMIN TO USER HRITIK00001;
GRANT ROLE R_ANALYTICS TO USER HRITIK00001;
GRANT ROLE R_REGIONAL_ANALYST TO USER HRITIK00001;

CREATE OR REPLACE TABLE SECURITY.USER_REGION_MAP (
    user_name VARCHAR,
    agency_region VARCHAR
);

INSERT INTO SECURITY.USER_REGION_MAP (user_name, agency_region) VALUES
('regional_user_east', 'EAST'),
('regional_user_west', 'WEST');

CREATE OR REPLACE MASKING POLICY SECURITY.MP_SSN
AS (val STRING) RETURNS STRING ->
    CASE
        WHEN IS_ROLE_IN_SESSION('R_ADMIN') THEN val
        WHEN val IS NULL THEN NULL
        ELSE 'XXX-XX-' || RIGHT(REGEXP_REPLACE(val, '[^0-9]', ''), 4)
    END;


CREATE OR REPLACE MASKING POLICY SECURITY.MP_EMAIL
AS (val STRING) RETURNS STRING ->
    CASE
        WHEN IS_ROLE_IN_SESSION('R_ADMIN') THEN val
        WHEN val IS NULL THEN NULL
        ELSE '***MASKED***'
    END;


CREATE OR REPLACE MASKING POLICY SECURITY.MP_ADDRESS
AS (val STRING) RETURNS STRING ->
    CASE
        WHEN IS_ROLE_IN_SESSION('R_ADMIN') THEN val
        WHEN val IS NULL THEN NULL
        ELSE '***MASKED ADDRESS***'
    END;

CREATE OR REPLACE SECURE VIEW CURATED.VW_FACT_POLICY_SECURE AS
SELECT
    F.*
FROM CURATED.FACT_POLICY F
JOIN CURATED.DIM_AGENT A
  ON F.agent_key = A.agent_key
WHERE
      IS_ROLE_IN_SESSION('R_ADMIN')
   OR IS_ROLE_IN_SESSION('R_ANALYTICS')
   OR (
        IS_ROLE_IN_SESSION('R_REGIONAL_ANALYST')
        AND EXISTS (
            SELECT 1
            FROM SECURITY.USER_REGION_MAP U
            WHERE UPPER(U.user_name) = UPPER(CURRENT_USER())
              AND UPPER(U.agency_region) = UPPER(A.agency_region)
        )
      );



-- 1) role banao
USE ROLE SECURITYADMIN;
CREATE ROLE IF NOT EXISTS WORKSPACE_ADMIN_ROLE;

-- 2) user banao
USE ROLE USERADMIN;
CREATE USER IF NOT EXISTS hritik
  PASSWORD = '123456789123'
  DEFAULT_ROLE = WORKSPACE_ADMIN_ROLE
  MUST_CHANGE_PASSWORD = TRUE;

-- 3) role user ko assign karo
USE ROLE SECURITYADMIN;
GRANT ROLE WORKSPACE_ADMIN_ROLE TO USER hritik;


-- warehouse access (query run karne ke liye)
GRANT USAGE, OPERATE ON WAREHOUSE compute_wh TO ROLE WORKSPACE_ADMIN_ROLE;

-- database access
GRANT USAGE ON DATABASE abc_insurance_1 TO ROLE WORKSPACE_ADMIN_ROLE;
GRANT CREATE SCHEMA ON DATABASE abc_insurance_1 TO ROLE WORKSPACE_ADMIN_ROLE;

-- existing schemas par access
GRANT USAGE ON ALL SCHEMAS IN DATABASE abc_insurance_1 TO ROLE WORKSPACE_ADMIN_ROLE;
GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT
  ON ALL SCHEMAS IN DATABASE abc_insurance_1 TO ROLE WORKSPACE_ADMIN_ROLE;

-- future schemas par bhi access
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE abc_insurance_1 TO ROLE WORKSPACE_ADMIN_ROLE;
GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT
  ON FUTURE SCHEMAS IN DATABASE abc_insurance_1 TO ROLE WORKSPACE_ADMIN_ROLE;

-- existing tables par DML
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES
  ON ALL TABLES IN DATABASE abc_insurance_1 TO ROLE WORKSPACE_ADMIN_ROLE;

-- future tables par bhi DML
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES
  ON FUTURE TABLES IN DATABASE abc_insurance_1 TO ROLE WORKSPACE_ADMIN_ROLE;

-- existing views
GRANT SELECT ON ALL VIEWS IN DATABASE abc_insurance_1 TO ROLE WORKSPACE_ADMIN_ROLE;

-- future views
GRANT SELECT ON FUTURE VIEWS IN DATABASE abc_insurance_1 TO ROLE WORKSPACE_ADMIN_ROLE;


