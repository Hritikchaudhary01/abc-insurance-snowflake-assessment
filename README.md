# ABC Insurance - Assessment Brief

This repository contains the implementation for the **ABC Insurance Data Engineering / Analytics Assessment**.  
The goal is to build a trusted, analytics-ready view of **policy** and **claim** data in Snowflake, with proper ingestion, validation, curation, governance, and KPI reporting.

---

## Business Objective

ABC Insurance aims to establish a consistent and trusted view of its policy and claims information to improve decision-making across underwriting, claims, and distribution functions.

This initiative focuses on improving the quality, completeness, and alignment of customer, policy, and claim records while highlighting important trends in:

- premium performance
- claim severity
- customer behavior
- overall data health

By standardizing how core business metrics are defined and monitored, leadership can gain clearer insights into:

- growth opportunities
- operational gaps
- areas requiring corrective action

Ultimately, this supports a more proactive and insights-driven operating model.

---

## Source File Details

### 1) Policy Source Files - CSV (Master + Incremental)

**Files**
- `policies_master_v3.csv` (500 rows)
- `policies_inc_2026-02-15_v3.csv` (50 rows)

**Fields**
- `policy_number`
- `customer_id`
- `first_name`
- `last_name`
- `ssn`
- `email`
- `phone`
- `address`
- `city`
- `state`
- `zip`
- `policy_type`
- `effective_date`
- `expiration_date`
- `annual_premium`
- `payment_frequency`
- `renewal_flag`
- `policy_status`
- `marital_status`
- `agent_id`
- `agency_name`
- `agency_region`

---

### 2) Claim Source Files - NDJSON (Master + Incremental)

**Files**
- `claims_master_v3.json` (500 lines)
- `claims_inc_2026-02-15_v3.json` (50 lines)

**Fields**
- `claim_id`
- `policy_number`
- `customer_id`
- `structured_address` (`state`, `city`, `zip`)
- `claim_type`
- `incident_date`
- `fnol_datetime`
- `status`
- `report_channel`
- `total_incurred`
- `total_paid`

---

## Task Requirements

The implementation should cover the following:

- Establish data ingestion using `COPY INTO` with validation
- Maintain layered architecture:
  - `RAW`
  - `VALIDATED`
  - `CURATED`
- Ensure consistency through:
  - referential integrity checks
  - standard formatting
  - deduplication
- Process JSON requests using `VARIANT` columns
- Implement rules using **Snowflake SQL** and **JavaScript UDFs**
- Automate incremental processing using **Snowflake Tasks** and **Streams**
- Ensure idempotency using **MERGE** logic
- Secure sensitive fields with **RBAC** and **Dynamic Data Masking**
- Maintain full audit trails with append-only logs and metadata columns

---

## Deliverables

### REQ1 - Agent Contact Data Quality
Provide the percentage of policies per agent where customer contact information is invalid or incomplete.

**Contact fields include**
- email
- phone
- PIN / personally sensitive identifier fields as defined in the assessment brief

---

### REQ2 - Policy Amount and Term Validation by State
Provide the percentage of policies per state with:

- premium anomalies, or
- invalid effective / expiration date combinations

---

### REQ3 - Policy-Claim Matching Coverage
Provide the percentage of active policies that have at least one valid, linked claim record.

---

### REQ4 - City Premium Benchmarking
Provide the average premium per city and indicate how each city compares to its overall state average.

---

### REQ5 - Top Cities by Claim Severity
Identify the top five cities within each state ranked by highest average closed-claim severity.

---

### REQ6 - Cross-Sell Penetration (AUTO and HOME) by Region
Provide the percentage of customers in each region who simultaneously hold both **AUTO** and **HOME** policies.

---

### REQ7 - 30-Day Onboarding Attachment Rate
Provide the percentage of new customers (last 6 months) who purchase an additional line of business within 30 days of their first policy.

---

### REQ8 - Policy-Only vs Claim-Only Customer Counts
Identify the number of customers who exist:

- exclusively in policy files, or
- exclusively in claim files

---

### REQ-OPT-A - Dynamic Data Masking Demonstration
Demonstrate that sensitive fields are masked appropriately depending on user role, while KPI results remain unchanged.

---

### REQ-OPT-B - Row Access Policy Demonstration
Demonstrate that users only see policy / claim records for regions they are authorized to access.

---

## Evaluation Criteria

The solution will be evaluated on:

- **Architecture and Ingestion**
- **Data Quality and Curation**
- **KPI Accuracy and Analytical Implementation**
- **Security and Governance**

---

## Suggested Repository Structure

```bash
.
├── README.md
├── data/
│   ├── policies_master_v3.csv
│   ├── policies_inc_2026-02-15_v3.csv
│   ├── claims_master_v3.json
│   └── claims_inc_2026-02-15_v3.json
├── sql/
│   ├── 01_schema.sql
│   ├── 02_raw_load.sql
│   ├── 03_validation.sql
│   ├── 04_curated_layer.sql
│   ├── 05_kpi_queries.sql
│   ├── 06_masking_policies.sql
│   └── 07_row_access_policies.sql
├── udf/
│   └── validation_udfs.sql
├── tasks/
│   └── incremental_pipeline.sql
└── docs/
    └── architecture.md
```

---

## Recommended Implementation Approach

### Data Layers
- **RAW**: load source files as-is
- **VALIDATED**: apply schema checks, null checks, formatting, and deduplication
- **CURATED**: create business-ready policy and claim models for reporting

### Core Controls
- Primary key and business key checks
- Policy-to-claim linkage validation
- Date validation for effective and expiration windows
- Premium anomaly detection
- Customer identity consistency checks
- Incremental upsert handling with `MERGE`
- Full audit metadata such as:
  - load timestamp
  - source filename
  - batch id
  - record hash
  - created / updated timestamps

### Security
- Role-based access control for users
- Dynamic masking for PII fields such as:
  - SSN
  - email
  - phone
- Row-level access by `agency_region` or claim region

---

## Output Expectations

The final solution should provide:

- reusable Snowflake SQL scripts
- ingestion and transformation logic
- KPI queries for all required deliverables
- masking and row access policy examples
- documentation for setup and execution

---

## Notes

- Policy source files are provided in **CSV**
- Claim source files are provided in **NDJSON**
- Incremental processing should be automated and idempotent
- Governance and auditability are considered part of the solution, not optional add-ons

---

## Author

This repository was prepared for the **ABC Insurance Assessment**.

