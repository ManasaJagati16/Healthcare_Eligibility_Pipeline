# Healthcare Eligibility Data Pipeline

## Overview
This project implements a **scalable, configuration-driven healthcare eligibility ingestion pipeline** using **Apache Spark (PySpark)**.  
It standardizes eligible data from multiple partner files into a unified schema while supporting **easy onboarding of new partners via configuration only**.

No core code changes are required when adding a new partner with a different delimiter, file format, or column names.

---

## Key Features

- Config-driven multi-partner ingestion
- Supports different file formats and delimiters
- Column standardization into a unified schema
- Phone and email normalization
- Flexible date parsing with error tolerance
- Validation and rejection handling (bonus requirement)
- Spark transformations with pandas-based output write (Windows-safe)

---

## Standard Output Schema

| Column        | Description |
|--------------|------------|
| external_id  | Unique member identifier |
| first_name  | Member first name |
| last_name   | Member last name |
| dob         | Date of birth (YYYY-MM-DD) |
| email       | Normalized lowercase email |
| phone       | Formatted phone (XXX-XXX-XXXX) |
| partner_code| Source partner identifier |

---

## Validation & Rejected Rows Handling

### Validation Rules
- `external_id` **must be present**
- Rows missing `external_id` are treated as **rejected**
- Invalid or malformed date values are safely handled without failing the pipeline

### Rejected Rows Logic
- Rejected rows are **not included in the final unified output**
- Rejected rows are **written to a separate file** for auditing and data quality analysis
- Accepted and rejected record counts are logged per partner

### Output Files Created
```
output/
├── unified_eligibility.csv
└── rejected_rows.csv
```

---

## Sample Rejected Rows Output

```
external_id | first_name | last_name | dob        | email               | phone       | partner_code | rejection_reason
------------------------------------------------------------------------------------------------------------------------
NULL        | Mark       | Taylor    | 03/10/1960 | mark.t@email.com    | 5551112222  | ACME         | missing_external_id
BC-009      | Emma       | Clark     |            | emma.clark@test.com | 5553334444  | BETTERCARE   | BAD_DOB_FORMAT
```

> Rejected rows retain all original data along with a `rejection_reason` column.

---

## Configuration-Driven Design

All partner-specific logic lives in `config/partners.json`.

To add a new partner:
- Define file path
- Define delimiter
- Map partner columns to standard columns
- Provide supported DOB formats

No Python code changes required

### Sample partner Configuration
 
{
  "newhealth": {
    "file_path": "data/newhealth.dat",
    "delimiter": "~",
    "has_header": true,
    "partner_code": "NEWHEALTH",
    "column_mapping": {
      "member_id": "external_id",
      "fname": "first_name",
      "lname": "last_name",
      "birth_date": "dob",
      "email_address": "email",
      "contact_number": "phone"
    },
    "dob_formats": ["dd-MM-yyyy", "yyyy/MM/dd"]
  }
}


---

## Output Strategy: Spark vs Pandas

### Why Pandas Is Used for Writing Output?
On Windows systems, Spark CSV writes may fail due to Hadoop native dependency issues (`winutils.exe`).  
To ensure reliable execution during assessment:

- **Spark is used for all transformations**
- **pandas is used only for writing output files**

This approach avoids Hadoop commit protocol issues while preserving Spark scalability.

> In Linux or cloud environments, Spark write can be enabled via configuration without code changes.

---

## How to Run?

```bash
python src/pipeline_spark.py
```

---

## Example Unified Output

```
external_id | first_name | last_name | dob        | email               | phone        | partner_code
------------------------------------------------------------------------------------------------
1234567890A | John       | Doe       | 1955-03-15 | john.doe@email.com  | 555-123-4567 | ACME
BC-001      | Alice      | Johnson   | 1965-08-10 | alice.j@test.com    | 555-222-3333 | BETTERCARE
```

---

## Scalability & Extensibility

- Unlimited partners can be included via config
- Easy extension for:
  - Parquet output
  - Cloud storage (S3 / GCS)
  - Airflow orchestration
  - Schema enforcement
  - Data quality dashboards

---


