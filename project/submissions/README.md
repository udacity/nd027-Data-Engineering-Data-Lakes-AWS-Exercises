# STEDI Human Balance Analytics Submission

This folder contains the final submission artifacts for the STEDI Human Balance Analytics project.

## Included files

- `create_customer_landing.sql`
- `create_accelerometer_landing.sql`
- `create_step_trainer_landing.sql`
- `customer_landing_to_trusted.py`
- `accelerometer_landing_to_trusted.py`
- `customer_trusted_to_curated.py`
- `step_trainer_trusted.py`
- `machine_learning_curated.py`
- 'accelerometer_landing.png'
- 'accelerometer_trusted_count.png'
- 'accelerometer_trusted.png'
- 'customer_curated.png'
- 'customer_landing_null_researchconsent.png'
- 'customer_landing.png'
- 'customer_trusted.png'
- 'machine_Learning_curated.png'
- 'step_trainer_landing.png'
- 'step_trainer_trusted.png'
- 'Project_Soln_Screenshots.docx'


## Recommended job order

1. Run the Athena DDL scripts to create the landing tables:
   - `create_customer_landing.sql`
   - `create_accelerometer_landing.sql`
   - `create_step_trainer_landing.sql`
2. Run Glue jobs in order:
   1. `customer_landing_to_trusted.py`
   2. `accelerometer_landing_to_trusted.py`
   3. `customer_trusted_to_curated.py`
   4. `step_trainer_trusted.py`
   5. `machine_learning_curated.py`

## Notes

- The scripts use `s3://s3spspark/` paths and `stedi_db` as the Glue/Athena database.
- Update the S3 bucket paths and database names if you use a different bucket or environment.
- The submission package is intentionally separate from the existing `Solutions/Queries` folder.
