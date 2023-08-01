# ETL-Data-Pipeline-Development-for-Car-Sharing-Data
Designed and built a robust ETL (Extract, Transform, Load) data pipeline to transfer raw car-sharing data from the data lake (Amazon S3) to the data warehouse (Amazon Redshift) in the AWS cloud platform

![CarsharingDataPipeline](https://github.com/bmbenz/ETL-Data-Pipeline-Development-for-Car-Sharing-Data/assets/93178744/73ee83c8-6c55-4d4c-b2f9-fad764b6084f)

- Uploaded the raw data to Amazon S3 and used a crawler to extract schema and create tables on the AWS Glue Catalog, enabling easy cataloging and organization of the data
- Developed a Python script on Glue Notebook Studio using PySpark to efficiently extract, transform, and load data between S3 and Redshift, ensuring data integrity and consistency throughout the ETL process