# ETL-Data-Pipeline-Development-for-Car-Sharing-Data
Designed and built a robust ETL (Extract, Transform, Load) data pipeline to transfer raw car-sharing data from the data lake (Amazon S3) to the data warehouse (Amazon Redshift) in the AWS cloud platform

![CarsharingDataPipeline](https://github.com/bmbenz/ETL-Data-Pipeline-Development-for-Car-Sharing-Data/assets/93178744/73ee83c8-6c55-4d4c-b2f9-fad764b6084f)

- Uploaded the raw data to Amazon S3 and used a crawler to extract schema and create tables on the AWS Glue Catalog, enabling easy cataloging and organization of the data
- Developed a Python script on Glue Notebook Studio using PySpark to efficiently extract, transform, and load data between S3 and Redshift, ensuring data integrity and consistency throughout the ETL process

[ETL_S3_Athena.ipynb](https://github.com/bmbenz/ETL-Data-Pipeline-Development-for-Car-Sharing-Data/blob/0161aecde25ac0ac985b351af0e7da90f5bdda30/ETL_S3_Athena.ipynb) or [ETL_S3_Athena.py](https://github.com/bmbenz/ETL-Data-Pipeline-Development-for-Car-Sharing-Data/blob/1f34fb7c77a5ce07bbcd92febfd7bd53b6856a79/ETL_S3_Athena.py): Python Script for Extracting data from Amazon S3, Transforming data on Glue Notebook Studio using PySpark, Loading data to Amazon S3, and creating tables on the AWS Glue Catalog.
