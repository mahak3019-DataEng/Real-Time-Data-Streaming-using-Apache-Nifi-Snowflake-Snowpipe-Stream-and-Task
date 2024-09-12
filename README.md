# Real-Time Data Streaming Using Apache NiFi, AWS S3, Snowpipe, Stream, and Task

### Overview
This project sets up a real-time data streaming pipeline that integrates Apache NiFi, AWS S3, Snowpipe, and Snowflake to process and store data efficiently. The pipeline generates random data using Python, uploads it to AWS S3, and uses Snowpipe to load and process the data in Snowflake. The pipeline also incorporates Slowly Changing Dimension (SCD) Type 2 techniques to manage historical data updates.

### Project Components
Docker Container: Hosts Apache NiFi and JupyterLab.
Data Generation: Uses Python’s Faker library to create random data.
Data Ingestion: Moves data from local storage to AWS S3 using Apache NiFi.
Data Processing: Utilizes Snowpipe to load data from S3 into Snowflake.
Data Transformation: Applies SCD Type 2 techniques for historical data management.
Streaming and Tasks: Uses Snowflake Streams and Tasks to handle real-time updates.
Getting Started

### Prerequisites
Docker: Ensure Docker is installed and running.
Snowflake Account: Required for Snowpipe and Snowflake Streams.
AWS Account: Necessary for AWS S3 storage.
Python: For generating data in JupyterLab.
### Setup
Clone the Repository


1. git clone <repository-url>

cd <repository-directory>

2. Start Docker Container

Ensure Docker is running and start the container with Apache NiFi and JupyterLab:


docker-compose up

The docker-compose.yml file should be configured to spin up Apache NiFi and JupyterLab.

3. Generate Random Data

Access JupyterLab at http://localhost:4888.
Use the provided Jupyter notebook to generate random data.
Example Python code for data generation:

python

from faker import Faker
import pandas as pd

fake = Faker()
data = [fake.profile() for _ in range(100)]

df = pd.DataFrame(data)
df.to_csv('random_data.csv', index=False)
Save the generated file in the Jupyter notebook directory.

4. Access Apache NiFi

Open a terminal and run:

docker exec -i -t nifi bash
Locate the generated data file in the NiFi container.

5. Configure Apache NiFi

Access NiFi UI at http://localhost:2080/nifi.
Create a NiFi data flow:
Use processors to read files from NiFi's local directory.
Use processors to upload files to an AWS S3 bucket.
6. Set Up AWS S3 and Snowpipe

Configure your AWS S3 bucket to receive files from NiFi.
Configure Snowpipe to load data from the S3 bucket into a Snowflake raw table.
7. Data Transformation in Snowflake
Create tables:

Customer Table: For current customer data.
Customer History Table: For historical data using SCD Type 2.
Use Snowflake’s merge functionality to upsert data from the raw table into the customer table.

Create tasks to automate the merging process.

8. Streaming and Task Creation

Create a stream on the customer table to capture all updates.
Create a view to manage and apply SCD Type 2 to the customer history table based on the customer table's updates.