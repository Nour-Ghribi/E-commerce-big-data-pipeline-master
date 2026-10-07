# E-commerce Big Data Pipeline

A complete Big Data analytics pipeline for processing e-commerce event data with Hadoop, Spark, Citus/PostgreSQL, and Power BI.

The project shows how raw product interaction data can be moved from distributed storage into a clean analytical warehouse model that is ready for reporting and business intelligence.

## Overview

This pipeline covers the main stages of a modern batch data engineering workflow:

1. Store raw e-commerce files in HDFS.
2. Process and clean the data with Apache Spark.
3. Build analytical dimension and fact tables.
4. Load the curated data into Citus/PostgreSQL.
5. Connect Power BI to create dashboards and insights.

## Architecture

```text
Local dataset
    |
    v
Dockerized Hadoop / HDFS
    |
    v
Apache Spark ETL
    |
    v
Citus / PostgreSQL warehouse
    |
    v
Power BI dashboards
```

## Tech Stack

- Docker and Docker Compose for local orchestration
- Hadoop HDFS for distributed storage
- Apache Spark for ETL and transformation
- CitusData PostgreSQL for distributed warehousing
- Power BI for visualization

## Data Model

The Spark job creates a simple star-schema style model:

| Table | Description |
| --- | --- |
| `dim_product` | Product, category, and brand information |
| `dim_user` | Unique users |
| `dim_category` | Category metadata |
| `fact_events` | Cleaned user events linked to dimensions |

## ETL Steps

The ETL script performs:

- removal of null rows
- whitespace trimming
- duplicate removal
- column normalization to lowercase snake case
- non-ASCII character cleanup
- date formatting
- dimension table generation
- fact table generation with foreign keys
- JDBC loading into PostgreSQL/Citus

## Project Structure

```text
.
|-- docker-compose.yml   # Hadoop, Spark and Citus services
|-- hadoop.env           # Hadoop environment configuration
|-- init.sh              # Citus/PostgreSQL replication initialization
|-- sparkcode.py         # Spark ETL job
|-- start.sh             # End-to-end pipeline launcher
`-- README.md
```

## Prerequisites

- Docker
- Docker Compose
- A local e-commerce dataset in Parquet format

By default, `start.sh` expects the dataset in a local `Data` directory:

```text
./Data/
```

If your dataset is stored elsewhere, pass its path with `DATA_DIR`:

```bash
DATA_DIR=/path/to/your/Data ./start.sh
```

## How to Run

Start the full environment and launch the ETL job:

```bash
chmod +x start.sh
./start.sh
```

The script will:

1. start Hadoop, Spark and Citus containers
2. wait until HDFS is ready
3. copy the local dataset into the namenode container
4. upload the data into HDFS
5. install the PostgreSQL JDBC driver in Spark
6. run `sparkcode.py`
7. write the warehouse tables into Citus/PostgreSQL

## Access Points

| Service | URL / Port |
| --- | --- |
| Hadoop Namenode UI | `http://localhost:9870` |
| Spark Master UI | `http://localhost:8080` |
| Citus/PostgreSQL | `localhost:55432` |

Database credentials used by the pipeline:

```text
User: postgres
Password: admin
Database: postgres
```

## Power BI Connection

After the ETL process finishes:

1. Open Power BI Desktop.
2. Choose PostgreSQL as the data source.
3. Connect using:

```text
Host: localhost
Port: 55432
Database: postgres
Username: postgres
Password: admin
```

4. Import the dimension and fact tables.
5. Build dashboards using product, category, user and event metrics.

## Useful Commands

Stop all services:

```bash
docker compose down
```

Stop services and remove volumes:

```bash
docker compose down -v
```

Check running containers:

```bash
docker compose ps
```

## Possible Dashboard Ideas

- event count by product category
- top brands by interactions
- user activity over time
- product price distribution
- conversion-style analysis by event type

## Author

[Nour Ghribi](https://github.com/Nour-Ghribi)
