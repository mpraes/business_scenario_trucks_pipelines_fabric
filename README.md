# Data Engineering Pipelines created on Ms Fabric

## Project Overview
This project aims to analyze and understand the factors influencing ice cream truck sales and operations. By correlating sales data with weather patterns, geographical information, and economic indicators like GDP and fuel prices, it seeks to provide actionable insights for optimizing business strategies. The solution leverages Microsoft Fabric to build a scalable data analytics platform, enabling efficient data processing and analysis.

## Project Structure
```
business_scenario_trucks_pipelines_fabric/
├── architecture/                    # Architecture diagrams and documentation
│   └── ice_trucks_project (2).png  # System architecture diagram
├── notebooks_spark/                 # Jupyter notebooks for development and analysis
├── pipelines_data_factory/         # Data pipeline definitions (subdirectories for each pipeline)
└── README.md                       # Project documentation
```

## Key Components
- **Data Ingestion**: Collection of raw data from diverse sources. This includes:
    - Extracting transactional data from an Azure SQL Database (simulating an ERP backend) using Azure Data Factory (ADF) pipelines.
    - Fetching external data such as economic indicators (GDP, inflation), fuel prices, and Big Mac index from various sources (CSV, XLS files in a data lake) via ADF pipelines.
    - Retrieving weather data from APIs using Spark notebooks (e.g., `br_notebook_api_weather.ipynb`).
- **Data Processing**: Transformation of raw ingested data into a structured and queryable format. This is primarily achieved using Apache Spark notebooks within the `notebooks_spark/` directory. Key processing steps include:
    - Cleaning and standardizing data from various sources (e.g., `sl_cleaning_dim_geography.ipynb`).
    - Enriching sales data by integrating it with geographical, weather (e.g., `sl_dim_fct_weather.ipynb`), and economic indicators (e.g., `sl_gdp_rate_data.ipynb`, `sl_road_fuel_data.ipynb`).
    - Performing aggregations and calculations to create meaningful features and fact tables (e.g., `sl_fct_sales_table.ipynb`).
- **Data Storage**: Data is organized within Microsoft Fabric's OneLake, following a Lakehouse architecture.
    - **Bronze Layer**: Stores raw data as ingested from various sources (e.g., outputs of ADF pipelines, raw API data from `br_notebook_api_weather.ipynb`), providing an unaltered historical record.
    - **Silver Layer**: Contains cleaned, validated, and integrated data. This layer holds the key dimensional models (e.g., `dim_geography`, `dim_products`, `dim_trucks` from notebooks like `sl_cleaning_dim_geography.ipynb`, `sl_dim_products.ipynb`) and fact tables (e.g., `fct_sales_table` from `sl_fct_sales_table.ipynb`) created by the Spark notebooks. In this project, the Silver layer is curated to be directly consumable for business intelligence and analytics, effectively serving the role of a Gold layer.
- **Data Analysis**: The curated data in the Silver layer is made available for consumption through business intelligence platforms. End-users can connect tools like Power BI or Tableau to this data to generate reports, create dashboards, and perform interactive analysis to derive insights on ice truck sales, operational efficiency, and market trends.
- **Documentation**: Key project documentation includes:
    - This `README.md` file: Provides an overview of the project, its structure, components, and setup.
    - Architecture diagrams: Located in the `architecture/` directory (e.g., `ice_trucks_project (2).png`), illustrating the system design and data flow.

## Architecture
The overall system architecture is depicted in the diagram below, located in the `architecture` directory. This diagram illustrates the data flow from various sources, through ingestion and processing stages within Microsoft Fabric, to the final consumption layer for analytics and reporting.

![Project Architecture](architecture/ice_trucks_project%20(2).png)

Key aspects shown include data ingestion using Azure Data Factory and Spark notebooks, data transformation using Spark notebooks, data storage in OneLake (Bronze and Silver layers), and connections to BI tools like Power BI and Tableau.

## Spark Notebooks (`notebooks_spark/`)
The `notebooks_spark/` directory contains Apache Spark notebooks used for various data ingestion, transformation, and enrichment tasks:

- **`br_notebook_api_weather.ipynb`**: Ingests raw weather data from an external API and stores it in the Bronze layer.
- **`sl_cleaning_dim_geography.ipynb`**: Cleans, transforms, and prepares the master data for the geography dimension table (Silver layer).
- **`sl_data_big_mac.ipynb`**: Processes and prepares Big Mac index data, likely used for economic analysis and enrichment (Silver layer).
- **`sl_dim_fct_weather.ipynb`**: Creates weather-related dimension tables (e.g., temperature bands, weather conditions) and potentially links weather data to fact tables (Silver layer).
- **`sl_dim_products.ipynb`**: Cleans, transforms, and prepares the master data for the product dimension table (Silver layer).
- **`sl_dim_trucks.ipynb`**: Cleans, transforms, and prepares the master data for the truck dimension table (Silver layer).
- **`sl_fct_sales_table.ipynb`**: Builds the central sales fact table by integrating various dimension tables (e.g., geography, product, weather, trucks) and transaction data (Silver layer).
- **`sl_gdp_rate_data.ipynb`**: Processes and prepares GDP (Gross Domestic Product) rate data, providing economic context for analysis (Silver layer).
- **`sl_road_fuel_data.ipynb`**: Processes and prepares road fuel price data, which can be used to analyze the impact of operational costs (Silver layer).

## Data Factory Pipelines (`pipelines_data_factory/`)
This directory contains Azure Data Factory (ADF) pipelines, primarily responsible for the initial ingestion of raw data into the Bronze layer of the data lake. These pipelines handle copying data from various sources:

- **SQL Database Ingestion**: A set of pipelines (e.g., `br_copy_sqldb_cities_table`, `br_copy_sqldb_countries_table`, `br_copy_sqldb_products_table`, `br_copy_sqldb_producttypes_table`, `br_copy_sqldb_saleitems_table`, `br_copy_sqldb_sales_table`) are dedicated to extracting data from the source Azure SQL Database, which simulates an ERP system.
- **External Data File Ingestion**: Pipelines like `br_copy_adls_big_mac_source_data`, `br_copy_adls_euro_tax_source_data`, `br_copy_adls_imf_economy_source_data`, and `br_copy_adls_road_fuel_source_data` are used to ingest data from various external files (e.g., CSV, XLS) representing public data like economic indicators, tax information, and fuel prices. These files are copied into Azure Data Lake Storage (ADLS).
- **Orchestration**: The `br_pipe_master_ohter_data_adls` pipeline likely serves as a master pipeline to orchestrate the execution of several data ingestion tasks for the ADLS sources.

These ADF pipelines are crucial for ensuring that all necessary raw data is available in the lakehouse for subsequent processing by Spark notebooks.

## Prerequisites
- A Microsoft Fabric enabled workspace.
- Sufficient permissions to create and run Data Factory pipelines, Spark notebooks, and Lakehouse artifacts (tables, files) within your Fabric workspace.
- Configured connections in Fabric to any external data sources, such as an Azure SQL Database. Source connection details (e.g., server names, database names, credentials) and data lake paths for external files will need to be updated within the pipelines and notebooks to match your environment.

## Setup and Usage
1.  **Deploy Artifacts**:
    *   **Data Factory Pipelines**: Import the JSON definitions from the `pipelines_data_factory/` subdirectories into your Fabric workspace's Data Factory.
    *   **Spark Notebooks**: Upload or import the `.ipynb` files from the `notebooks_spark/` directory into your Fabric workspace.
2.  **Configure Connections and Parameters**:
    *   Review and update connection details within the imported Data Factory pipelines to point to your specific source systems (e.g., Azure SQL server details, API keys if applicable).
    *   Modify parameters within Spark notebooks (e.g., file paths, database names) to align with your Fabric environment and Lakehouse structure.
3.  **Initial Data Ingestion (Bronze Layer)**:
    *   Execute the Azure Data Factory pipelines. Start with pipelines that ingest data from Azure SQL Database (e.g., `br_copy_sqldb_cities_table`, `br_copy_sqldb_sales_table`, etc.).
    *   Run pipelines that ingest external data into ADLS (e.g., `br_copy_adls_big_mac_source_data`, `br_copy_adls_road_fuel_source_data`). The `br_pipe_master_ohter_data_adls` might orchestrate some of these.
    *   Run the `br_notebook_api_weather.ipynb` Spark notebook to fetch and store raw weather data.
4.  **Data Processing (Silver Layer)**:
    *   Execute the Spark notebooks prefixed with `sl_` in the `notebooks_spark/` directory. It's generally advisable to run them in an order that respects dependencies, for example:
        1.  Dimension cleaning and creation notebooks (e.g., `sl_cleaning_dim_geography.ipynb`, `sl_dim_products.ipynb`, `sl_dim_trucks.ipynb`).
        2.  Notebooks processing other supplementary data (e.g., `sl_data_big_mac.ipynb`, `sl_gdp_rate_data.ipynb`, `sl_road_fuel_data.ipynb`, `sl_dim_fct_weather.ipynb`).
        3.  Finally, the fact table creation notebook (e.g., `sl_fct_sales_table.ipynb`).
5.  **Data Analysis and Consumption**:
    *   Once the Silver layer tables are populated in your Lakehouse, connect your preferred BI tools (e.g., Power BI, Tableau) to the Lakehouse SQL endpoint or directly to the tables to build reports, dashboards, and perform analysis.

## Technology Stack
- Microsoft Fabric

## Sources
- Azure SQL Database (simulating an Back-end 3NF normal form system ERP sales)
- APIs (Weather)
- Data Lake files (csv, xls) which has some public data like gdp, fuel prices, inflation, etc.

## Stage
- When implemented in a Fabric environment, this project artefacts is in the stage of just consuming with reports like Power BI or Tableau.


