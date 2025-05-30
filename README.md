# Data Engineering Pipelines created on Ms Fabric

## Project Overview
This project implements data engineering pipelines using Microsoft Fabric for processing and analyzing ice truck data. The solution leverages modern data engineering practices and Microsoft's Fabric platform for efficient data processing and analytics. This is kind of Lakehouse Architecture, but some silver table should be like gold layers to people who consumes.

## Project Structure
```
business_scenario_trucks_pipelines_fabric/
├── architecture/                    # Architecture diagrams and documentation
│   └── ice_trucks_project.png      # System architecture diagram
├── data/                           # Data storage and processing
│   ├── raw/                        # Raw data files
│   ├── processed/                  # Processed data files
│   └── curated/                    # Curated data for analysis
├── notebooks/                      # Jupyter notebooks for development
├── pipelines_data_factory/         # Data pipeline definitions
├── scripts/                        # Utility scripts and tools
└── README.md                       # Project documentation
```

## Key Components
- **Data Ingestion**: Raw data collection and initial processing
- **Data Processing**: ETL pipelines for data transformation
- **Data Storage**: Organized data storage in different layers
- **Data Analysis**: Tools and notebooks for data analysis
- **Documentation**: Architecture and implementation details

## Technology Stack
- Microsoft Fabric

## Sources
- Azure SQL Database (simulating an Back-end 3NF normal form system ERP sales)
- APIs (Weather)
- Data Lake files (csv, xls) which has some public data like gdp, fuel prices, inflation, etc.

## Stage
- When implemented in a Fabric environment, this project artefacts is in the stage of just consuming with reports like Power BI or Tableau.


