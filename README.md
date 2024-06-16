# Fitbit_AzureStreaming
This project showcases an end-to-end Azure streaming data solution designed to gather, process, and analyze fitness data from user Fitbit watches. The architecture leverages various Azure services and technologies to achieve seamless data ingestion, transformation, storage, and visualization.
![Project Architecture](project_image/architecture_diagram.png)

## Key Components and Technologies

### Data Collection and Ingestion
- Fitbit watch data, including user registration information and gym login/logout status, is collected and stored in Azure SQL Database.
- Continuous data streams are captured via Kafka, including user profile change data capture (CDC), BPM data, and workout session details.

### Data Processing and Transformation
- Azure Data Factory orchestrates data movement tasks, extracting data from Kafka using Kafka Connect and loading it into Azure Data Lake Storage Gen2 (ADLS Gen2).
- Data transformation processes are implemented in Azure Databricks, maintaining a medallion architecture with three layers to construct and optimize data tables in the Unity catalog.

### Data Storage and Management
- Azure SQL Database serves as the primary data repository for structured Fitbit and user data, facilitating efficient querying and data access.
- Azure Data Lake Storage Gen2 stores raw and transformed data, enabling scalable and cost-effective storage for analytics and archival purposes.

### Analytics and Visualization
- Azure AI/BI Dashboard is utilized to build comprehensive Fitbit analysis dashboards, integrating KPIs derived from the processed data.
- Insights and visualizations from the dashboard provide actionable insights into user fitness patterns, trends, and performance metrics.

## Project Goals and Outcomes
- **End-to-End Solution:** Demonstrates a complete Azure-based streaming data pipeline from data ingestion to visualization, showcasing robust integration across Azure services.
- **Scalability and Efficiency:** Utilizes cloud-native services to ensure scalability, real-time data processing, and cost-efficiency in managing large volumes of fitness data.
- **Actionable Insights:** Empowers stakeholders with actionable insights derived from Fitbit data, enabling informed decisions in health and fitness management.


## Conclusion
This project exemplifies a robust Azure streaming data solution tailored for fitness data analysis, leveraging Azure's powerful ecosystem to deliver actionable insights and drive informed decisions in health and wellness management.

