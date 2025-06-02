# Retail_data_analysis

This project uses PySpark Structured Streaming to analyse transaction data streaming over a Kafka topic in order to obtain insights from real-time retail activity. In order to calculate significant KPIs, the script processes both orders and returns. The results are then made available via console output and JSON files.
To calculate various Key Performance Indicators (KPIs) for the e-commerce company RetailCorp Inc., you are provided with real-time sales data from across the globe. This dataset includes invoice details for customer orders placed worldwide.

In a typical industry setup, an end-to-end data pipeline is implemented to handle this process. Technologies like HDFS (Hadoop Distributed File System) are used to store the data, which is then processed using real-time data processing frameworks. The results are visualized on dashboards using tools such as Tableau and Power BI. The image below illustrates an example of a complete data pipeline.

![image](https://github.com/user-attachments/assets/e6648547-8c90-49fa-b67f-060fd162e476)

For our project, we will be focusing on the ‘Order Intelligence’ part of this data pipeline. The image given below shows the architecture of the data pipeline that we will follow in this project.

![image](https://github.com/user-attachments/assets/0eaf846c-7818-4dee-9118-bf6b3f9cc60e)

The following taks will be performed:
1. Reading the sales data from the Kafka server
2. Preprocessing the data to calculate additional derived columns such as total_cost etc
3. Calculating the time-based KPIs and time and country-based KPIs
4. Storing the KPIs (both time-based and time- and country-based) for a 10-minute interval into separate JSON files for further analysis
