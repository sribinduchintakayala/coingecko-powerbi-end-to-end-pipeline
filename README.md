# Coingecko End-to-End Pipeline 

## Project Overview  
This project demonstrates an **end-to-end data engineering and analytics pipeline** for real-time cryptocurrency monitoring using **CoinGecko API**, **Google Cloud Platform (GCP)** services, and **Power BI** for visualization.  

The goal is to **automate data collection, transformation, and visualization** to analyze live crypto trends such as top-performing coins, market capitalization, and 24-hour price changes.


## Tech Stack  

| Layer | Tool / Technology | Purpose |
|-------|------------------|---------|
| Data Source | [CoinGecko API](https://www.coingecko.com/en/api/documentation) | Fetch real-time cryptocurrency data |
| Ingestion | Python (Google Colab) | Extract API data and upload to BigQuery |
| Storage | Google BigQuery | Store structured data |
| Cleaning | SQL (Google Colab) | Clean, and transform raw data |
| Automation & Scheduling | Apache Airflow (Cloud Composer) | Schedule and automate data refreshes |
| Visualization | Power BI | Build interactive dashboards |


## Pipeline Architecture  

<img width="525" height="117" alt="image" src="https://github.com/user-attachments/assets/4b750d2f-60e7-4f58-9533-71a90b81f0ab" />


**Explanation:**  
- **API & delay:** Fetched 250 rows per page for 8 pages, with 4-second delays.
- **Python/Colab:** Handles both fetching and cleaning of data.  
- **BigQuery:** Stores raw and cleaned datasets in separate tables/folders.  
- **Airflow DAG:** Automates execution every 2 hours.  
- **Power BI:** Visualizes cleaned BigQuery data in real time.


## Workflow Steps  
### 1. Data Fetching  
- Fetched live cryptocurrency data:  
- Handled **paginated API** with delays to avoid being blocked:  
- 8 pages × 250 rows/page  
- 4-second delay between pages  

### 2. Data Cleaning & Transformation  
- Performed **data cleaning in Colab**:  
- Converted timestamps to proper **datetime**  
- Fixed zero/null values in numeric fields  
- Created summary measures for visualization  

### 3. Storage in BigQuery  
- **Raw data:** Stored in a dedicated table 
- **Cleaned data:** Stored in a separate table ready for analytics  

### 4. Automation with Airflow  
- Built an **Airflow DAG** to run every **2 hours**:  
  - Fetch new API data  
  - Clean & transform  
  - Load into BigQuery  

### Visualization in Power BI  
- Connected Power BI to **cleaned BigQuery data**  
- Built visuals including:  
  - **KPI Cards:** Total Market Cap, Total 24h Volume, Avg 24h Price Change %, Coins Tracked, Last Updated  
  - **Top N Coins Bar Chart:** Controlled via slicer  
  - **Donut Chart:** Market cap distribution among top coins  
  - **Line Chart:** Daily average price trend  
  - **Table:** Top 15 coins with current price  

## Key Insights  

- Identify **top-performing coins** by market capitalization  
- Track **daily and 24-hour trends**  
- Observe **market share distribution** and **volatility**  
- Monitor **price change percentages** in real time  



