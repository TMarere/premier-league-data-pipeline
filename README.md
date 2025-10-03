
# ⚽ Premier League Injury & Match Analytics (AWS + PySpark)

## 📌 Project Overview
This project builds an **end-to-end data pipeline** for analyzing **Premier League injuries and match data**.  
It demonstrates skills in **data engineering, cloud computing, big data analytics, and visualization** by answering key sports analytics questions such as:  

- How many injuries occur per season and per team?  
- Which players miss the most matches due to injuries?  
- Do injury trends impact team performance?  

The pipeline was implemented using **AWS S3, PySpark, Athena/Redshift, and QuickSight/Tableau**.  

---

## 🏗️ Architecture
![Architecture Diagram](architecture_diagram.png)

**Flow:**  
1. **Raw Data Ingestion** → CSVs uploaded to **AWS S3 (Raw Zone)**.  
2. **ETL Processing** → PySpark cleans & transforms data, stores results in **S3 (Processed Zone)** as Parquet.  
3. **Query Layer** → **Athena/Redshift** runs SQL queries on processed data.  
4. **Visualization** → Dashboards created in **QuickSight/Tableau**.  

---

## 📂 Repository Structure
