# Bees Data Engineering Test Case

This repository contains the solution for the Data Engineering test case. The project consists of a data pipeline that consumes data from the Open Brewery DB API, processes it through a **Medallion Architecture** (Bronze, Silver, Gold), and persists the results in a Data Lake using Parquet format.

## 🛠️ Technologies Used

* **Language:** Python 3.9+
* **Libraries:** Pandas, Requests, PyArrow, Pytest
* **Containerization:** Docker
* **Orchestration:** Cron (running inside Docker)
* **Format:** Parquet (Columnar storage)

---

## 🏗️ Architecture & Design Choices

### 1. Data Lake Architecture (Medallion)
The pipeline follows the medallion architecture patterns to organize data quality and progression:

* **Bronze Layer (Raw):**
    * Ingests raw JSON data from the API.
    * **Transformation:** Adds a `date_request` column for partitioning.
    * **Storage:** Parquet, partitioned by extraction date.
* **Silver Layer (Curated):**
    * Reads from Bronze.
    * **Transformation:** Cleans IDs (whitespace removal) and ensures schema consistency.
    * **Storage:** Parquet, partitioned by `date_request` and `country` (as requested in the instructions to handle skew/partitioning logic).
* **Gold Layer (Analytical):**
    * Reads from Silver.
    * **Transformation:** Aggregates data to count breweries per type, country, and state.
    * **Storage:** Parquet, partitioned by `date_request`.

### 2. Orchestration Strategy
**Tool Chosen:** Docker + Cron.

**Trade-off Analysis:**
While tools like **Airflow** or **Mage** are powerful for complex dependencies, they introduce significant overhead (databases, webservers, schedulers) for a single pipeline script.
* **Decision:** I chose to wrap the Python script in a **Docker container managed by Cron**.
* **Benefit:** This satisfies the requirement for scheduling and containerization while keeping the solution lightweight, portable, and easy to review without setting up a complex environment.

---

## 🚀 How to Run

This project utilizes **Docker** to ensure a reproducible environment.

### Prerequisites
* Docker installed on your machine.

### 1. Build the Docker Image
Build the image containing the environment, dependencies, and the application code:

```bash
docker build -t bees-pipeline .
```

### Execution Modes

You can run the pipeline in two ways: Manual (for immediate results) or Orchestrated (simulating production).

#### Option A: Manual Trigger (Immediate Execution)

Use this command to run the ETL process immediately. We use a volume (-v) mapping so the data generated inside the container appears in your local data/ folder.

```bash
# Creates a local 'data' folder and runs the script inside the container
docker run --rm -v $(pwd)/data:/app/data bees-pipeline python main.py
```

*After running this, check the newly created data/ folder in your project root to verify the Bronze, Silver, and Gold layers.*

#### Option B: Orchestrated Mode (Scheduled via Cron)

To run the container in the background with the internal **Cron** scheduler active (configured to run at **06:00 AM daily**):

```bash
docker run -d --name bees-etl -v $(pwd)/data:/app/data bees-pipeline
```

To view the execution logs in this mode:

```bash
docker exec bees-etl tail -f /var/log/cron.log
```



