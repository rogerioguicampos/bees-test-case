# Bees Data Engineering Test Case

This repository contains the solution for the Data Engineering test case. The project consists of a data pipeline that consumes data from the Open Brewery DB API, processes it through a **Medallion Architecture** (Bronze, Silver, Gold), and persists the results in a Data Lake using Parquet format.

------

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

This project includes a **Makefile** to automate setup, execution, testing, and Docker operations. This ensures a consistent developer experience across different environments (Linux/Arch, Mac, etc.).

### Prerequisites

* **Docker** installed and running. 
* **Make** (usually installed by default on Linux/Mac). 
* **Python 3.9+** (The project handles version management via `uv`).

### 1. Quick Start (Makefile)

To see all available commands, simply run:

```bash
make help
```

### 2. Local Development Environment

We use `uv` for fast dependency management and virtual environment creation.

- **Setup Environment:** Installs `uv`, creates a Python 3.9 virtual environment, and installs dependencies.

```bash
make setup
```

* **Run Pipeline Locally:** Executes `main.py` using the isolated environment.

```bash
make run
```

* **Run Tests:** Executes unit tests using `pytest`.

```bash
make test
```

### 3. Docker Execution (Production Simulation)

* **Build Image:** Builds the Docker image `bees-pipeline`.

```bash
make docker-build
```

* **Manual Execution (One-off):** Runs the pipeline inside the container and **automatically fixes file permissions** so you can edit the generated `data/` files on your host machine.

```bash
make docker-run
```

* **Orchestrated Execution (Cron Scheduler):** Runs the container in the background with the internal Cron scheduler active (06:00 AM daily).

```bash
make docker-up
```

* **View Logs:** Follows the logs of the background Cron container.

```bash
make docker-logs
```

* **Debug Shell:** Opens a `bash` terminal inside the container with volume mounting, allowing you to debug code inside the Docker environment.

```bash
make docker-shell
```

#### 4. Maintenance

To clean up generated data, logs, caches (`__pycache__`, `.pytest_cache`), and the virtual environment:

```bash
make clean
```

> [!NOTE]
>
> This command automatically detects if files were created by Docker (Root user) and uses `sudo` if necessary to remove them properly.

------

## 📡 Monitoring & Alerting Plan

In a production environment (e.g., AWS/Azure), the following strategy would be implemented to ensure reliability:

1. **Pipeline Failures:**
   - **Mechanism:** The entry point script matches the Python process exit code.
   - **Alerting:** If `exit_code != 0`, a webhook is triggered to send a notification to **Slack** or **Email** (via AWS SNS or PagerDuty).
2. **Logging:**
   - **Current State:** Logs are directed to `stdout` and `/var/log/cron.log`.
   - **Production:** A sidecar container (e.g., Filebeat/Fluentd) would ship these logs to **Elasticsearch/Kibana** or **CloudWatch Logs** for centralized analysis.
3. **Data Quality:**
   - **Volume Checks:** An alert is triggered if the Gold layer row count deviates by >20% compared to the moving average of the last 7 days.
   - **Schema Validation:** Using libraries like *Great Expectations* to ensure no null values in critical columns (IDs, Country) during the Silver transformation.

------

## 📂 Project Structure

```tex
.
├── data/                 # Data Lake storage (Generated locally via volumes)
│   ├── bronze/           # Raw data partitioned by date
│   ├── silver/           # Cleaned data partitioned by country
│   └── gold/             # Aggregated metrics
├── logs/                 # Execution logs (persisted from Docker)
├── tests/
│   └── test_pipeline.py  # Unit tests
├── .python-version       # Python version specification for uv
├── Dockerfile            # Container definition
├── Makefile              # Automation commands (Setup, Run, Test, Docker)
├── README.md             # Project documentation
├── main.py               # ETL Pipeline source code
└── requirements.txt      # Python dependencies
```
