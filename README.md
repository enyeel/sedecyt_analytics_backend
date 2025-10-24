# SEDECyT Analytics - Backend API

![Status: In Development](https://img.shields.io/badge/status-in%20development-yellow)
![Platform: Google Cloud Run](https://img.shields.io/badge/Platform-Google%20Cloud%20Run-blue)
![Database: Supabase](https://img.shields.io/badge/Database-Supabase%20(Postgres)-green)

This is the serverless backend API for **SEDECyT Analytics**, a modern data platform designed to automate economic analysis for the Secretariat of Economic Development, Science, and Technology (SEDECyT) of Aguascalientes.

This service is responsible for the automated extraction, cleaning, and storage of economic data, serving it via a high-speed API to a decoupled frontend.

***

## The Problem: Why This Project Exists

Currently, SEDECyT's process for generating key economic reports is manual, slow, and error-prone:
1.  **Manual Data Entry:** Data is consolidated by hand from various sources (Google Forms, spreadsheets, etc.) into a central Excel file.
2.  **Static Reports:** This data is then manually transferred to PowerPoint presentations for analysis.
3.  **Inefficiency:** This workflow consumes dozens of hours, increases the risk of human error, and makes real-time data analysis impossible.

## The Solution: An Automated Data Pipeline

This backend solves the problem by implementing an **automated ETL (Extract, Transform, Load)** pipeline that powers a "master truth table."

1.  **Extract:** A Python service connects to Google Sheets and other sources to pull raw, new data.
2.  **Transform:** The raw data is run through a modular **Python (`clean.py`)** service that normalizes, cleans, and validates every column (e.g., standardizing names, validating phone numbers, cleaning addresses).
3.  **Load:** The clean, analysis-ready data is loaded into a **Supabase (Postgres)** database.

The frontend dashboard then queries this clean, fast, and reliable "truth table" instead of a messy spreadsheet, allowing for instant, dynamic visualizations.

***

## Tech Stack

* **Backend:** **Python 3.12**
* **Containerization:** **Docker**
* **Host Platform:** **Google Cloud Run** (Serverless)
* **Database:** **Supabase** (PostgreSQL)
* **Key Libraries:** **Pandas** (for data transformation), **GSpread** (for Google Sheets), **psycopg2-binary** (for Postgres)
* **Database Structure:** Leverages **`jsonb`** data types in Postgres to flexibly store highly variable survey data without schema changes.

***

## Project Status: 🚧 In Development

This project is actively being developed as part of a university internship.

* **Core Architecture:** The decoupled two-repo structure is in place.
* **Deployment:** The backend is successfully containerized with Docker and deployable to Google Cloud Run.
* **Database:** The Supabase Postgres schema is defined.
* **ETL Script:** The data transformation script (`clean.py`) is approximately **70% complete**.

Immediate next steps involve finalizing the cleaning modules for all data types and building out the statistical API endpoints.

***

## 📂 Project Structure 

```bash
.gitignore
Dockerfile
README.md
app/
├── init.py
├── api/
│ ├── init.py
│ └── routes.py
├── services/
│ ├── init.py
│ ├── auth_decorator.py
│ ├── data_cleaning_service.py
│ ├── data_processing_service.py
│ ├── etl_script.py
│ ├── google_sheets_service.py
│ └── supabase_service.py
certifications.txt
config/
├── init.py
├── certifications_catalog_data.py
├── cleaning_map.json
└── sheets_credentials.py
requirements.txt
run.py
test_api.http
```

***

## Getting Started (Local Development)

Instructions to get the project running locally using Docker.

### Prerequisites

* [Git](https://git-scm.com/downloads)
* [Docker Desktop](https://www.docker.com/products/docker-desktop/)
* **Supabase Project:** A running Supabase project.
* **Google Credentials:** A `credentials.json` file for a Google Service Account (with Google Sheets API enabled).

***

### 1. Clone the Repository

```bash
git clone https://github.com/AngelGTZ28/sedecyt_analytics_backend.git
cd sedecyt_analytics_backend
```

***

### 2. Configure Environment

This project uses environment variables. Create a `.env` file in the root directory. **This file is git-ignored and should never be committed.**

```.env
# Supabase Credentials
SUPABASE_URL="[https://your-project.supabase.co](https://your-project.supabase.co)"
SUPABASE_KEY="your-supabase-service-role-key"

# Google Credentials Path (as it will be inside the container)
GOOGLE_CREDENTIALS_PATH="/app/credentials.json"
```

***

### 3. Place Your Google Credentials

Place your downloaded `credentials.json` file in the root of the project. (It will be added to the Docker container by the `docker run` command).

### 4. Build and Run the Docker Container

This command builds the image and runs a container, securely mounting your local credentials file into the container where the app expects it.

```bash
# 1. Build the image
docker build -t sedecyt-backend .

# 2. Run the container
docker run -p 5000:5000 \
  -v ./credentials.json:/app/credentials.json \
  -v .:/app \
  --env-file .env \
  --name sedecyt-api \
  sedecyt-backend
```
The API will now be running and accessible at http://localhost:5000.

***

## API Endpoints (In Progress)

The following API contract is being built to serve the frontend.

| Method | Endpoint | Description | Status |
| :--- | :--- | :--- | :--- |
| `POST` | `/api/etl/run` | Triggers the full ETL pipeline. | 🚧 In Development |
| `GET` | `/api/empresas` | Gets a paginated list of all companies. | ⏳ Planned |
| `GET` | `/api/empresas/<id>` | Gets the full detail for a single company. | ⏳ Planned |
| `GET` | `/api/statistics/empresas-por-rubro` | Gets aggregated data for the "Companies by Industry" chart. | ⏳ Planned |

***

## 👥 Collaborators

- **(Ángel)[github.com/enyeel]** — Data processing, backend architecture & overall project design  
- **(Emilio)[github.com/AngelGTZ28]** — API & infrastructure development (Google Cloud, Supabase integration)  
- **(Yara)[github.com/Yara09-L]** — Frontend development & UI integration  

> _This project is part of the university internship program at SEDECYT Aguascalientes._

---

## 🔮 Future Improvements & Planned Features


- Split the ETL service into modular components  
- Add statistical calculation modules for dashboard visualizations  
- Implement additional API endpoints for frontend features  
- Integrate frontend with backend and Supabase 
(**[github.com/enyeel/sedecyt-analytics-frontend]**)    
