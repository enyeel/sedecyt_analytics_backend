# SEDECyT Analytics - Backend API

![Status: Production](https://img.shields.io/badge/status-production-green)
![Platform: Google Cloud Run](https://img.shields.io/badge/Platform-Google%20Cloud%20Run-blue)
![Database: Supabase](https://img.shields.io/badge/Database-Supabase%20(Postgres)-green)

This is the serverless backend API for **SEDECyT Analytics**, a modern data platform designed to automate economic analysis for the Secretariat of Economic Development, Science, and Technology (SEDECyT) of Aguascalientes.

This service is responsible for the automated extraction, cleaning, and storage of economic data, serving it via a high-speed API to its decoupled frontend dashboard.

* **View the Backend:** `sedecyt_analytics_backend` (This repo)
* **View the Frontend:** [sedecyt_analytics_frontend](https://github.com/enyeel/sedecyt_analytics_frontend)
***

## The Problem: Why This Project Exists

Currently, SEDECyT's process for generating key economic reports is manual, slow, and error-prone:
1.  **Manual Data Entry:** Data is consolidated by hand from various sources (Google Forms, spreadsheets, etc.) into a central Excel file.
2.  **Static Reports:** This data is then manually transferred to PowerPoint presentations for analysis.
3.  **Inefficiency:** This workflow consumes dozens of hours, increases the risk of human error, and makes real-time data analysis impossible.

## The Solution: An Automated Data Pipeline

This backend solves the problem by implementing a **fully automated, modular ETL (Extract, Transform, Load)** pipeline that powers a "master truth table" and generates pre-calculated analytics dashboards.

1.  **Extract:** A Python service connects to Google Sheets and other sources to pull raw, new data.
2.  **Transform:** The raw data is run through a modular, production-ready cleaning pipeline that normalizes, validates, and enriches every column (e.g., standardizing RFCs, validating phone numbers, fuzzy-matching municipalities and industrial parks, processing certifications).
3.  **Load:** The clean, analysis-ready data is loaded into a **Supabase (Postgres)** database with proper foreign key relationships.
4.  **Analytics:** A separate analytics pipeline pre-calculates chart data and stores it in optimized format for instant dashboard rendering.

The frontend dashboard then queries this clean, fast, and reliable "truth table" instead of a messy spreadsheet, allowing for instant, dynamic visualizations.

***

## Tech Stack

* **Backend:** **Python 3.11**
* **Framework:** **Flask** with Blueprint architecture
* **Containerization:** **Docker** with Gunicorn
* **Host Platform:** **Google Cloud Run** (Serverless)
* **Database:** **Supabase** (PostgreSQL)
* **Key Libraries:** 
  * **Pandas** (for data transformation)
  * **GSpread** (for Google Sheets integration)
  * **Supabase Python Client** (for database operations)
  * **RapidFuzz** (for fuzzy string matching)
  * **Unicode normalization** (for text cleaning)
* **Database Structure:** Leverages **`jsonb`** data types in Postgres to flexibly store highly variable survey data without schema changes.

***

## Project Status: ✅ Production Ready

This project is **fully deployed and operational** as part of a university internship program.

* **Core Architecture:** The decoupled two-repo structure is complete and production-ready.
* **Deployment:** The backend is successfully containerized with Docker and deployed to Google Cloud Run.
* **Database:** The Supabase Postgres schema is fully defined with proper relationships and indexes.
* **ETL Pipeline:** The data transformation pipeline is **100% complete** and modularized into separate components:
  * Data extraction from Google Sheets
  * Comprehensive data cleaning and normalization
  * Catalog matching (municipalities, industrial parks, certifications)
  * Data processing and entity separation (companies, contacts, responses)
  * Historical data tracking vs. latest snapshot
* **Analytics Pipeline:** Pre-calculation of dashboard charts with configurable analysis functions.
* **API:** All endpoints are implemented, tested, and protected with Supabase Auth token validation.

***

## 📂 Project Structure 

The project follows a clean, modular architecture with clear separation of concerns:

```bash
.
├── app/
│   ├── __init__.py              # Flask app factory
│   ├── api/
│   │   ├── routes.py            # All API endpoints (Blueprints)
│   │   └── auth_decorator.py    # Token-based authentication middleware
│   ├── core/
│   │   └── connections/
│   │       ├── supabase_service.py      # Supabase client & data operations
│   │       └── google_sheets_service.py # Google Sheets integration
│   ├── pipelines/
│   │   ├── etl/
│   │   │   ├── run.py           # Main ETL orchestrator
│   │   │   ├── cleaning.py      # Data cleaning functions (RFC, email, phone, etc.)
│   │   │   ├── processing.py    # Data processing & entity separation
│   │   │   └── certifications.py # Certification analysis & matching
│   │   └── analytics/
│   │       ├── run.py           # Analytics pipeline orchestrator
│   │       ├── analysis_functions.py # Chart data calculation functions
│   │       └── update_chart_visibility.py # Chart visibility management
│   ├── services/
│   │   └── dashboard_service.py # Dashboard data retrieval service
│   └── data/
│       ├── inputs/              # Mock data for testing
│       └── outputs/             # ETL output files (debug)
├── config/
│   ├── dashboards_config.py    # Dashboard & chart configuration
│   ├── certifications_catalog_data.py # Certifications catalog
│   ├── cleaning_map.json       # Column mapping configuration
│   └── sheets_credentials.py   # Google Sheets credentials config
├── Dockerfile
├── requirements.txt
├── run.py                       # Application entry point
└── README.md
```

***

## Getting Started (Local Development)

Instructions to get the project running locally using Docker.

### Prerequisites

* [Git](https://git-scm.com/downloads)
* [Docker Desktop](https://www.docker.com/products/docker-desktop/)
* **Supabase Project:** A running Supabase project with the required schema.
* **Google Credentials:** A `credentials.json` file for a Google Service Account (with Google Sheets API enabled).

***

### 1. Clone the Repository

```bash
git clone https://github.com/enyeel/sedecyt_analytics_backend.git
cd sedecyt_analytics_backend
```

***

### 2. Configure Environment

This project uses environment variables. Create a `.env` file in the root directory. **This file is git-ignored and should never be committed.**

```.env
# Supabase Credentials
SUPABASE_URL="https://your-project.supabase.co"
SUPABASE_SERVICE_KEY="your-supabase-service-role-key"

# Google Credentials Path (as it will be inside the container)
GOOGLE_CREDENTIALS_PATH="/app/credentials.json"

# Frontend URL for CORS (optional, for local development)
FRONTEND_URL="http://localhost:3000"
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
docker run -p 8080:8080 \
  -v ./credentials.json:/app/credentials.json \
  -v .:/app \
  --env-file .env \
  --name sedecyt-api \
  sedecyt-backend
```

The API will now be running and accessible at http://localhost:8080.

***

## API Endpoints

All endpoints are protected with Supabase Auth token validation (except `/api/health`).

### Public Endpoints

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/api/health` | Health check endpoint (used by keep-alive workflow) |

### Protected Endpoints (Require `Authorization: Bearer <token>`)

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/api/dashboards` | Gets a lightweight list of all available dashboards |
| `GET` | `/api/dashboards/<slug>` | Gets a complete dashboard with all chart data |
| `GET` | `/api/dashboards/meta` | Gets dashboard metadata (count, source) |
| `GET` | `/api/data/companies-view` | Gets formatted companies data for table view |
| `GET` | `/api/data/contacts-view` | Gets formatted contacts data for table view |
| `GET` | `/api/data/responses-view` | Gets formatted responses data (full history) for table view |
| `GET` | `/api/companies/search?q=<query>` | Searches for a company by trade name |
| `GET` | `/api/table/<table_name>` | Gets raw data from any table (admin use) |

***

## ETL Pipeline

The ETL pipeline is fully modularized and can be run independently:

### Running the ETL Pipeline

```bash
# Inside the Docker container or local environment
python -m app.pipelines.etl.run
```

**What it does:**
1. **Extracts** data from Google Sheets ("Formulario Desarrollo Industria")
2. **Cleans** all columns (RFCs, emails, phones, addresses, etc.)
3. **Matches** municipalities and industrial parks using fuzzy matching
4. **Processes** certifications (both checkbox selections and free-text)
5. **Separates** data into three entities: companies, contacts, responses
6. **Uploads** to Supabase with proper foreign key relationships
7. **Maintains** both latest snapshot (companies table) and full history (responses table)

### Running the Analytics Pipeline

```bash
# Inside the Docker container or local environment
python -m app.pipelines.analytics.run
```

**What it does:**
1. **Fetches** all required data from Supabase
2. **Enriches** data with catalog information (municipality names, park names)
3. **Calculates** chart data using configurable analysis functions
4. **Formats** data into Chart.js-compatible JSON
5. **Stores** pre-calculated charts in Supabase for instant dashboard loading

The analytics pipeline reads from `config/dashboards_config.py` to determine which charts to generate.

***

## Data Cleaning Features

The cleaning pipeline includes sophisticated data normalization:

* **RFC Validation:** Validates Mexican RFC format (12-13 characters) and handles foreign tax IDs
* **Phone Normalization:** Converts all phone numbers to E.164 format (+52 for Mexico)
* **Email Cleaning:** Normalizes emails to lowercase, removes spaces
* **Address Standardization:** Cleans and normalizes address fields
* **Fuzzy Matching:** Uses RapidFuzz for intelligent catalog matching:
  * Municipalities (with keyword support)
  * Industrial parks
  * Certifications (ISO standards)
* **Text Normalization:** Unicode normalization, accent removal, case standardization
* **Historical Tracking:** Maintains full response history while keeping latest company snapshot

***

## 👥 Collaborators

* **[Ángel](https://github.com/enyeel)** — Data processing, backend architecture & overall project design  
* **[Emilio](https://github.com/AngelGTZ28)** — API & infrastructure development (Google Cloud, Supabase integration)  
* **[Yara](https://github.com/Yara09-L)** — Frontend development & UI integration  

> _This project is part of the university internship program at SEDECYT Aguascalientes._

---

## 🔮 Future Improvements & Planned Features

* Add scheduled ETL runs via Cloud Scheduler or GitHub Actions
* Implement data validation webhooks for real-time updates
* Add export functionality (CSV/Excel) for dashboard data
* Enhance fuzzy matching with machine learning models
* Add API rate limiting and caching layers
* Implement comprehensive logging and monitoring
* Add unit and integration tests for ETL pipelines
