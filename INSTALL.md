# Installation Guide - SEDECyT Analytics Backend

This document provides detailed installation instructions for setting up the SEDECyT Analytics backend API locally.

## Prerequisites

* [Git](https://git-scm.com/downloads)
* [Docker Desktop](https://www.docker.com/products/docker-desktop/)
* **Supabase Project:** A running Supabase project with the required schema.
* **Google Credentials:** A `credentials.json` file for a Google Service Account (with Google Sheets API enabled).

## Step-by-Step Installation

### 1. Clone the Repository

```bash
git clone https://github.com/enyeel/sedecyt_analytics_backend.git
cd sedecyt_analytics_backend
```

### 2. Configure Environment Variables

Create a `.env` file in the root directory. **This file is git-ignored and should never be committed.**

```.env
# Supabase Credentials
SUPABASE_URL="https://your-project.supabase.co"
SUPABASE_SERVICE_KEY="your-supabase-service-role-key"

# Google Credentials Path (as it will be inside the container)
GOOGLE_CREDENTIALS_PATH="/app/credentials.json"

# Frontend URL for CORS (optional, for local development)
FRONTEND_URL="http://localhost:3000"
```

### 3. Place Your Google Credentials

Place your downloaded `credentials.json` file in the root of the project. This file will be mounted into the Docker container.

### 4. Build the Docker Image

```bash
docker build -t sedecyt-backend .
```

### 5. Run the Container

```bash
docker run -p 8080:8080 \
  -v ./credentials.json:/app/credentials.json \
  -v .:/app \
  --env-file .env \
  --name sedecyt-api \
  sedecyt-backend
```

The API will now be running and accessible at http://localhost:8080.

### 6. Verify Installation

Test the health endpoint:

```bash
curl http://localhost:8080/api/health
```

You should receive a JSON response with the API status.

## Running ETL and Analytics Pipelines

### Running the ETL Pipeline

```bash
# Inside the Docker container
docker exec -it sedecyt-api python -m app.pipelines.etl.run
```

Or if running locally (without Docker):

```bash
python -m app.pipelines.etl.run
```

### Running the Analytics Pipeline

```bash
# Inside the Docker container
docker exec -it sedecyt-api python -m app.pipelines.analytics.run
```

Or if running locally:

```bash
python -m app.pipelines.analytics.run
```

## Troubleshooting

### Port Already in Use

If port 8080 is already in use, change the port mapping:

```bash
docker run -p 8081:8080 ...
```

Then access the API at http://localhost:8081.

### Google Sheets Access Issues

Ensure your `credentials.json` file:
1. Has the Google Sheets API enabled
2. Has access to the specific spreadsheet
3. Is correctly mounted in the container

### Supabase Connection Issues

Verify:
1. Your `SUPABASE_URL` is correct (no trailing slash)
2. Your `SUPABASE_SERVICE_KEY` is the service role key (not the anon key)
3. Your Supabase project is active and accessible

## Development Mode

For development with hot-reload, you can run Flask directly:

```bash
# Install dependencies locally
pip install -r requirements.txt

# Run Flask in debug mode
python run.py
```

This will run on port 8080 with debug mode enabled.

