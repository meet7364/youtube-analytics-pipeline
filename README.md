# YouTube Analytics Pipeline (ETL + Metabase)

A production-grade, containerized data pipeline that extracts YouTube channel, video, and comment data, loads it into Supabase (PostgreSQL), and visualizes it using a local Metabase instance.

Optimized for **macOS Apple Silicon (M1/M2/M3)**.

## 🏗 Architecture

```mermaid
graph LR
    A[YouTube Data API] -->|Extract| B(Python ETL Container)
    B -->|Transform| B
    B -->|Load| C[(Supabase PostgreSQL)]
    C -->|Connect| D[Metabase (Docker)]
    D -->|Visualize| E[Dashboards]
```

### Components
1.  **Python ETL**:
    -   Run via Docker using `python:3.11-slim` and `uv`.
    -   Idempotent execution: Upserts data to avoid duplicates.
    -   Collects: Channel stats, Video stats, Comments, Unified Metrics.
2.  **Database (Supabase)**:
    -   Cloud PostgreSQL.
    -   Stores raw tables (`youtube_channels`, `youtube_videos`) and unified metrics (`youtube_metrics`).
    -   Provides SQL Views (`analytics.*`) for reporting.
3.  **Visualization (Metabase)**:
    -   Local Docker container running on port `3000`.
    -   Connects directly to Supabase.

## 🚀 Setup & Installation

### Prerequisites
-   Docker Desktop (ensure it's running).
-   Supabase Project (Connection string).
-   YouTube Data API Key.
-   `git` installed.

### 1. Clone & Configure
```bash
git clone <repo_url>
cd youtube-analytics-pipeline

# Create config from template
cp .env.example .env
```

**Edit `.env`** with your credentials:
```ini
YOUTUBE_API_KEY=your_google_api_key
CHANNEL_IDS=comma,separated,channel_ids
DB_HOST=aws-0-us-west-1.pooler.supabase.com
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres.username
DB_PASSWORD=your_password
```

### 2. Run the Stack
Use the unified Docker Compose to start Metabase and build the ETL image:
```bash
docker-compose up -d
```
*Note: This starts Metabase and prepares the ETL service.*

### 3. Initialize Database
Prepare the Supabase database (create tables and views). Run this command (using the ETL container):
```bash
# Initialize schema (Warning: Drops existing compatible tables to ensure fresh schema)
docker-compose run --rm etl uv run python etl/scripts/init_db.py
```

### 4. Run ETL Pipeline
Execute the extraction and loading process manually:
```bash
docker-compose run --rm etl
# OR explicitly:
# docker-compose run --rm etl uv run python etl/src/main.py
```

### 5. Access Metabase
1.  Open [http://localhost:3000](http://localhost:3000).
2.  Complete the initial Metabase setup (create admin account).
3.  **Connect Database**:
    -   Select **PostgreSQL**.
    -   **Host**: Your Supabase Host (`DB_HOST`).
    -   **Database Name**: `postgres` (or as configured).
    -   **Username/Password**: From your `.env`.
    -   **Use a secure connection (SSL)**: Required for Supabase.
4.  **Explore Data**:
    -   Go to "Browse Data".
    -   You will see tables: `youtube_channels`, `youtube_videos`, etc.
    -   You will see views in `analytics` schema (e.g., `channel_summary`).

## 📊 Analytics Dashboards
Recommended dashboards to create in Metabase:

1.  **Channel Overview**:
    -   Use `analytics.channel_summary`.
    -   Metrics: Total Views, Subscriber Count.
2.  **Top Videos**:
    -   Use `analytics.video_performance`.
    -   Table: Sort by `view_count` descending.
3.  **Engagement**:
    -   Plot `like_count` vs `view_count` scatter.
    -   Comments analysis using `youtube_comments` table.

## 🛠 Development
The project uses `uv` for minimal and fast dependency management.

**Folder Structure**:
```
project-root/
├── etl/                # Python ETL Application
│   ├── src/            # Source code (extract, transform, load)
│   ├── sql/            # Schema and Views
│   ├── scripts/        # Init DB and utility scripts
│   └── Dockerfile      # Container definition
├── metabase/           # Metabase configuration
├── docker-compose.yml  # Root orchestration
└── .env.example        # Config template
```
