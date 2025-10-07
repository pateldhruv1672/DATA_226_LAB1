# DATA_226_LAB1

An end-to-end mini data platform for **DATA 226**, combining:
- 🧩 **Airflow** for ETL/ELT pipeline orchestration  
- ⚙️ **FastAPI** backend service for API and data integration  
- 💻 **React** frontend for visualization and interaction  
- 🐳 **Docker Compose** for running the full stack locally  

> This README corresponds to the `future_work_integration` branch.

---

## 🗂️ Project Structure

```
.
├── dags/                  # Airflow DAGs for ETL pipelines
├── stock-app/
│   ├── backend/           # FastAPI backend
│   └── frontend/          # React app (npm)
└── docker-compose.yaml    # Orchestration for Airflow + backend + frontend
```

---

## ✅ Requirements

- **Docker Desktop** (or Docker Engine + Compose v2)
- **Python 3.10+** (for local backend dev)
- **Node.js 18+ / npm 9+** (for frontend dev)
- Optional: `make`, `.env` file for configuration

---

## 🔐 Environment Variables

Create a `.env` file in the project root if not already present:

```bash
# Airflow
AIRFLOW_UID=50000
AIRFLOW_GID=0

# Backend API
BACKEND_PORT=8000

# Frontend
FRONTEND_PORT=3000

# (Optional) External API keys / database credentials
# ALPHA_VANTAGE_API_KEY=...
# DATABASE_URL=postgresql+psycopg2://user:pass@db:5432/appdb
```

---

## 🚀 Quickstart

### 1️⃣ Clone & Checkout

```bash
git clone https://github.com/pateldhruv1672/DATA_226_LAB1.git
cd DATA_226_LAB1
git checkout future_work_integration
```

### 2️⃣ Setup Airflow Folders (first time)

```bash
mkdir -p ./airflow/{dags,logs,plugins}
```

### 3️⃣ Start Services

```bash
docker compose up -d --build
```

Then visit:
- Airflow → [http://localhost:8080](http://localhost:8080)
- FastAPI backend → [http://localhost:8000/docs](http://localhost:8000/docs)
- React frontend → [http://localhost:3000](http://localhost:3000)

> Default Airflow login: `airflow / airflow`

---

## ⚙️ Local Development (without Docker)

### Backend (FastAPI)
```bash
cd stock-app/backend
python -m venv .venv
source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Run locally
uvicorn main:app --reload --port 8000
```

### Frontend (React)
```bash
cd stock-app/frontend
npm install
npm start
```
Frontend runs at [http://localhost:3000](http://localhost:3000)  
and proxies API requests to the FastAPI backend.

---

## 🧭 Airflow ETL Pipelines

- Located under `dags/`
- Designed for data extraction, transformation, and loading (e.g., stock data)
- Use your preferred connections (API, DB, CSV, etc.)

**Typical DAG structure:**
```python
@task
def extract() -> pd.DataFrame: ...
  
@task
def transform(df: pd.DataFrame) -> pd.DataFrame: ...
  
@task
def load(df: pd.DataFrame) -> None: ...
```

**Usage:**
1. Start Airflow via Docker  
2. Open Airflow UI → [http://localhost:8080](http://localhost:8080)  
3. Enable and trigger your DAG(s)

---

## 🧩 Integration Flow

```
Data Source (API/CSV)
        ↓
   Airflow DAG
        ↓
 FastAPI Backend (serves processed data)
        ↓
 React Frontend (visualizes data)
```

---

## 🧪 Testing & Linting

### Backend
```bash
pytest -q
ruff check .
ruff format .
```

### Frontend
```bash
npm test
npm run lint
```

---

## 🔧 Common Docker Commands

```bash
# Stop containers
docker compose down

# Rebuild images after code changes
docker compose build

# View logs for each service
docker compose logs -f webserver
docker compose logs -f backend
docker compose logs -f frontend
```

---

## 🗺️ Future Work (Integration Branch)

- Integrate backend API with Airflow outputs  
- Add persistent database (PostgreSQL or Snowflake)  
- Implement authentication for API and frontend  
- Introduce caching and job status endpoints  
- Enhance UI with charts and filters (React + Chart.js / Recharts)  
- Add CI/CD (GitHub Actions for tests & linting)

---

## 📄 License

MIT (or your preferred license)

---

## 🙌 Credits

- **Apache Airflow** for orchestration  
- **FastAPI** for backend APIs  
- **React** for frontend UI  
- **Docker Compose** for environment orchestration
