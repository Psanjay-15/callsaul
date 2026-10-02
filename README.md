# CallSaul Voice Agent

A browser-based voice agent that helps a caller reschedule a courier delivery.

This repository is being built in small, tested steps. The current foundation contains a FastAPI backend, a React/Vite frontend, and MongoDB Atlas configuration.

## Prerequisites

- Python 3.12 or newer
- Node.js 20 or newer
- A MongoDB Atlas database

## 1. Configure the application

```bash
cp .env.example .env
```

Replace `MONGODB_URI` with the connection string from MongoDB Atlas. Keep the database name as `callsaul` unless you intentionally want a different name.

No voice-provider API keys are required for the foundation step.

## 2. Start the backend

```bash
python3 -m venv server/.venv
source server/.venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r server/requirements.txt
uvicorn app.main:app --app-dir server --reload
```

The API is available at `http://localhost:8000`. Its health endpoint is `http://localhost:8000/api/health`. The response reports whether MongoDB Atlas is connected.

## 3. Start the frontend

In another terminal:

```bash
cd client
npm install
npm run dev
```

Open `http://localhost:5173`.

## Tests

With the Python virtual environment active:

```bash
python -m pytest server/tests
```
