# Databricks 24/7 Reliability Agent

## Overview

The Databricks 24/7 Reliability Agent is an autonomous monitoring and remediation system designed to eliminate manual pipeline triage. It continuously scans Databricks Jobs and Delta Live Tables (DLT) for failures, uses GPT-4o to diagnose root causes, and applies automated fixes such as retries or targeted task repairs.

## Key Features

- **Dual-Strategy Monitoring**: Strategy A (completed run scan) and Strategy B (active transition tracker) ensure zero-loss failure detection.
- **AI-Driven Diagnostics**: Deep analysis of error traces and logs via GPT-4o.
- **Automated Remediation**: Executes run-now (retry) or repair_run (task-specific fix) based on AI confidence.
- **Resilience**: Built-in exponential backoff and LLM circuit breaker for platform stability.
- **Docker-Native**: Packaged for high-availability deployment with built-in health checks.

## Prerequisites

- Python 3.11+ (for local execution).
- Docker & Docker Compose (for containerized deployment).
- Databricks Workspace: Access to Jobs and Pipelines.
- OpenAI API Key: For GPT-4o failure analysis.

## Configuration

The agent requires a `.env` file in the root directory. Use the provided `.env.example` as a template.

```bash
# Databricks Configuration
DATABRICKS_HOST=https://adb-xxx.azuredatabricks.net
DATABRICKS_TOKEN=dapi_your_token_here

# AI Configuration
OPENAI_API_KEY=sk_your_key_here
OPENAI_MODEL=gpt-4o

# SMTP Configuration (Optional for Alerts)
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your_email@gmail.com
SMTP_PASSWORD=your_app_password
EMAIL_TO=oncall@company.com
```

## Execution

### Option 1: Local Execution

**Install Dependencies:**

```bash
pip install -r requirements.txt
```

**Run the Agent:**

```bash
python main.py
```

### Option 2: Docker Deployment (Recommended)

**Build and Start the Container:**

```bash
docker-compose up -d --build
```

**Monitor Logs:**

```bash
docker logs -f reliability-agent
```

**Check Health Status:**

```bash
curl http://localhost:8080/health
```

## Project Structure

- `main.py`: Entry point and system lifecycle management.
- `monitor.py`: Orchestrates the 60-second scan loop and failure detection.
- `analyzer.py`: Interfaces with GPT-4o for root-cause diagnosis.
- `databricks_client.py`: Hardened wrapper for Databricks REST APIs.
- `notifier.py`: Manages SMTP alerts and fix confirmations.
- `config.py`: Environment variable validation and policy enforcement.

## Security Warning

**CRITICAL:** Never commit your `.env` file to Git history.  
Ensure `.env` is listed in your `.gitignore`.  
If you accidentally staged secrets, use `git reset --soft HEAD~1` to undo the commit before pushing to a remote repository.  
Treat any pushed tokens as compromised and rotate them immediately in the Databricks and OpenAI consoles.  