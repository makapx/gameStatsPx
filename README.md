# GameStatsPx

> A real-time game statistics processing platform with machine learning capabilities

[![Docker](https://img.shields.io/badge/Docker-20.10%2B-blue.svg)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3.8%2B-green.svg)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Kafka-Latest-red.svg)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Spark-ML-orange.svg)](https://spark.apache.org/)
[![Elasticsearch](https://img.shields.io/badge/Elasticsearch-9.0.4-yellow.svg)](https://www.elastic.co/)

GameStatsPx is a distributed real-time data processing platform designed to collect, process, and analyze game statistics using modern data engineering tools and machine learning techniques.

## Table of Contents

- [Features](#-features)
- [Architecture](#-architecture)
- [Prerequisites](#-prerequisites)
- [Installation](#-installation)
- [Quick Start](#-quick-start)
- [Project Structure](#-project-structure)
- [Configuration](#-configuration)
- [Usage](#-usage)
- [Documentation](#-documentation)
- [Monitoring](#-monitoring)
- [Troubleshooting](#-troubleshooting)
- [Contributing](#-contributing)
- [License](#-license)

## Features

- **Real-time Data Pipeline**: Stream processing with Apache Kafka
- **Machine Learning**: Linear and non-linear regression models using Apache Spark
- **Distributed Architecture**: Microservices-based design with Docker
- **Data Storage**: Elasticsearch for indexing and fast querying
- **Visualization**: Kibana dashboards for real-time analytics
- **Log Processing**: Fluent Bit for centralized log aggregation
- **Monitoring**: Kafka UI for message queue monitoring
- **Scalable**: Horizontal scaling capabilities at multiple layers
- **Security**: X-Pack security with authentication and RBAC

## Architecture

```
┌─────────────┐
│   Crawler   │ ──── Scrapes game data
└──────┬──────┘
       │ logs
       ▼
┌─────────────┐
│ Fluent Bit  │ ──── Aggregates & forwards
└──────┬──────┘
       │ stream
       ▼
┌─────────────┐
│    Kafka    │ ──── Message queue (Topic: gameStatsPx)
└──────┬──────┘
       │ consume
       ▼
┌─────────────┐
│    Spark    │ ──── ML predictions (Linear & Non-linear regression)
└──────┬──────┘
       │ results
       ▼
┌──────────────┐
│Elasticsearch │ ──── Storage & indexing
└──────┬───────┘
       │
       ▼
┌─────────────┐
│   Kibana    │ ──── Visualization
└─────────────┘
```

### Components

| Component | Description | Port |
|-----------|-------------|------|
| **Games Crawler** | Data collection service | - |
| **Fluent Bit** | Log aggregation and forwarding | 9090 |
| **Apache Kafka** | Message broker (KRaft mode) | 9092 |
| **Kafka UI** | Web-based Kafka monitoring | 8585 |
| **Apache Spark** | ML processing engine | - |
| **Elasticsearch** | Search and analytics engine | 9200 |
| **Kibana** | Data visualization platform | 5601 |
| **Python Init Script** | Dataset initialization | - |

For a detailed architecture diagram, see [architecture](./docs/architecture.png).

## Prerequisites

### System Requirements

- **OS**: Linux, macOS, or Windows with WSL2
- **CPU**: 4 cores minimum
- **RAM**: 8GB minimum (16GB recommended)
- **Disk**: 20GB free space
- **Network**: Stable internet connection

### Software Requirements

- [Docker](https://www.docker.com/) 20.10+
- [Docker Compose](https://docs.docker.com/compose/) 1.29+
- [Python](https://www.python.org/) 3.8+
- [Git](https://git-scm.com/)

## Installation

### 1. Clone the Repository

```bash
git clone git@github.com:makapx/gameStatsPx.git
cd gameStatsPx
```

## ⚡ Quick Start

### 1. Start All Services

```bash
docker-compose up -d
```

This command will start all services in detached mode:
- Kafka (with KRaft)
- Kafka UI
- Fluent Bit
- Elasticsearch
- Kibana
- Spark
- Games Crawler

### 2. Verify Services Are Running

```bash
docker-compose ps
```

All services should show status as "Up".

### 3. Initialize Dataset

Before running the initialization script, you need to set up a Python virtual environment:

```bash
# Navigate to the games-crawler directory
cd games-crawler

# Create a virtual environment
python3 -m venv venv

# Activate the virtual environment
# On Linux/macOS:
source venv/bin/activate
# On Windows:
# venv\Scripts\activate

# Install required dependencies
pip install elasticsearch requests

# Run the initialization script
python dataset_gen.py

# Deactivate virtual environment when done
deactivate

# Return to project root
cd ..
```

### 4. Access Web Interfaces

- **Kafka UI**: http://localhost:8585
- **Kibana**: http://localhost:5601
- **Elasticsearch**: http://localhost:9200

#### Default Credentials

**Elasticsearch:**
- Username: `elastic`
- Password: `changeme`

**Kibana:**
- Username: `kibana_system_user`
- Password: `kibanapass123`

## Project Structure

```
gameStatsPx/
├── README.md
├── docker-compose.yml          # Main orchestration 
├── README.md                   # This file
├── games-crawler/              # Data collection 
│   ├── Dockerfile
│   ├── dataset_gen.py          # Dataset 
│   ├── games.py                # Crawler
├── spark/                      # Apache Spark ML 
│   ├── Dockerfile
│   ├── data/
├── fluent-bit/                 # Log processing 
│   └── fluent-bit.conf
└── docs/                       # Documentation
    ├── architecture.svg        # Architecture diagram
    ├── doc.ipynb               # Jupyter presentation
```

## Configuration

### Kafka Configuration

Kafka is configured in **KRaft mode** (no Zookeeper required).

- **Broker ID**: 1
- **Topic**: `gameStatsPx` (auto-created)
- **Partitions**: 1
- **Replication Factor**: 1

### Elasticsearch Configuration

- **Cluster Mode**: Single-node
- **Heap Size**: 512MB (Xms/Xmx)
- **Security**: X-Pack enabled
- **Port**: 9200

### Spark Configuration

The Spark service implements:
- **Linear Regression**: For linear relationships in game statistics
- **Non-Linear Regression**: For complex patterns and predictions

Output datasets are stored in `./spark/data/container_data/`.

## Usage

### Starting Services

```bash
# Start all services
docker-compose up -d

# Start specific service
docker-compose up -d kafka
```

### Stopping Services

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (WARNING: deletes data)
docker-compose down -v
```

### Viewing Logs

```bash
# View all logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f spark
docker-compose logs -f kafka
docker-compose logs -f elasticsearch
```

### Restarting Services

```bash
# Restart all services
docker-compose restart

# Restart specific service
docker-compose restart kafka
```

### Rebuilding Containers

```bash
# Rebuild all containers
docker-compose build

# Rebuild specific container
docker-compose build crawler

# Rebuild and restart
docker-compose up -d --build
```

## Documentation

### Viewing the Jupyter Presentation

The project includes a comprehensive Jupyter notebook presentation.

```bash
# Navigate to docs directory
cd docs

# Create a virtual environment
python3 -m venv venv

# Activate the virtual environment
# On Linux/macOS:
source venv/bin/activate
# On Windows:
# venv\Scripts\activate

# Install Jupyter
pip install jupyter

# Start Jupyter
jupyter notebook doc.ipynb

# Deactivate when done
deactivate
```

### Converting to Slideshow

You can convert the notebook to a presentation:

```bash
jupyter nbconvert doc.ipynb --to slides --post serve
```
## Monitoring

### Kafka Monitoring

Access Kafka UI at http://localhost:8585 to:
- View topics and partitions
- Monitor consumer groups
- Check message throughput
- Inspect message content

### Elasticsearch Health

```bash
# Check cluster health
curl -u elastic:changeme http://localhost:9200/_cluster/health?pretty

# List indices
curl -u elastic:changeme http://localhost:9200/_cat/indices?v

# Get cluster stats
curl -u elastic:changeme http://localhost:9200/_cluster/stats?pretty
```

### Kibana Dashboards

1. Navigate to http://localhost:5601
2. Log in with credentials
3. Go to **Management** → **Stack Management** → **Index Patterns**
4. Create index patterns for your data
5. Use **Discover** and **Dashboard** to visualize data

## Troubleshooting

### Services Not Starting

**Check Docker:**
```bash
docker --version
docker-compose --version
```

**Check logs:**
```bash
docker-compose logs [service-name]
```

### Elasticsearch Connection Refused

**Verify Elasticsearch is running:**
```bash
docker-compose ps elasticsearch
```

**Check health:**
```bash
curl http://localhost:9200/_cluster/health
```

**Fix permissions:**
```bash
sudo chown -R 1000:1000 esdata
docker-compose restart elasticsearch
```

### Kafka Connection Issues

**Check if Kafka is running:**
```bash
docker exec kafka kafka-topics --list --bootstrap-server localhost:39092
```

**View Kafka logs:**
```bash
docker-compose logs -f kafka
```

### Python Script Errors

**Ensure virtual environment is activated:**
```bash
# In games-crawler directory
source venv/bin/activate  # Linux/macOS
# or
venv\Scripts\activate  # Windows
```

**Verify dependencies:**
```bash
pip list | grep -E "elasticsearch|requests"
```

### Port Conflicts

If ports are already in use, modify `docker-compose.yml`:

```yaml
ports:
  - "8585:8080"  # Change 8585 to another port
```

### Memory Issues

If services crash due to memory:

**Increase Docker memory:**
- Docker Desktop → Settings → Resources → Memory
- Increase to at least 8GB

**Reduce Elasticsearch heap:**
```yaml
environment:
  - "ES_JAVA_OPTS=-Xms256m -Xmx256m"  # Reduce from 512m
```