# OrcaFlow - Intelligent Workload Orchestration System

An empirical distributed computing system for submitting, monitoring, and executing real Apache Spark workloads with integrated real-time analytics dashboard.

## Pipeline Overview

This project is organized as a 4-part pipeline for data processing and job orchestration:

### 1. Data Preprocessing Pipeline
**Script**: `orcaflow/jobs/sample_pyspark.py`

**Output**: `data/processed_music_data/`

Processes raw transaction data and prepares it for distributed analysis. Generates analytics datasets with the following components:
- Transaction records (100,000+ records)
- Customer spending analysis
- Category-based aggregations
- High-value transaction detection

### 2. Job Submission & Dashboard
**Components**: `orcaflow/ui/dashboard.html` + `orcaflow/api/start_server.py`

**Features**:
- Real-time job submission interface
- Live cluster metrics (CPU, Memory, Disk usage)
- Job queue visualization
- Top running jobs leaderboard
- Activity timeline with trend analysis

Interactive dashboard for:
- Submitting batch analytics workloads
- Monitoring resource utilization
- Tracking job progress (0-100%)
- Viewing active jobs and completion statistics

### 3. Real Spark Execution Engine
**Script**: `orcaflow/api/spark_executor.py`

**Output**: `data/spark_jobs/` - Job execution logs and metrics

Manages the complete lifecycle of PySpark jobs:
- **Job Submission**: Subprocess-based job launching with isolated process groups
- **Monitoring**: Background thread tracking with real-time metrics collection
- **Resource Tracking**: CPU usage, memory consumption, execution time
- **Status Management**: Complete job state from submission to completion

Features:
- Automatic job output capture (logs stored in `/tmp/orcaflow_jobs/`)
- Metrics extraction: records processed, execution time, resource utilization
- Job cancellation with graceful shutdown
- Process tracking with PID management

### 4. REST API & Cluster Management
**Server**: `orcaflow/api/start_server.py`

**Endpoints**:

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/` | GET | Serve interactive dashboard |
| `/health` | GET | Health check |
| `/api/cluster/status` | GET | Get cluster metrics and job statistics |
| `/api/jobs/submit` | POST | Submit new PySpark job |
| `/api/jobs` | GET | List all jobs (filterable by status) |
| `/api/jobs/{job_id}` | GET | Get specific job status |
| `/api/jobs/{job_id}/output` | GET | Get job output/logs |
| `/api/jobs/{job_id}/cancel` | POST | Cancel running job |

## Quick Start

### Prerequisites
- Python 3.9+
- Java 21+ (for Spark)
- 4GB+ RAM

### Installation

```bash
# 1. Clone and navigate
git clone <repository-url>
cd orcaflow

# 2. Create virtual environment
python -m venv .venv
.venv\Scripts\activate  # Windows
source .venv/bin/activate  # Linux/Mac

# 3. Install dependencies
pip install -r requirements.txt

# 4. Start server
cd orcaflow/api
python start_server.py
```

### Access

- **Dashboard**: http://localhost:8000
- **API Docs**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health

## Job Submission Example

### Via Dashboard
1. Fill form: Dataset size, complexity, memory, CPU cores
2. Click "Submit Job"
3. Monitor progress on dashboard

### Via API
```bash
curl -X POST "http://localhost:8000/api/jobs/submit" \
  -H "Content-Type: application/json" \
  -d '{
    "job_type": "batch_analytics",
    "dataset_size_mb": 2500,
    "code_complexity_score": 7,
    "memory_requirement_mb": 2048,
    "cpu_requirement_cores": 4,
    "priority": 5
  }'
```

## Architecture

```
┌─────────────────────────────────────────┐
│   OrcaFlow Dashboard (Interactive UI)   │
│    Real-time metrics & job control      │
└────────────────┬────────────────────────┘
                 │
        ┌────────v────────┐
        │  REST API Server │
        │ (FastAPI + Uvicorn)
        └────────┬────────┘
                 │
    ┌────────────v──────────────┐
    │  Spark Job Executor       │
    │ - Subprocess Management   │
    │ - Process Monitoring      │
    │ - Metrics Collection      │
    └────────────┬──────────────┘
                 │
     ┌───────────v──────────────┐
     │ Real PySpark Execution   │
     │ - Job Processing         │
     │ - Data Analytics         │
     │ - Result Generation      │
     └──────────────────────────┘
```

## Project Structure

```
orcaflow/
├── api/
│   ├── start_server.py          ✅ Main API server (consolidated)
│   ├── spark_executor.py        ✅ Job execution engine
│   ├── models.py                ✅ Data models
│   └── requirements.txt         ✅ API dependencies
├── jobs/
│   └── sample_pyspark.py        ✅ Sample Spark job template
├── ui/
│   └── dashboard.html           ✅ Interactive dashboard
├── monitoring/
│   └── prometheus.yml           ✅ Metrics configuration
└── ml-classifier/
    ├── train.py                 ✅ Model training
    └── inference.py             ✅ Model inference

root/
├── README.md                    📖 This file
├── requirements.txt             📦 Root dependencies
├── startup.py                   ⚙️ Automated setup script
└── .gitignore                   🔒 Git configuration
```

## Configuration

### Environment Variables
```bash
# Server
PORT=8000
HOST=0.0.0.0

# Java/Spark
JAVA_HOME=/path/to/java21
SPARK_HOME=/path/to/spark
```

### Spark Tuning (in `sample_pyspark.py`)
```python
.config("spark.driver.memory", "2g")
.config("spark.executor.memory", "2g")
.config("spark.executor.cores", "2")
.config("spark.sql.shuffle.partitions", "200")
```

## Key Features

✅ **Real PySpark Execution** - Not simulated, actual distributed computing
✅ **Live Dashboard** - Real-time metrics and job monitoring
✅ **REST API** - Complete job management endpoints
✅ **Background Monitoring** - Non-blocking job tracking
✅ **Resource Tracking** - CPU, memory, execution time metrics
✅ **Job Cancellation** - Graceful shutdown with PID management
✅ **Comprehensive Logging** - Structured logs for debugging
✅ **Type Safety** - Full type hints and documentation

## Performance Characteristics

### Throughput
- **Job Submission**: <100ms per job
- **Metrics Update**: 5-second dashboard refresh interval
- **Status Query**: <50ms per job

### Scaling
- **Concurrent Jobs**: Limited by system resources and Spark configuration
- **Memory**: 2GB driver + executor memory (configurable)
- **CPU**: 2 executor cores per job (configurable)

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Java version error | Install Java 21+: `pip install appmod-install-jdk` |
| Spark not found | Set `SPARK_HOME` or install via pip |
| Port in use | Change PORT env var or kill process on port 8000 |
| Job fails | Check `/tmp/orcaflow_jobs/{job_id}/job.log` |

## Development

### Code Quality
```bash
# Format code
pip install black
black orcaflow/

# Check style
pip install flake8
flake8 orcaflow/

# Run tests (if available)
pytest tests/
```

### Debugging
Enable debug logging by setting in `start_server.py`:
```python
logging.basicConfig(level=logging.DEBUG)
```

## Contributing

1. Fork repository
2. Create feature branch: `git checkout -b feature/xyz`
3. Commit: `git commit -am 'Add feature'`
4. Push: `git push origin feature/xyz`
5. Submit pull request

## Performance Tips

1. **Tune Shuffle Partitions**: Adjust based on data size
2. **Memory Configuration**: Match available RAM
3. **Job Batching**: Group small jobs for efficiency
4. **Dashboard Refresh**: 5 seconds optimal for responsiveness

## Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| API Framework | FastAPI | 0.104.1 |
| ASGI Server | Uvicorn | 0.24.0 |
| Distributed Computing | PySpark | 4.1.1 |
| JVM Runtime | Java | 21+ |
| System Monitoring | psutil | 5.9.6 |
| Data Validation | Pydantic | 2.5.0 |

## License

MIT License - See LICENSE file for details

## Support

- 📖 **Documentation**: See [DEVELOPMENT.md](DEVELOPMENT.md)
- 🐛 **Issues**: GitHub Issues
- 💬 **Discussions**: GitHub Discussions
- 📚 **API Docs**: http://localhost:8000/docs (when server running)

## Changelog

### v1.0.0 (Current)
- ✅ Real PySpark job execution
- ✅ REST API with full CRUD operations
- ✅ Interactive web dashboard
- ✅ Real-time metrics monitoring
- ✅ Job lifecycle management
- ✅ Background job monitoring
- ✅ Comprehensive error handling
- ✅ Production-ready code with type hints

---

**Status**: Production Ready ✅ | **Last Updated**: April 2026 | **Maintainer**: OrcaFlow Team
| Aryan Yadav | API/Integration | ay3140@nyu.edu |

## References

- [Google Borg: Large-scale Cluster Management](https://research.google/pubs/large-scale-cluster-management-at-google-with-borg/)
- [Kubernetes Documentation](https://kubernetes.io/docs/)
- [Apache Spark Scheduling](https://spark.apache.org/docs/latest/job-scheduling.html)
- [gRPC Guide](https://grpc.io/docs/guides/)

## License

MIT License - See LICENSE file for details

---

**Last Updated**: April 23, 2026  
**Version**: 1.0.0-alpha
