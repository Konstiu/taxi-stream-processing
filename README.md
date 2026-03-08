# Taxi Real-Time Stream Processing System

A distributed real-time data streaming and processing system for analyzing taxi location data using Apache Kafka, Apache Storm, and Redis, with a modern web dashboard for visualization.

## 🎥 Live Demo

![Real-Time Taxi Stream Processing System](demo/demo.gif)

🎬 **Full demo video (HD):**  
<https://unterweger.tech/old/assets/vid/Taxi_Video.mp4>

## 🏗️ System Architecture

The system consists of four main components:

- **Apache Kafka**: Message broker for streaming taxi location data
- **Apache Storm**: Distributed stream processing engine for real-time analytics
- **Redis**: In-memory data store for caching taxi state and alerts
- **Web Dashboard**: Real-time visualization of taxi positions and alerts

## 📋 Prerequisites

- Docker and Docker Compose
- A `data/` folder in the project root containing data files with taxi location data
  - Supported file extensions: `.txt` or `.csv`
  - CSV format: `taxi_id, timestamp, longitude, latitude` (comma-separated)
  - Supported timestamp formats:
    - `YYYY-MM-DD HH:MM:SS` (e.g., 2024-01-15 10:30:45)
    - `YYYY/MM/DD HH:MM:SS` (e.g., 2024/01/15 10:30:45)
    - Unix epoch in seconds or milliseconds

## 🚀 Quick Start

### 1. Prepare Data

Ensure you have a `data/` folder in the project root with your CSV files (`.txt` or `.csv` extensions):

```bash
mkdir -p data
# Add your CSV files here
```

#### Download sample data (T-Drive)

You can download a real-world taxi trajectory dataset from Microsoft Research (T-Drive):

- Dataset page: [https://www.microsoft.com/en-us/research/publication/t-drive-trajectory-data-sample/](https://www.microsoft.com/en-us/research/publication/t-drive-trajectory-data-sample/)

After downloading, extract the files and place the trajectory files into the local `data/` directory so the feeder can ingest them.

### 2. Start All Services

To start all services, run:

```bash
docker compose up --build -d
```

This will:

- Start Kafka broker and topic initialization
- Start the feeder service (reads data from `data/` folder into Kafka)
- Start Storm topology for stream processing
- Start Redis for state storage
- Start the Web API and Dashboard
- Start Kafka UI for monitoring

The feeder will run as a one-shot service and exit once all data is processed.

**Optional**: Clean up before starting (removes orphaned containers):

```bash
docker compose down --remove-orphans
```

**Optional**: Monitor feeder progress in real-time:

```bash
docker compose logs -f feeder
```

### 3. Access the Dashboard

Open your browser and navigate to:

```
http://localhost:800
```

## 📊 Monitoring & Debugging

### View Kafka Topics

Open the Kafka UI in your browser:

```
http://localhost:8085/
```

### Check Live Taxi Data

Query Redis directly for current taxi state and tracking information:

```bash
# View taxi state (current position, speed, etc.)
docker exec -it redis redis-cli hgetall taxi:100:state

# View taxi track history (last 5 updates)
docker exec -it redis redis-cli lrange taxi:100:track 0 5
```

Replace `100` with the desired taxi ID.

### View Processing Logs

View all service logs together:

```bash
docker compose logs -f
```

Or monitor specific services:

Monitor the feeder progress:

```bash
docker compose logs -f feeder
```

Monitor Storm nimbus (main processing):

```bash
docker compose logs -f storm-nimbus
```

Monitor Storm supervisor:

```bash
docker compose logs -f storm-supervisor
```

View API logs:

```bash
docker compose logs -f api
```

## 🛑 Stopping the System

```bash
docker compose down
```

To remove all data and start fresh:

```bash
docker compose down --remove-orphans -v
```

## 📁 Project Structure

- `producer/`: Kafka producer that ingests CSV data from the `data/` folder
- `storm/`: Apache Storm topology for stream processing and analytics
- `webGui/api/`: FastAPI backend for serving taxi and alert data via REST/WebSocket
- `webGui/web/`: Frontend dashboard for real-time visualization
- `data/`: Input CSV files (not included in repo)
- `compose.yaml`: Docker Compose configuration for all services

## 🔧 Configuration

Edit `compose.yaml` to adjust:

- **File glob pattern**: Change `--glob` in the `feeder` service to match different file types (default is `/data/*.txt`, use `/data/*.{txt,csv}` or `/data/*` for all files)
- **Processing speed (pace)**: Modify `--pace` parameter in the `feeder` service:
  - `0` = Process data as fast as possible (no delay between records)
  - `1` = Process at recorded speed (real-time playback, sleeps based on actual timestamp deltas)
  - `0.001` = 1000x faster than recorded speed (sleep = 0.1% of time delta between records)
  - `0.5` = 2x faster (sleep = 50% of time delta)
  - `2` = 0.5x speed, twice as slow (sleep = 200% of time delta)
  
  **Formula**: `sleep_time = (next_timestamp - prev_timestamp) × pace`
- **Number of taxi records**: Set `--max-files` to limit the number of data files processed (0 = all files)
- **Kafka partitions**: Change `--partitions` in the `topic-init` service
- **Batch settings**: Adjust `--linger-ms` and `--batch-bytes` for performance tuning

## 📝 Data Format

CSV files should follow this format (tab or comma-separated):

```
taxi_id,timestamp,longitude,latitude
100,2024-01-15 10:30:45,13.404954,52.520008
100,2024-01-15 10:30:50,13.405100,52.520100
```

## 🤝 Team

See [CONTRIBUTIONS.md](report/CONTRIBUTIONS.md) for authorship and contributions.
