# 🚢 Real-Time Vessel Tracking System

A production-ready real-time vessel tracking system built with Apache Kafka, PySpark Structured Streaming, Delta Lake, and Streamlit. Features interactive Mapbox visualization with historical vessel trails, status-based color coding, and port proximity detection.

![Vessel Tracking Dashboard](output.png)

## 🏗️ Architecture

```
Navtor API → Kafka Producer → Kafka Topic → PySpark Streaming → Delta Lake (Bronze)
                                                                        ↓
                                                            PySpark Transformation
                                                                        ↓
                                                            Delta Lake (Silver)
                                                                        ↓
                                                            Streamlit Dashboard
```

## ✨ Features

### Data Pipeline
- **Real-time Data Ingestion**: Continuous vessel data streaming from Navtor API via Kafka
- **Bronze Layer**: Raw data ingestion with Spark Structured Streaming
- **Silver Layer**: Enriched data with:
  - AIS navigational status mapping (15 status codes)
  - Port proximity detection using Haversine distance
  - UN/LOCODE port name resolution
  - Current port identification for anchored/moored vessels
  - Destination parsing and enrichment

### Interactive Dashboard
- **Interactive Map**: Mapbox GL JS with street-level visualization
- **Vessel Markers**: Custom SVG arrow icons that rotate based on vessel heading
- **Status-Based Colors**:
  - 🟢 Green: Under way using engine
  - 🟡 Yellow: At anchor
  - 🟠 Orange: Moored
  - ⚫ Gray: Other statuses
- **Historical Trails**: Blue circle markers showing vessel movement history
- **Rich Tooltips**: 2-column grid layout with 9 data points per vessel
- **Search & Filter**: Real-time vessel search by name
- **Sidebar Navigation**: Scrollable vessel list with status badges
- **Smooth Animations**: Fly-to transitions and hover effects

## 🛠️ Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Stream Processing | Apache Kafka | 2.3.0 |
| Data Processing | PySpark | 3.5.7 |
| Storage | Delta Lake | 3.1.0 |
| Visualization | Streamlit | 1.54.0 |
| Mapping | Mapbox GL JS | 3.4.0 |
| Language | Python | 3.10+ |

## 📋 Prerequisites

- Python 3.10 or higher
- Java 17 (for PySpark)
- Apache Kafka (running on `localhost:9092`)
- Mapbox account and API token
- Navtor API credentials (username, password, client_id, client_secret)

## 🚀 Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd "Vessel Tracking System"
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

**Key dependencies:**
- `pyspark==3.5.7` - Distributed data processing
- `delta-spark==3.1.0` - Delta Lake integration
- `kafka-python==2.3.0` - Kafka producer/consumer
- `streamlit==1.54.0` - Web dashboard framework
- `pandas` - Data manipulation
- `python-dotenv` - Environment variable management
- `requests` - API calls

### 3. Configure Environment Variables

Create a `.env` file in the `config/` directory (copy from `.env.example`):

```bash
cp config/.env.example config/.env
```

Edit `config/.env` with your credentials:

```env
# Mapbox Token
MAPBOX_TOKEN=your_mapbox_token_here

# Navtor API Credentials
NAVTOR_USERNAME=your_username
NAVTOR_PASSWORD=your_password
NAVTOR_CLIENT_ID=your_client_id
NAVTOR_CLIENT_SECRET=your_client_secret
NAVTOR_TOKEN_URL=https://api.navtor.com/token
NAVTOR_STATUS_URL=https://api.navtor.com/vessel/status
```

### 4. Setup Data Files

Download required reference data:

1. **UN/LOCODE Code List**: Place `code-list.csv` in the project root
   - Contains country and location codes for port name resolution

2. **Ports JSON**: Place `ports.json` in the project root
   - Contains port coordinates (latitude, longitude, city, country)
   - Used for port proximity detection

### 5. Start Kafka

Ensure Kafka is running on `localhost:9092`:

```bash
# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka Broker (in separate terminal)
bin/kafka-server-start.sh config/server.properties

# Create topic (optional - auto-created by producer)
bin/kafka-topics.sh --create --topic vessel-tracking-system \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

## 📊 Running the Pipeline

### Step 1: Start the Kafka Producer

Fetches vessel data from Navtor API and sends to Kafka every 5 minutes:

```bash
python producer/api_to_kafka.py
```

**Features:**
- Automatic token refresh on 401 errors
- Rate limit handling (429 errors)
- Retry logic for authentication failures
- Continuous data streaming with 300-second intervals

**Output:**
```
Received Navtor Token.
Received Navtor Data.
✓ Sent 15 vessels to Kafka topic
```

### Step 2: Start Bronze Layer Ingestion

Reads from Kafka and writes raw data to Delta Lake bronze layer:

```bash
python ingestion/kafka_to_bronze.py
```

**Features:**
- Spark Structured Streaming
- Append-only writes to Delta Lake
- Checkpoint management for fault tolerance
- Schema: `key (STRING)`, `value (STRING)`, `createdAt (TIMESTAMP)`

**Delta Path:** `/tmp/delta/bronze/vessel_tracking`

### Step 3: Start Silver Layer Transformation

Processes bronze data with enrichment and writes to silver layer:

```bash
python processing/bronze_to_silver.py
```

**Transformations:**
1. **JSON Parsing**: Extracts vessel attributes from raw strings
2. **Status Mapping**: Converts numeric codes (0-15) to human-readable status
3. **Destination Parsing**: Extracts country and LOCODE from destination strings
4. **Port Lookup**: Joins with UN/LOCODE data for port names
5. **Proximity Detection**: Calculates Haversine distance to nearest port
6. **Current Port Identification**: Assigns port for anchored/moored vessels

**Delta Path:** `/tmp/delta/silver/vessel_tracking`

**Output Schema:**
```
imo (INT), vesselName (STRING), latitude (DOUBLE), longitude (DOUBLE),
course (DOUBLE), speed (DOUBLE), navigationalStatus (STRING),
timeStamp (STRING), ETA (STRING), destination (STRING), current_port (STRING)
```

### Step 4: Launch the Dashboard

```bash
streamlit run dashboard/streamlit_app.py
```

The dashboard will open at `http://localhost:8501`

## 🎯 Using the Dashboard

### Main Features

1. **Vessel List (Left Sidebar)**
   - Scrollable list of all vessels
   - Search bar for filtering by vessel name
   - Each card shows: ETA, Destination, Status badge
   - Click any vessel to:
     - Fly to vessel location on map
     - Display historical trail

2. **Interactive Map**
   - Arrow markers show current vessel positions
   - Arrow rotation indicates vessel heading
   - Color indicates navigational status
   - Click markers to view detailed tooltip

3. **Historical Trails**
   - Blue circle markers for past positions
   - Latest position shown as arrow (not duplicated)
   - Sorted by timestamp (newest to oldest)
   - Click historical points for historical data

4. **Vessel Tooltips**
   - Compact 320px card with 2-column grid
   - 9 data points:
     - Date/Time, ETA, Current Port, Destination
     - Status (with colored badge), Speed, Course
     - Latitude, Longitude
   - Styled close button (top-right)

5. **Home Button**
   - Top-left "Home" button
   - Clears historical trails
   - Resets map to global view

## 📁 Project Structure

```
Vessel Tracking System/
├── producer/
│   └── api_to_kafka.py          # Kafka producer - API to Kafka
├── ingestion/
│   └── kafka_to_bronze.py       # Spark streaming - Kafka to Bronze
├── processing/
│   └── bronze_to_silver.py      # Transformation - Bronze to Silver
├── dashboard/
│   └── streamlit_app.py         # Streamlit dashboard with Mapbox
├── config/
│   └── .env.example             # Environment variables template
├── requirements.txt              # Python dependencies
├── code-list.csv                # UN/LOCODE reference data
├── ports.json                   # Port coordinates data
└── README.md                    # This file
```

## 🐛 Troubleshooting

### Common Issues

**1. Kafka Connection Error**
```
Error: unable to connect to localhost:9092
```
**Solution:** Ensure Kafka is running and accessible on port 9092

**2. Java Home Not Set**
```
Error: JAVA_HOME is not set
```
**Solution:** Set JAVA_HOME in your environment or update the path in scripts:
```python
os.environ["JAVA_HOME"] = "/path/to/java/home"
```

**3. Delta Lake Not Found**
```
Error: Delta table not found
```
**Solution:** Ensure bronze layer is running before starting silver layer

**4. Mapbox Token Invalid**
```
Error: Unauthorized - Invalid token
```
**Solution:** Verify your Mapbox token in `.env` file

**5. Port Data Missing**
```
Error: File not found - ports.json
```
**Solution:** Download and place `ports.json` and `code-list.csv` in project root

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## Author

**Subash Santhosh B**  
Data Engineer

