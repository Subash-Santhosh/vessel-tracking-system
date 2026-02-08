import streamlit as st
import streamlit.components.v1 as components
import pydeck as pdk
import time
from pyspark.sql import SparkSession
import os
import json
from dotenv import load_dotenv
import time

load_dotenv()

os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"

MAPBOX_TOKEN = os.getenv("MAPBOX_TOKEN")

st.set_page_config(layout="wide", initial_sidebar_state="collapsed")

# Hide Streamlit header and reduce padding
st.markdown("""
<style>
    header {visibility: hidden;}
    .block-container {padding-top: 0rem; padding-bottom: 0rem;}
    iframe {display: block;}
</style>
""", unsafe_allow_html=True)

spark = SparkSession.builder \
    .appName("vessel-tracking") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "io.delta:delta-spark_2.12:3.1.0"
    ) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

silver_path = "/tmp/delta/silver/vessel_tracking"

def status_color(status):
    if status and "Under way" in status:
        return "#059669"  # Darker green
    elif status == "At anchor":
        return "#eab308"  # Darker yellow
    elif status == "Moored":
        return "#ea580c"  # Darker orange
    elif status == "Not under command":
        return "#7c3aed"  # Darker purple
    else:
        return "#4b5563"  # Darker gray

df = spark.read.format("delta").load(silver_path)

pdf = df.toPandas()

pdf["color"] = pdf["navigationalStatus"].apply(status_color)

# Sort by timestamp to ensure proper ordering
pdf = pdf.sort_values('timeStamp', ascending=False)

# Get all vessel data (for historical trail) - keep ALL records
all_vessel_data = json.dumps(pdf.to_dict(orient="records"))

# Get latest vessel data (for main markers) - one per vessel
latest_pdf = pdf.drop_duplicates('imo', keep='first')
latest_vessel_data = json.dumps(latest_pdf.to_dict(orient="records"))

html = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Vessel Tracking System</title>

<script src="https://api.mapbox.com/mapbox-gl-js/v3.4.0/mapbox-gl.js"></script>
<link href="https://api.mapbox.com/mapbox-gl-js/v3.4.0/mapbox-gl.css" rel="stylesheet"/>

<style>
body {{ margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; }}
.main-container {{ height: 100vh; display: flex; }}
#sidebar {{ width: 360px; background: #1f2937; padding: 16px; color: white; overflow-y: auto; box-shadow: inset -1px 0 0 #374151; flex-shrink: 0; display: flex; flex-direction: column; }}
#map {{ flex-grow: 1; height: 100%; position: relative; }}

.sidebar-header {{ display: flex; align-items: center; margin-bottom: 16px; gap: 12px; }}
.sidebar-title {{ flex-grow: 1; text-align: center; font-size: 18px; color: #60a5fa; font-weight: bold; }}
#search-input {{ width: 100%; padding: 10px; border-radius: 6px; border: 1px solid #374151; font-size: 13px; box-sizing: border-box; background-color: #374151; color: #fff; margin-bottom: 16px; }}
#search-input::placeholder {{ color: #9ca3af; }}

.full-view-btn {{ background: #2563eb; color: #fff; position: absolute; top: 15px; left: 15px; z-index: 2; border: none; padding: 8px 16px; border-radius: 6px; font-size: 14px; font-weight: bold; cursor: pointer; box-shadow: 0 2px 4px rgba(0,0,0,0.2); }}
.full-view-btn:hover {{ background: #1d4ed8; }}

#vessel-list {{ overflow-y: auto; flex-grow: 1; padding-right: 6px; }}
.vessel-item {{ background: #2563eb; margin-bottom: 10px; padding: 12px; border-radius: 6px; cursor: pointer; box-shadow: 0 1px 3px rgba(0, 0, 0, 0.15); transition: transform 0.2s, background 0.2s; }}
.vessel-item:hover {{ background: #1d4ed8; transform: translateX(4px); }}
.vessel-title {{ font-size: 15px; font-weight: bold; color: #ffffff; margin-bottom: 6px; }}
.vessel-meta {{ font-size: 12px; color: #e5e7eb; margin-bottom: 3px; }}
.badge {{ display: inline-block; padding: 4px 10px; border-radius: 12px; font-weight: 600; font-size: 11px; margin-top: 6px; }}
.badge-green {{ background-color: #10b981; color: white; }}
.badge-yellow {{ background-color: #f5f10b; color: #333; }}
.badge-orange {{ background-color: #f59e0b; color: white; }}
.badge-gray {{ background-color: #6c757d; color: white; }}

.mapboxgl-popup {{
  max-width: none !important;
}}

.mapboxgl-popup-content {{
  padding: 0 !important;
  border-radius: 12px !important;
  box-shadow: 0 6px 24px rgba(0,0,0,0.12) !important;
  max-width: none !important;
  overflow: hidden !important;
}}

.mapboxgl-popup-close-button {{
  width: 32px !important;
  height: 32px !important;
  font-size: 18px !important;
  font-weight: 300 !important;
  padding: 0 !important;
  color: #666 !important;
  background: #fff !important;
  border-radius: 6px !important;
  line-height: 32px !important;
  text-align: center !important;
  transition: all 0.2s ease !important;
  border: none !important;
  box-shadow: 0 2px 6px rgba(0,0,0,0.08) !important;
  cursor: pointer !important;
  outline: none !important;
  margin: 6px !important;
}}

.mapboxgl-popup-close-button:hover {{
  background: #f5f5f5 !important;
  color: #333 !important;
}}

.vessel-card {{
  width: 320px;
  background: #fff;
  border-radius: 10px;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
  overflow: hidden;
  color: #1a1a1a;
}}

.vessel-name {{
  font-size: 16px;
  font-weight: 700;
  color: #1a1a1a;
  margin: 0;
  padding: 12px 45px 10px 14px;
  letter-spacing: -0.2px;
  line-height: 1.3;
  word-wrap: break-word;
  background: #ffffff;
  border-bottom: 1px solid #e5e7eb;
}}

.info-grid {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 8px 12px;
  padding: 12px 14px;
  background: #ffffff;
}}

.info-item {{
  padding: 0;
}}

.info-label {{
  font-size: 11px;
  font-weight: 700;
  color: #111827;
  margin-bottom: 2px;
  text-transform: uppercase;
  letter-spacing: 0.3px;
}}

.info-value {{
  font-size: 13px;
  color: #374151;
  font-weight: 500;
  line-height: 1.4;
}}

.info-value.na {{
  color: #a855f7;
}}

.badge-popup {{
  background: #fef3c7;
  color: #92400e;
  padding: 4px 10px;
  border-radius: 4px;
  font-size: 11px;
  font-weight: 600;
  display: inline-block;
  border: none;
}}
</style>
</head>

<body>
<div class="main-container">
  <div id="sidebar">
    <div class="sidebar-header">
      <span class="sidebar-title">Vessel List</span>
    </div>
    <input type="text" id="search-input" placeholder="Search vessel...">
    <div id="vessel-list"></div>
  </div>
  <div id="map">
    <button class="full-view-btn" id="full-view-btn">Home</button>
  </div>
</div>

<script>
mapboxgl.accessToken = "{MAPBOX_TOKEN}";

const latestVessels = {latest_vessel_data};
const allTrailPoints = {all_vessel_data};

let vesselMarkers = {{}};
let trailMarkers = [];
let map;

function createVesselIcon(vessel) {{
  const el = document.createElement('div');

  // Get color - if not present, determine from status
  let color = vessel.color;
  if (!color) {{
    const status = vessel.navigationalStatus || '';
    if (status.includes('Under way')) {{
      color = '#059669';
    }} else if (status === 'At anchor') {{
      color = '#eab308';
    }} else if (status === 'Moored') {{
      color = '#ea580c';
    }} else {{
      color = '#4b5563';
    }}
  }}

  const rotation = vessel.course || 0;

  el.innerHTML = `<svg width="17" height="17" viewBox="0 0 24 24" style="transform: rotate(${{rotation}}deg); filter: drop-shadow(0px 2px 3px rgba(0,0,0,0.3));"><path d="m5.767 19.838 6.259 -16.166a1.155 1.155 0 0 1 0.279 -0.345 0.909 0.909 0 0 1 0.391 -0.115c0.103 0 0.297 0.053 0.393 0.115a1.155 1.155 0 0 1 0.279 0.345l6.257 16.166a1.174 1.174 0 0 1 0.021 0.466 0.943 0.943 0 0 1 -0.237 0.353 0.936 0.936 0 0 1 -0.394 0.159 1.197 1.197 0 0 1 -0.451 -0.113L12.696 16.913l-5.866 3.79a1.205 1.205 0 0 1 -0.453 0.113 0.927 0.927 0 0 1 -0.393 -0.159 0.917 0.917 0 0 1 -0.237 -0.355 1.166 1.166 0 0 1 0.019 -0.465Zm12.206 -0.945L12.696 5.261l-5.277 13.632 4.886 -3.156a1.409 1.409 0 0 1 0.782 0Z" fill="${{color}}" /></svg>`;

  return el;
}}

function getStatusBadge(status) {{
  status = status || "N/A";
  if (status.includes("Under way")) return `<span class='badge badge-green'>${{status}}</span>`;
  if (status === "Moored") return `<span class='badge badge-orange'>${{status}}</span>`;
  if (status === "At anchor") return `<span class='badge badge-yellow'>${{status}}</span>`;
  return `<span class='badge badge-gray'>${{status}}</span>`;
}}

function getStatusBadgePopup(status) {{
  status = status || "N/A";
  let bgColor, textColor;

  if (status.includes("Under way")) {{
    bgColor = "#d1fae5";
    textColor = "#065f46";
  }} else if (status === "At anchor") {{
    bgColor = "#fef3c7";
    textColor = "#92400e";
  }} else if (status === "Moored") {{
    bgColor = "#fed7aa";
    textColor = "#9a3412";
  }} else {{
    bgColor = "#e5e7eb";
    textColor = "#374151";
  }}

  return `<span style="background: ${{bgColor}}; color: ${{textColor}}; padding: 4px 10px; border-radius: 4px; font-size: 11px; font-weight: 600; display: inline-block;">${{status}}</span>`;
}}

function createPopupContent(v) {{
  return `
    <div class="vessel-card">
      <h1 class="vessel-name">${{v.vesselName}}</h1>
      <div class="info-grid">
        <div class="info-item">
          <div class="info-label">Date/Time:</div>
          <div class="info-value">${{v.timeStamp || "N/A"}}</div>
        </div>
        <div class="info-item">
          <div class="info-label">ETA:</div>
          <div class="info-value">${{v.ETA || '<span class="na">N/A</span>'}}</div>
        </div>
        <div class="info-item">
          <div class="info-label">Current Port:</div>
          <div class="info-value">${{v.current_port || 'N/A'}}</div>
        </div>
        <div class="info-item">
          <div class="info-label">Destination:</div>
          <div class="info-value">${{v.destination || 'N/A'}}</div>
        </div>
        <div class="info-item">
          <div class="info-label">Status:</div>
          <div class="info-value">${{getStatusBadgePopup(v.navigationalStatus)}}</div>
        </div>
        <div class="info-item">
          <div class="info-label">Speed:</div>
          <div class="info-value">${{v.speed}} kts</div>
        </div>
        <div class="info-item">
          <div class="info-label">Course:</div>
          <div class="info-value">${{v.course}}°</div>
        </div>
        <div class="info-item">
          <div class="info-label">Lat:</div>
          <div class="info-value">${{v.latitude.toFixed(6)}}</div>
        </div>
        <div class="info-item">
          <div class="info-label">Lng:</div>
          <div class="info-value">${{v.longitude.toFixed(6)}}</div>
        </div>
      </div>
    </div>
  `;
}}

function showTrailForIMO(imo, currentLat, currentLng) {{
  clearTrail();

  console.log('Searching for trails for IMO:', imo);
  console.log('Current position:', currentLat, currentLng);
  console.log('Total trail points available:', allTrailPoints.length);

  // Get all points for this IMO - check both 'imo' and 'IMO' field names
  const trail = allTrailPoints.filter(p => p.imo === imo || p.IMO === imo);

  if (trail.length === 0) {{
    console.log('No trail points found for IMO:', imo);
    console.log('Available IMOs:', [...new Set(allTrailPoints.map(p => p.imo || p.IMO))]);
    return;
  }}

  console.log(`Found ${{trail.length}} trail points for IMO ${{imo}}`);

  // Sort by timestamp descending to get latest first
  trail.sort((a, b) => {{
    const timeA = new Date(a.timeStamp).getTime();
    const timeB = new Date(b.timeStamp).getTime();
    return timeB - timeA;
  }});

  // Show historical points only (exclude the current position that's already shown as arrow)
  trail.forEach((v, index) => {{
    // Skip if this point matches the current position coordinates
    const isSamePosition = (Math.abs(v.latitude - currentLat) < 0.00001 &&
                           Math.abs(v.longitude - currentLng) < 0.00001);

    if (isSamePosition) {{
      console.log('Skipping current position at index', index);
      return;
    }}

    // Historical points - show as blue circles
    const markerElement = document.createElement('div');
    markerElement.style.cssText = 'width: 10px; height: 10px; border-radius: 50%; background-color: #3b82f6; cursor: pointer;';

    const popup = new mapboxgl.Popup({{
      offset: 10,
      closeButton: true,
      closeOnClick: true,
      maxWidth: 'none'
    }}).setHTML(createPopupContent(v));

    const marker = new mapboxgl.Marker({{ element: markerElement }})
      .setLngLat([v.longitude, v.latitude])
      .setPopup(popup)
      .addTo(map);

    trailMarkers.push(marker);
  }});

  console.log(`Added ${{trailMarkers.length}} historical trail markers`);
}}

function clearTrail() {{
  trailMarkers.forEach(m => m.remove());
  trailMarkers = [];
}}

function clearAllVesselMarkers() {{
  Object.values(vesselMarkers).forEach(marker => marker.remove());
  vesselMarkers = {{}};
}}

function renderVesselList(vesselsToRender) {{
  const listContainer = document.getElementById('vessel-list');
  listContainer.innerHTML = '';
  clearAllVesselMarkers();

  vesselsToRender.sort((a, b) => {{
    const nameA = a.vesselName ? a.vesselName.toLowerCase() : '';
    const nameB = b.vesselName ? b.vesselName.toLowerCase() : '';
    return nameA.localeCompare(nameB);
  }});

  vesselsToRender.forEach(v => {{
    const popup = new mapboxgl.Popup({{
      offset: 25,
      closeButton: true,
      closeOnClick: true,
      maxWidth: 'none'
    }}).setHTML(createPopupContent(v));
    const marker = new mapboxgl.Marker({{ element: createVesselIcon(v) }})
      .setLngLat([v.longitude, v.latitude])
      .setPopup(popup)
      .addTo(map);
    vesselMarkers[v.imo] = marker;

    const item = document.createElement('div');
    item.className = 'vessel-item';
    item.innerHTML = `
      <div class='vessel-title'>${{v.vesselName}}</div>
      <div class='vessel-meta'><strong>ETA:</strong> ${{v.ETA || 'N/A'}}</div>
      <div class='vessel-meta'><strong>Destination:</strong> ${{v.destination || 'N/A'}}</div>
      <div class='vessel-meta'><strong>Status:</strong> ${{getStatusBadge(v.navigationalStatus)}}</div>
    `;
    item.onclick = () => {{
      console.log('Clicked vessel:', v.vesselName, 'IMO:', v.imo || v.IMO);
      map.flyTo({{ center: [v.longitude, v.latitude], zoom: 8, duration: 2500 }});
      showTrailForIMO(v.imo || v.IMO, v.latitude, v.longitude);
    }};
    listContainer.appendChild(item);
  }});
}}

function applyFilters() {{
  const searchQuery = document.getElementById('search-input').value.toLowerCase();

  const filteredVessels = latestVessels.filter(vessel => {{
    const matchesSearch = vessel.vesselName.toLowerCase().includes(searchQuery);
    return matchesSearch;
  }});

  renderVesselList(filteredVessels);
}}

document.addEventListener('DOMContentLoaded', () => {{
  const searchInput = document.getElementById('search-input');

  map = new mapboxgl.Map({{
    container: 'map',
    style: 'mapbox://styles/mapbox/streets-v12',
    center: [0, 20],
    zoom: 1.5
  }});

  searchInput.addEventListener('input', applyFilters);

  document.getElementById('full-view-btn').onclick = () => {{
    clearTrail();
    map.flyTo({{ center: [0, 20], zoom: 1.5, duration: 1500 }});
  }};

  applyFilters();
}});
</script>

</body>
</html>
"""

components.html(html, height=1000, scrolling=False)
