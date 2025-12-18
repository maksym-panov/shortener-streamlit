# 📊 Shortener Service Analytics Dashboard

![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![Streamlit](https://img.shields.io/badge/Streamlit-1.28%2B-red)
![Status](https://img.shields.io/badge/Status-Diploma%20Work-success)

## 📝 Project Description
The application serves as a monitoring and **Data Mining** module for a URL Shortener service. It is designed to analyze large datasets (Clickstream logs), evaluate the Quality of Service (QoS), and identify user behavior patterns to optimize system performance (e.g., tuning Auto Scaling policies or Caching strategies).

## 🚀 Key Features

The application is divided into 4 key analytical modules:

### 1. 🌍 Geography & Devices
* **Interactive Map (Choropleth Map):** Visualization of global traffic distribution with a focus on target regions.
* **Sunburst Chart:** Hierarchical analysis of platform and browser popularity (Mobile/Desktop -> User Agent).

### 2. 📈 Performance & Reliability
* **Load vs. Latency:** Correlation analysis between the number of requests (RPH) and response time. This helps identify bottlenecks during peak loads.
* **Apdex Score:** Calculation of the user satisfaction index (an industry standard for measuring software performance).
* **Heatmap:** Activity heatmap (Day of Week vs. Hour of Day).

### 3. 🧠 Feature Engineering (Scientific Analysis)
* **Box Plots:** Deep analysis of latency distribution, identification of outliers, and performance dependency on the time of day (Day/Night).
* **Correlation Matrix:** Search for hidden dependencies between features to build future ML models.

### 4. 💼 Business Metrics
* **Pareto Principle (80/20):** Analysis of the "Long Tail". It demonstrates that 20% of links generate 80% of the traffic, providing a data-driven justification for implementing **Redis Cache** for "hot" records.

---

## 🛠 Tech Stack

* **Python:** Core programming language.
* **Streamlit:** Framework for building the interactive Web UI.
* **Pandas & NumPy:** Data processing, aggregation, and vectorized calculations.
* **Plotly:** Rendering interactive charts and diagrams.
* **Pycountry:** Handling ISO country codes for geospatial analysis.

---

## ⚙️ Installation & Run

### 1. Clone the repository
```bash
git clone [https://github.com/Diploma-Panov/shortener-service.git](https://github.com/Diploma-Panov/shortener-service.git)
cd shortener-service

# Windows
python -m venv venv
venv\Scripts\activate

# macOS/Linux
python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt

# Windows
python generator.py

# macOS/Linux
python3 generator.py

# Run the Dashboard
streamlit run app.py
```

# Project structure
```
├── shortener-stats.py    # Main Streamlit application file
├── generator.py          # Data generation script (Postgres + DynamoDB simulation)
├── clicks_stream.csv     # Generated click logs (gitignored)
├── urls_metadata.csv     # Generated URL metadata (gitignored)
├── requirements.txt      # List of dependencies
└── README.md             # Documentation
```