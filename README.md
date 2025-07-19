# 📰 NYT Article Count Analysis with Historical API & Linear Regression

This project consists of two Jupyter notebooks designed to collect and analyze article publication data from the New York Times using its Archive API, and apply linear regression to detect publication trends.

## 📘 Notebooks

### 1. `Historical API.ipynb`
- Uses the New York Times Archive API to fetch article metadata by month/year
- Parses the JSON results and extracts article counts
- Collects time series data for further analysis
- Requires an NYT API key

### 2. `Linear Regression.ipynb`
- Loads publication data (e.g., count by date) from BigQuery or a CSV
- Cleans and converts dates to ordinal format
- Uses scikit-learn's `LinearRegression` to fit a trend line over time
- Visualizes historical publication trends

## 🔐 Security Note

API keys and service account credentials must **never be hardcoded**.  
Use environment variables or configuration files instead. For example:

```python
import os
nyt_api_key = os.getenv("NYT_API_KEY")
```

Or load your BigQuery credentials securely:
```python
from google.cloud import bigquery
client = bigquery.Client()  # With GOOGLE_APPLICATION_CREDENTIALS env var
```

## 🚀 Setup & Dependencies

```bash
pip install pandas numpy matplotlib scikit-learn google-cloud-bigquery requests
```

Ensure your Google Cloud and NYT API credentials are properly configured in your environment.

## 📌 License

This project is for academic and research use. Use of the NYT API is subject to their terms of service.
