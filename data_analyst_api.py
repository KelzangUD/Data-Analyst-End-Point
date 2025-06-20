from flask import Flask, request, jsonify
import pandas as pd
import os
from flask_cors import CORS # Import CORS

app = Flask(__name__)
DATA_FILE = "data/iot_sensor_data.csv"
# Initialize CORS
CORS(app)
CORS(app, origins="http://localhost:3000")

df = pd.read_csv(DATA_FILE)

def clean_data(df):
    expected_types = {
        'Timestamp': 'datetime64[ns]',
        'Temperature_C': 'float64',
        'Humidity_Percent': 'float64',
        'Light_Lux': 'float64',
        'Motion_Detected': 'int64',
        'Door_Open': 'int64',
        'Power_Consumption_Watts': 'float64'
    }
    df_updated = df.copy()
    # Convert mismatched columns
    print("\n--- Initial Type Conversion Check ---")
    for column, expected_type in expected_types.items():
        actual_type = str(df_updated.dtypes.get(column))
        if actual_type != expected_type:
            try:
                if "datetime" in expected_type:
                    df_updated[column] = pd.to_datetime(df_updated[column], errors='coerce')
                else:
                    df_updated[column] = df_updated[column].astype(expected_type)
            except Exception as e:
                return f"Failed to convert '{column}': {e}"

    if 'Timestamp' in df_updated.columns:
        df_updated = df_updated.set_index('Timestamp')
    else:
        return "Warning: 'Timestamp' column not found to set as index. Is it already the index or named differently?"

    nan_in_index = df_updated.index.isna().sum()

    if nan_in_index > 0:
        df_updated = df_updated[df_updated.index.notna()] 
    else:
        return "No NaT values found in the Timestamp index."

    df_cleaned = df_updated.copy()
    return df_cleaned

#ensure the data directory exists
if not os.path.exists(DATA_FILE):
    pd.DataFrame(columns=["Timestamp", "Temperature_C", "Humidity_Percent", "Light_Lux", "Motion_Detected", "Door_Open", "Power_Consumption_Watts"]).to_csv(DATA_FILE, index=False)

@app.route('/data', methods=['GET'])
def get_data():
    """Fetch the IoT sensor data."""
    try:
        return jsonify(df.to_dict(orient='records')), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/summary', methods=['GET'])
def get_summary():
    """Fetch Summary IoT sensor data."""
    try:
        return jsonify(clean_data(df).to_dict(orient='records')), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003, debug=True)