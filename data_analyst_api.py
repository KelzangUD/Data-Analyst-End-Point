from flask import Flask, request, jsonify
import pandas as pd
import os
from flask_cors import CORS # Import CORS

app = Flask(__name__)
DATA_FILE = "data/iot_sensor_data.csv"
# Initialize CORS
# This will allow requests from 'http://localhost:3000' to any route in your Flask app.
# You can specify methods if needed, e.g., methods=["GET", "POST"]
CORS(app, resources={r"/data*": {"origins": "http://localhost:3000"}})

# If you want to allow CORS for all origins (less secure, but quick for local development):
# CORS(app)

# If you want to allow CORS for all methods and headers from specific origin:
# CORS(app, origins="http://localhost:3000")



#ensure the data directory exists
if not os.path.exists(DATA_FILE):
    pd.DataFrame(columns=["Timestamp", "Temperature_C", "Humidity_Percent", "Light_Lux", "Motion_Detected", "Door_Open", "Power_Consumption_Watts"]).to_csv(DATA_FILE, index=False)

@app.route('/data', methods=['GET'])
def get_data():
    """Fetch the IoT sensor data."""
    try:
        df = pd.read_csv(DATA_FILE)
        return jsonify(df.to_dict(orient='records')), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003, debug=True)