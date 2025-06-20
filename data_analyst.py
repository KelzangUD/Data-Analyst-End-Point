# import libraries
import pandas as pd;

# read the csv file
df = pd.read_csv("C:/Users/user/Desktop/learning/GovTech-IoT Training/6-20-2025/iot_sensor_data.csv");

# display data
print("First 5 rows of the dataset:");
print(df.head(5));
print("\nLast 5 rows of the dataset:");
print(df.tail(5));

# check data types of all columns
expected_types = {
    'Timestamp': 'datetime64[ns]',
    'Temperature_C': 'float64',
    'Humidity_Percent': 'float64',
    'Light_Lux': 'float64',
    'Motion_Detected': 'int64',
    'Door_Open': 'int64',
    'Power_Consumption_Watts': 'float64'
}
# Create a copy of the DataFrame to store the updated data
df_updated = df.copy()

# Convert mismatched columns
print("\n--- Initial Type Conversion Check ---")
for column, expected_type in expected_types.items():
    actual_type = str(df_updated.dtypes.get(column))
    if actual_type != expected_type:
        print(f"Converting '{column}' from '{actual_type}' to '{expected_type}'")
        try:
            if "datetime" in expected_type:
                df_updated[column] = pd.to_datetime(df_updated[column], errors='coerce')
            else:
                df_updated[column] = df_updated[column].astype(expected_type)
        except Exception as e:
            print(f"Failed to convert '{column}': {e}")
print("--- End Initial Type Conversion Check ---")


# --- NEW IMPORTANT STEP: Set 'Timestamp' as the DataFrame index ---
print("\n--- Setting 'Timestamp' as DataFrame index ---")
if 'Timestamp' in df_updated.columns:
    df_updated = df_updated.set_index('Timestamp')
    print("Timestamp column successfully set as index.")
else:
    print("Warning: 'Timestamp' column not found to set as index. Is it already the index or named differently?")
print(f"Index type after setting: {df_updated.index.dtype}")
print(f"Is index a DatetimeIndex after setting? {isinstance(df_updated.index, pd.DatetimeIndex)}")
print("--- End Index Setting ---")


# The updated data is now in 'df_updated'
print("\nDataFrame after type conversion and index setting:")
print(df_updated.head()) # Show head after index is set

# 1. Check if there are any NaT values in the index (now that it's the Timestamp)
nan_in_index = df_updated.index.isna().sum()
print(f"\nNumber of NaT values in the Timestamp index: {nan_in_index}")

# 2. If there are NaT values, drop rows where the index is NaT
if nan_in_index > 0:
    print(f"Dropping {nan_in_index} rows with NaT in the Timestamp index...")
    # This will drop rows where the index itself is NaT, or where any other column is NaN
    # A cleaner way for just index NaT: df_updated = df_updated[df_updated.index.notna()]
    df_updated = df_updated[df_updated.index.notna()] # Use this more explicit way
    print("Rows with NaT in Timestamp index dropped.")
else:
    print("No NaT values found in the Timestamp index.")

# Now, assign the potentially cleaned DataFrame to a new variable, e.g., df_cleaned
df_cleaned = df_updated.copy() # Good practice to work on a copy after major cleaning steps

print("\nFirst few rows of df_cleaned after checking for NaT in index:")
print(df_cleaned.head())


# Get a summary of numerical columns
numerical_summary = df_cleaned.describe()
print("\nSummary of numerical columns:")
print(numerical_summary)

# Check for missing values in each column
missing_values = df_cleaned.isnull().sum()

# Print the number of missing values per column
print("\nMissing values per column (before interpolation/fillna):")
print(missing_values)

# Additionally, you can check the total number of missing values in the entire DataFrame
total_missing_values = df_cleaned.isnull().sum().sum()
print(f"\nTotal number of missing values in the dataset (before interpolation/fillna): {total_missing_values}")

# You can also check the percentage of missing values
missing_percentage = (df_cleaned.isnull().sum() / len(df_cleaned)) * 100 # Use len(df_cleaned) here
print("\nPercentage of missing values per column (before interpolation/fillna):")
print(missing_percentage)

# Step 4: Handle missing values
# df_cleaned = df_cleaned.copy() # Already copied above. No need to copy again immediately.

# --- REMOVED: df_cleaned.dropna(subset=['Timestamp'], inplace=True) ---
# This line is no longer needed/correct because 'Timestamp' is now the index.
# Any NaT values in the index would have been handled by df_updated = df_updated[df_updated.index.notna()] above.

# Interpolate continuous sensor data
continuous_cols = ['Temperature_C', 'Humidity_Percent', 'Light_Lux', 'Power_Consumption_Watts']
for col in continuous_cols:
    if df_cleaned[col].isna().any(): # Only interpolate if there are NaNs
        print(f"Interpolating missing values in '{col}'...")
        df_cleaned[col] = df_cleaned[col].interpolate(method='linear')
    else:
        print(f"No missing values in '{col}'. Skipping interpolation.")

# Handle binary sensor data
# Fill with mode for Motion_Detected
if 'Motion_Detected' in df_cleaned.columns and df_cleaned['Motion_Detected'].isna().any():
    mode_motion = df_cleaned['Motion_Detected'].mode()
    if not mode_motion.empty:
        print("Filling missing 'Motion_Detected' with mode...")
        df_cleaned['Motion_Detected'].fillna(mode_motion[0], inplace=True)
    else:
        print("Mode for 'Motion_Detected' is empty. Cannot fill missing values.")
else:
    print("No missing values or 'Motion_Detected' column not found. Skipping fillna for 'Motion_Detected'.")


# Forward fill Door_Open status
if 'Door_Open' in df_cleaned.columns and df_cleaned['Door_Open'].isna().any():
    print("Forward filling missing 'Door_Open' values...")
    df_cleaned['Door_Open'].fillna(method='ffill', inplace=True)
    # If the first value is NaN, ffill won't work. Consider bfill after ffill or a default.
    if df_cleaned['Door_Open'].isna().any(): # Check if NaNs still exist after ffill (e.g., at the start)
        print("Remaining NaNs in 'Door_Open' after ffill, attempting backfill.")
        df_cleaned['Door_Open'].fillna(method='bfill', inplace=True)
else:
    print("No missing values or 'Door_Open' column not found. Skipping fillna for 'Door_Open'.")


# Optional: Re-check if any missing values remain
missing_summary = df_cleaned.isnull().sum()
print("\nMissing values after all cleaning steps:\n", missing_summary)

# Assuming df_cleaned is your DataFrame with the Timestamp index

# Identify numerical and binary columns
numerical_cols = ['Temperature_C', 'Humidity_Percent', 'Light_Lux', 'Power_Consumption_Watts']
binary_cols = ['Motion_Detected', 'Door_Open']

# Create a dictionary for aggregation specifications
aggregation_rules = {col: 'mean' for col in numerical_cols}
aggregation_rules.update({col: 'sum' for col in binary_cols})

# --- Verify index type RIGHT BEFORE RESAMPLING (again, for final check) ---
print(f"\nType of df_cleaned.index BEFORE RESAMPLING: {df_cleaned.index.dtype}")
print(f"Is df_cleaned.index a DatetimeIndex BEFORE RESAMPLING? {isinstance(df_cleaned.index, pd.DatetimeIndex)}")


try:
    # Resample the data to an hourly frequency using 'h' (lowercase)
    df_hourly = df_cleaned.resample('h').agg(aggregation_rules)

    # Display the first few rows of the hourly resampled data
    print("\nHourly Resampled Data (Mean for Numerical, Sum for Binary):")
    print(df_hourly.head())

    # Check the shape to see the number of hourly entries
    print(f"\nShape of the hourly resampled DataFrame: {df_hourly.shape}")

except Exception as e:
    print(f"\nAn error occurred during resampling: {e}")
    print("Please carefully review the full output, especially the index type checks.")

try:
    # Resample the data to a daily frequency
    df_daily = df_cleaned.resample('D').agg(aggregation_rules)

    # Display the first few rows of the daily resampled data
    print("\nDaily Resampled Data (Mean for Numerical, Sum for Binary):")
    print(df_daily.head())

    # Check the shape to see the number of daily entries
    print(f"\nShape of the daily resampled DataFrame: {df_daily.shape}")

    # You might also want to see the last few rows to check the date range
    print("\nLast few rows of the daily resampled data:")
    print(df_daily.tail())

except Exception as e:
    print(f"\nAn error occurred during daily resampling: {e}")
    print("Please ensure your df_cleaned DataFrame has a DatetimeIndex.")


# Define the numerical columns for which we want the average
numerical_cols_for_average = [
    'Temperature_C',
    'Humidity_Percent',
    'Light_Lux',
    'Power_Consumption_Watts'
]

# Extract the hour from the Timestamp index
# .index.hour gives you an integer representing the hour (0-23)
df_cleaned['hour_of_day'] = df_cleaned.index.hour

# Group by 'hour_of_day' and calculate the mean for the specified columns
hourly_average_profile = df_cleaned.groupby('hour_of_day')[numerical_cols_for_average].mean()

# Display the results
print("Average values for each hour of the day across the entire dataset:")
print(hourly_average_profile)

# Calculate Q1, Q3, and IQR for Power_Consumption_Watts
Q1 = df_cleaned['Power_Consumption_Watts'].quantile(0.25)
Q3 = df_cleaned['Power_Consumption_Watts'].quantile(0.75)
IQR = Q3 - Q1

# Define anomaly bounds
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR

# Identify anomalies
anomalies = df_cleaned[(df_cleaned['Power_Consumption_Watts'] < lower_bound) | (df_cleaned['Power_Consumption_Watts'] > upper_bound)]

print(f"--- Anomaly Detection for Power_Consumption_Watts ---")
print(f"Q1 (25th percentile): {Q1:.2f} Watts")
print(f"Q3 (75th percentile): {Q3:.2f} Watts")
print(f"IQR: {IQR:.2f} Watts")
print(f"Lower Bound (Q1 - 1.5*IQR): {lower_bound:.2f} Watts")
print(f"Upper Bound (Q3 + 1.5*IQR): {upper_bound:.2f} Watts")

print(f"\nTotal Anomalies Detected: {len(anomalies)}")

if not anomalies.empty:
    print("\nDetected Anomalies (Timestamp and Power_Consumption_Watts):")
    print(anomalies[['Power_Consumption_Watts']])

else:
    print("\nNo anomalies detected based on the IQR method.")


# Define the threshold for low light conditions
low_light_threshold = 10 # Lux

# Filter the DataFrame for low light conditions
low_light_df = df_cleaned[df_cleaned['Light_Lux'] < low_light_threshold].copy()

# Ensure Motion_Detected is treated as a category or integer for grouping
low_light_df['Motion_Detected'] = low_light_df['Motion_Detected'].astype(int)

# Calculate the average Power_Consumption_Watts for each Motion_Detected state
# during low light conditions
power_consumption_by_motion = low_light_df.groupby('Motion_Detected')['Power_Consumption_Watts'].mean()

print(f"--- Power Consumption Analysis during Low Light (Light_Lux < {low_light_threshold}) ---")

if not low_light_df.empty:
    print("\nAverage Power_Consumption_Watts when Motion_Detected is:")
    if 0 in power_consumption_by_motion.index:
        print(f"  0 (No Motion): {power_consumption_by_motion[0]:.2f} Watts")
    else:
        print("  0 (No Motion): No data for this condition.")

    if 1 in power_consumption_by_motion.index:
        print(f"  1 (Motion Detected): {power_consumption_by_motion[1]:.2f} Watts")
    else:
        print("  1 (Motion Detected): No data for this condition.")

    # Compare the averages
    if 0 in power_consumption_by_motion.index and 1 in power_consumption_by_motion.index:
        diff = power_consumption_by_motion[1] - power_consumption_by_motion[0]
        print(f"\nDifference (Motion - No Motion): {diff:.2f} Watts")
        if diff > 0:
            print("Insight: During low light, power consumption is higher when motion is detected, suggesting lights or other devices are activated by motion.")
        elif diff < 0:
            print("Insight: During low light, power consumption is lower when motion is detected (unusual, check data/setup).")
        else:
            print("Insight: During low light, power consumption is roughly the same whether motion is detected or not.")
    else:
        print("\nInsufficient data to compare both motion states during low light.")


else:
    print(f"\nNo data found under low light conditions (Light_Lux < {low_light_threshold}). Please check your Light_Lux values or adjust the threshold.")

