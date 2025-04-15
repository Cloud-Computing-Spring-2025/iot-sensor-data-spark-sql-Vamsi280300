
# IoT Sensor Data Analysis using PySpark

This project performs data analysis on IoT sensor readings using Apache Spark. The dataset includes environmental data (e.g., temperature and humidity) collected from sensors across various locations and times.

## Dataset

The input dataset `sensor_data.csv` includes the following columns:

- `sensor_id`: Unique identifier for each sensor
- `timestamp`: Time when the reading was recorded
- `temperature`: Temperature recorded by the sensor
- `humidity`: Humidity recorded by the sensor
- `location`: Location of the sensor (e.g., BuildingA_Floor1)
- `sensor_type`: Type of sensor (e.g., TypeA, TypeB)

---

## Tasks

### Task 1: Load & Basic Exploration
- Load the CSV file with inferred schema.
- Create a temporary SQL view.
- Display first 5 records, count total entries, and list distinct locations.
- Save sample output to `task1_output.csv`.

### Task 2: Filtering & Simple Aggregations
- Filter readings where temperature is outside the [18, 30] °C range.
- Count in-range and out-of-range records.
- Compute average temperature and humidity per location.
- Save aggregated output to `task2_output.csv`.

### Task 3: Time-Based Analysis
- Convert the timestamp column to a proper `TimestampType`.
- Extract the hour of the day and compute the average temperature per hour.
- Identify the hour with the highest average temperature.
- Save results to `task3_output.csv`.

### Task 4: Window Function
- Calculate the average temperature for each sensor.
- Use a window function to rank sensors by average temperature in descending order.
- Display and save the top 5 sensors to `task4_output.csv`.

### Task 5: Pivot & Interpretation
- Pivot data with `location` as rows and `hour_of_day` as columns showing average temperatures.
- Identify the (location, hour) pair with the highest temperature.
- Save the pivot table to `task5_output.csv`.

---

## Output Files

Each task generates a CSV file:

| Task | Output File        | Description                             |
|------|--------------------|-----------------------------------------|
| 1    | `task1_output.csv` | Sample data preview                     |
| 2    | `task2_output.csv` | Aggregated data by location             |
| 3    | `task3_output.csv` | Average temperature per hour            |
| 4    | `task4_output.csv` | Ranked sensors by average temperature   |
| 5    | `task5_output.csv` | Pivot table of temperature by hour/location |

---

##  Requirements

- Python 3.x
- Apache Spark (PySpark)
- CSV data file named `sensor_data.csv` in the working directory

To install PySpark:
```bash
pip install pyspark
