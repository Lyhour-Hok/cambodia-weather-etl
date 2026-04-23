import json
import glob
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, lower, trim, when

def main():
    spark = SparkSession.builder \
        .appName("CambodiaWeatherTransform") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Read JSON Array
    with open("/tmp/weather_raw.json", "r") as f:
        data = json.load(f)

    print(f"📥 Input rows: {len(data)}")

    # Convert to DataFrame
    rdd = spark.sparkContext.parallelize(data)
    df = spark.read.json(rdd)

    # Transform
    df_clean = df.select(
        trim(col("city")).alias("city"),
        trim(col("province")).alias("province"),
        col("country"),
        col("latitude"),
        col("longitude"),
        spark_round(col("temperature"), 1).alias("temperature"),
        spark_round(col("feels_like"), 1).alias("feels_like"),
        spark_round(col("temp_min"), 1).alias("temp_min"),
        spark_round(col("temp_max"), 1).alias("temp_max"),
        col("humidity").cast("int"),
        col("pressure").cast("int"),
        lower(trim(col("weather"))).alias("weather"),
        col("weather_main"),
        spark_round(col("wind_speed"), 1).alias("wind_speed"),
        col("wind_deg").cast("int"),
        col("cloudiness").cast("int"),
        col("visibility").cast("int"),
        col("timestamp")
    )

    # Add heat_level
    df_clean = df_clean.withColumn(
        "heat_level",
        when(col("temperature") >= 38, "Extreme Heat")
        .when(col("temperature") >= 35, "Very Hot")
        .when(col("temperature") >= 30, "Hot")
        .when(col("temperature") >= 25, "Warm")
        .otherwise("Cool")
    )

    # Sort
    df_clean = df_clean.orderBy(col("temperature").desc())

    # Save to temp folder
    output_path = "/tmp/weather_clean_spark"
    df_clean.coalesce(1).write.mode("overwrite").json(output_path)

    # Find output file
    all_files = os.listdir(output_path)
    print(f"Files in output: {all_files}")

    part_files = [f for f in all_files if f.startswith("part-")]
    print(f"Part files: {part_files}")

    # Read and save as JSON array
    all_rows = []
    for pf in part_files:
        full_path = os.path.join(output_path, pf)
        with open(full_path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    all_rows.append(json.loads(line))

    print(f"📤 Output rows: {len(all_rows)}")

    with open("/tmp/weather_clean.json", "w") as f:
        json.dump(all_rows, f, indent=2, ensure_ascii=False)

    count = df_clean.count()
    print(f"✅ Spark Transform done! {count} provinces.")
    spark.stop()

if __name__ == "__main__":
    main()