import datetime
import io
import zipfile
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, desc, dense_rank
from pyspark.sql.window import Window

def report_avg_trip_duration_per_day(df):
    processed_df = (
        df.groupBy(date_format("start_time", "yyyy-MM-dd").alias("day"))
        .agg({"tripduration": "avg"})
        .orderBy("day"))
    report_data(processed_df, "1_avg_trip_duration_per_day")

def report_trip_count_per_day(df):
    processed_df = (
        df.groupBy(date_format("start_time", "yyyy-MM-dd").alias("day"))
        .count()
        .orderBy("day"))
    report_data(processed_df, "2_trip_count_per_day")

def report_most_popular_station_per_month(df):
    processed_df = (
        df.withColumn("month", date_format("start_time", "yyyy-MM"))
        .groupBy("month", "from_station_name")
        .count()
        .orderBy("month", col("count").desc())
        .groupBy("month")
        .agg({"from_station_name": "first"})
        .orderBy("month")
        .withColumnRenamed("first(from_station_name)", "most_popular_station"))
    report_data(processed_df, "3_most_popular_station_per_month")

def report_top3_stations_last_two_weeks(df):
    df_selection = df.withColumn("start_time", col("start_time").cast("timestamp"))
    end_date = df_selection.agg({"start_time": "max"}).collect()[0][0]
    start_date = end_date - datetime.timedelta(days=14)
    two_weeks_df = df_selection.filter((col("start_time") >= start_date) & (col("start_time") <= end_date))
    station_counts = two_weeks_df.groupBy("from_station_id", "from_station_name").count()
    window_spec = Window.orderBy(desc("count"))
    ranked_stations = station_counts.withColumn("rank", dense_rank().over(window_spec))
    top_stations = ranked_stations.filter(col("rank") <= 3)
    report_data(top_stations, "4_top3_stations_last_two_weeks")

def report_avg_trip_duration_by_gender(df):
    processed_df = (
        df.groupBy("gender")
        .agg({"tripduration": "avg"})
        .orderBy("gender"))
    report_data(processed_df, "5_avg_trip_duration_by_gender")

def report_avg_trip_duration_by_age(df):
    processed_df = (
        df.groupBy("birthyear")
        .agg({"tripduration": "avg"})
        .orderBy(col("avg(tripduration)").desc())
        .limit(10))
    report_data(processed_df, "6_avg_trip_duration_by_age")

def report_data(df, name):
    df.write.csv(f"reports/{name}", header=True, mode="overwrite")

def collect_data_from_zipped_csv(binary_files):
    zip_file = zipfile.ZipFile(io.BytesIO(binary_files[1]), "r")
    csv_data = {}
    for file in zip_file.namelist():
        if file.lower().endswith('.csv'):
            csv_content = zip_file.read(file).decode("ISO-8859-1")
            csv_data[file] = pd.read_csv(io.StringIO(csv_content))
    return csv_data

def main():
    spark = SparkSession.builder.appName("Lab5").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    archives = sc.binaryFiles("data/*.zip")
    archives_data = archives.map(collect_data_from_zipped_csv).collect()

    for data in archives_data:
        for _, content in data.items():
            if "merged_df" not in locals():
                merged_df = spark.createDataFrame(content)

    merged_df.show()
    report_avg_trip_duration_per_day(merged_df)
    report_trip_count_per_day(merged_df)
    report_most_popular_station_per_month(merged_df)
    report_top3_stations_last_two_weeks(merged_df)
    report_avg_trip_duration_by_gender(merged_df)
    report_avg_trip_duration_by_age(merged_df)
    spark.stop()

if __name__ == "__main__":
    main()