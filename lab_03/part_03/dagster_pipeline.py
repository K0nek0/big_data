from dagster import job, op
import kagglehub
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import json
from datetime import datetime
import os


@op
def download_data():
    print("Скачивание данных...")
    path = kagglehub.dataset_download("arevel/chess-games")
    return path

@op
def analyze_data(data_path):
    print("Запуск Spark анализа...")
    spark = SparkSession.builder.appName("ChessAnalysis").getOrCreate()
    
    schema = StructType([
        StructField("event", StringType(), True),
        StructField("white", StringType(), True),
        StructField("black", StringType(), True),
        StructField("result", StringType(), True),
        StructField("utcdate", StringType(), True),
        StructField("utctime", StringType(), True),
        StructField("white_elo", IntegerType(), True),
        StructField("black_elo", IntegerType(), True),
        StructField("white_rating_diff", IntegerType(), True),
        StructField("black_rating_diff", IntegerType(), True),
        StructField("eco", StringType(), True),
        StructField("opening", StringType(), True),
        StructField("time_control", StringType(), True),
        StructField("termination", StringType(), True),
        StructField("an", StringType(), True)
    ])
    
    csv_file = None
    for root, _, files in os.walk(data_path):
        for file in files:
            if file.endswith('.csv'):
                csv_file = os.path.join(root, file)
                break
        if csv_file:
            break
    
    df = spark.read.csv(csv_file, schema=schema, header=True)
    
    result = df.agg(
        count("*").alias("total_games"),
        sum(when(col("termination").contains("Time forfeit"), 1).otherwise(0)).alias("total_time_forfeit"),
        sum(when(
            ((col("result") == "1-0") & (col("white_elo") - col("black_elo") < -300)) |
            ((col("result") == "0-1") & (col("black_elo") - col("white_elo") < -300)), 1
        ).otherwise(0)).alias("upset_total"),
        sum(when(
            (col("termination").contains("Time")) &
            (((col("result") == "1-0") & (col("white_elo") - col("black_elo") < -300)) |
             ((col("result") == "0-1") & (col("black_elo") - col("white_elo") < -300))), 1
        ).otherwise(0)).alias("upset_time_forfeit")
    )
    
    stats = result.collect()[0]
    spark.stop()
    
    upset_percent = (stats['upset_time_forfeit'] / stats['upset_total'] * 100) if stats['upset_total'] else 0
    total_percent = (stats['total_time_forfeit'] / stats['total_games'] * 100) if stats['total_games'] else 0
    ratio = upset_percent / total_percent if total_percent else 0
    
    results = {
        "total_games": int(stats['total_games']),
        "total_time_forfeit": int(stats['total_time_forfeit']),
        "upset_total": int(stats['upset_total']),
        "upset_time_forfeit": int(stats['upset_time_forfeit']),
        "upset_percentage": round(float(upset_percent), 2),
        "total_percentage": round(float(total_percent), 2),
        "ratio": round(float(ratio), 2),   
        "hypothesis_confirmed": ratio > 1
    }
    
    return results

@op
def save_report(results):
    print("Сохранение отчета...")
    report = {
        "timestamp": datetime.now().isoformat(),
        "results": results
    }
    
    filename = f"chess_report_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
    with open(filename, 'w') as f:
        json.dump(report, f, indent=2)

    print(f"Отчет сохранен: {filename}")
    return filename

@job
def chess_analysis():
    data = download_data()
    results = analyze_data(data)
    save_report(results)

if __name__ == "__main__":
    print("Запуск анализа...")
    chess_analysis.execute_in_process()
