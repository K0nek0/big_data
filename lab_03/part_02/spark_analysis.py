from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from time import time


def spark_analysis():
    spark = SparkSession.builder \
        .appName("ChessAnalysis") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    # Схема данных
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
    
    # Загрузка данных
    df = spark.read.csv("C:/Users/silen/.cache/kagglehub/datasets/arevel/chess-games/versions/1/chess_games.csv", schema=schema, header=True)
    
    # Анализ
    result = df.agg(
        count("*").alias("total_games"),

        spark_sum(when(col("termination").contains("Time forfeit"), 1).otherwise(0)).alias("total_time_forfeit"),

        spark_sum(when(
            ((col("result") == "1-0") & (col("white_elo") - col("black_elo") < -300)) |
            ((col("result") == "0-1") & (col("black_elo") - col("white_elo") < -300)), 1
        ).otherwise(0)).alias("upset_total"),

        spark_sum(when(
            (col("termination").contains("Time")) &
            (((col("result") == "1-0") & (col("white_elo") - col("black_elo") < -300)) |
             ((col("result") == "0-1") & (col("black_elo") - col("white_elo") < -300))), 1
        ).otherwise(0)).alias("upset_time_forfeit")
    )

    # Вывод результатов
    stats = result.collect()[0]

    upset_percent = (stats['upset_time_forfeit'] / stats['upset_total'] * 100) if stats['upset_total'] else 0
    total_percent = (stats['total_time_forfeit'] / stats['total_games'] * 100) if stats['total_games'] else 0

    print(f"Всего игр: {stats['total_games']:,}")
    print(f"Апсет-побед: {stats['upset_total']:,}")
    print(f"Апсеты с Time forfeit: {stats['upset_time_forfeit']:,} ({upset_percent:.1f}%)")
    print(f"Всего Time forfeit: {stats['total_time_forfeit']:,} ({total_percent:.1f}%)")
    
    if total_percent > 0:
        ratio = upset_percent / total_percent
        print(f"Отношение: {ratio:.2f}x")
        print("Гипотеза подтверждается" if ratio > 1 else "Гипотеза отвергается")
    
    spark.stop()

if __name__ == "__main__":
    start_time = time()

    spark_analysis()

    print(f"Время обработки: {time() - start_time:.2f}с")
