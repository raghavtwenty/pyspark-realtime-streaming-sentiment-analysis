"""
Project Name: Sentiment Analysis in PySpark Realtime Streaming
Filename: _3_sentiment_analysis_pyspark.py
Title: PySpark realtime stream analytics from FastAPI
Author: Raghava | GitHub: @raghavtwenty
Date Created: April 28, 2024 | Last Updated: June 13, 2024
Language: Python | Version: 3.12.3, 64-bit
"""

# Importing required libraries
import requests
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, FloatType, StructType, StructField
from textblob import TextBlob


# Initialize SparkSession
spark = SparkSession.builder.appName("realtime_sentiment_analysis").getOrCreate()

# Schema for the incoming data
schema = StructType(
    [
        StructField(
            "message",
            StringType(),
            nullable=True,
        ),
    ]
)


# Sentiment analysis scoring
def get_sentiment_score(message):
    if message:
        # Finding score
        score = TextBlob(message).sentiment.polarity
        return score
    else:
        return 0


# Sentiment analysis labeling
def get_sentiment_label(score):
    if score >= 0.5:
        return "Positive"
    elif score > -0.5 and score < 0.5:
        return "Neutral"
    elif score <= -0.5:
        return "Negative"
    else:
        return "N/A"


# User defined functions with Spark
sentiment_udf_score = udf(get_sentiment_score, FloatType())
sentiment_udf_label = udf(get_sentiment_label, StringType())


# Receive data from FastAPI endpoint every second
def receive_data():
    response = requests.get("http://localhost:8000/")
    data = response.json()
    return data


# Main
if __name__ == "__main__":
    while True:
        # New stream
        data = receive_data()
        # Data frame for new stream
        df = spark.createDataFrame([data], schema)

        # Sentiment analysis dataframe
        analysis_df = (
            df.withColumn("Sentiment Score", sentiment_udf_score(col("message")))
            .withColumn("Sentiment Label", sentiment_udf_label(col("Sentiment Score")))
            .select("message", "Sentiment Score", "Sentiment Label")
        )

        # Final show
        analysis_df.show(truncate=False)

        # Every second find sentiment for new stream
        time.sleep(1)
