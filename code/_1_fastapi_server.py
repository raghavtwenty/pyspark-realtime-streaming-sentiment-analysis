"""
Project Name: Sentiment Analysis in PySpark Realtime Streaming
Filename: _1_fastapi_server.py
Title: Fast API server for realtime streaming simulation
Author: Raghava | GitHub: @raghavtwenty
Date Created: April 28, 2024 | Last Updated: June 13, 2024
Language: Python | Version: 3.12.3, 64-bit
"""

# Importing required libraries
from fastapi import FastAPI
import random

# Initialize fast api
app = FastAPI()


# Home route for sending data
@app.get("/")
def root():

    # Sample data
    options = [
        "Raghava liked your picture",
        "Dhoni liked your picture",
        "Steve Jobs liked your picture",
        "You failed the test",
        "Great job on your presentation",
        "I'm so disappointed in you",
        "Congratulations on your promotion",
        "This is the worst service ever",
    ]

    # Randomize the values
    random_name = random.choice(options)
    return {"message": random_name}
