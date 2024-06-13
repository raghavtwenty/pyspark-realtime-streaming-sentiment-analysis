"""
Project Name: Sentiment Analysis in PySpark Realtime Streaming
Filename: _2_receive_data.py
Title: Call the FastAPI endpoint (Testing purpose)
Author: Raghava | GitHub: @raghavtwenty
Date Created: April 28, 2024 | Last Updated: June 13, 2024
Language: Python | Version: 3.12.3, 64-bit
"""

# Importing required libraries
import requests
import time


# Receive the data from the fast api endpoint
def receive_data():
    while True:

        # Access the api endpoint
        response = requests.get("http://localhost:8000/")

        if response.status_code == 200:
            temp_stream = response.json()
            print("Stream data:", temp_stream)
        else:
            print("The server has been stopped.")

        # Time delay
        time.sleep(1)


# Main
if __name__ == "__main__":
    receive_data()
