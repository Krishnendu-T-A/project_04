
# # ===============================================================================
#                              GoFreeLab Proprietary
# -------------------------------------------------------------------------------

# Project Name    : <Project Name>
# File Name       : <File Name>
# Author          : <Author Name>
# Created Date    : <Date>
# Version         : <Version>
# Description     : <Short description of what this file does>

# -------------------------------------------------------------------------------
# Copyright (c) <Year> GoFreeLab. All rights reserved.

# This source code and all its contents are the proprietary property of GoFreeLab.
# Unauthorized copying, sharing, or distribution of this code, in whole or in 
# part, via any medium is strictly prohibited without prior written permission 
# from GoFreeLab.

# This software is for use only by employees, contractors, or partners of 
# GoFreeLab with explicit authorization.

# For questions or permissions, please contact: info@gofreelab.com
# ===============================================================================
# """
from pymongo import MongoClient
import pandas as pd

# MongoDB Connection
client = MongoClient("mongodb+srv://freego:freego%2322Monkey@freego-c0.y47x2e3.mongodb.net")
db = client["freego"]  # Database name
source_collection = db["user_match_out"]  # Source collection
filtered_collection = db["filtered_data"]  # New collection for filtered/sorted data

# Updated Similarity Score Threshold
SIMILARITY_THRESHOLD = 0.8  

def filter_and_sort_data():
    """Fetches, filters, sorts, and stores data in MongoDB."""
    # Fetch data from the source collection
    data = list(source_collection.find())

    if not data:
        print("No data found in source collection.")
        return

    # Convert to Pandas DataFrame
    df = pd.DataFrame(data)

    # Ensure similarity_score exists
    if "similarity_score" not in df.columns:
        print("similarity_score column missing in data.")
        return

    # Filter: Keep only records with similarity_score >= 0.9
    df_filtered = df[df["similarity_score"] >= SIMILARITY_THRESHOLD]

    # Sort: Descending order of similarity_score
    df_sorted = df_filtered.sort_values(by="similarity_score", ascending=False)

    # Convert to list of JSON
    sorted_data_json = df_sorted.to_dict(orient="records")

    # Clear previous data in the target collection
    filtered_collection.delete_many({})  

    # Insert new filtered & sorted data
    if sorted_data_json:
        filtered_collection.insert_many(sorted_data_json)
        print("Filtered and sorted data updated in MongoDB.")
    else:
        print("No records met the threshold (0.9).")

# Call function to initially process the data
filter_and_sort_data()

# MongoDB Change Stream to Watch for New Inserts
def watch_for_changes():
    """Monitors the source collection for new data and updates the filtered collection."""
    print("Watching for changes in the source collection...")
    with source_collection.watch() as stream:
        for change in stream:
            print("Change detected! Reprocessing data...")
            filter_and_sort_data()  # Re-run filter and sorting process

# Start watching for real-time updates
watch_for_changes()
