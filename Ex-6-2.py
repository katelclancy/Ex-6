# -*- coding: utf-8 -*-
"""
Created on Thu Apr 10 00:11:31 2025

@author: KateClancy
"""

# bloom_filter_spark.py

import requests

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

# --- Step 1: Get bad words from AFINN ---
def get_badwords():
    bad_words = []
    afinn = requests.get('https://raw.githubusercontent.com/fnielsen/afinn/master/afinn/data/AFINN-en-165.txt')
    for line in afinn.text.splitlines():
        word, score = line.split('\t')
        if int(score) in [-4, -5]:
            bad_words.append(word)
    return bad_words

# --- Step 2: Build Bloom Filter ---
def build_bloom_filter(bad_words, size=2000, num_hashes=3):
    bit_array = [0] * size
    for word in bad_words:
        for seed in range(num_hashes):
            # Simulate multiple hash functions by combining word with seed
            combined = f"{word.lower()}_{seed}"
            index = abs(hash(combined)) % size
            bit_array[index] = 1
    return bit_array

# --- Step 3: Create Bloom Filter and Spark Session ---
bad_words = get_badwords()
bit_array = build_bloom_filter(bad_words)

spark = SparkSession.builder \
    .appName("BloomFilterStream") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# --- Step 4: Register Bloom Filter UDF ---

def is_clean(line):
    words = line.split()
    for word in words:
        for seed in range(3):  # Simulate 3 hash functions
            index = abs(hash(f"{word.lower()}_{seed}")) % 2000
            if bit_array[index] == 0:
                return True  # Not a bad word → allow the line
    return False  # Possibly a bad word → block the line

is_clean_udf = udf(is_clean, BooleanType())

# --- Step 5: Read from socket stream ---
df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# --- Step 6: Filter stream with Bloom Filter ---
clean_df = df.filter(is_clean_udf(df["value"]))

# --- Step 7: Write to console ---
query = clean_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()