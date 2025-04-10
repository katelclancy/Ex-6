# -*- coding: utf-8 -*-
"""
Created on Thu Apr 10 00:11:31 2025

@author: KateClancy
"""
import base64
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

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

b64_string = spark.read.text("hdfs:///user/katelclancy/bloom_filter.txt").collect()[0][0]

# Decode from Base64
byte_array = base64.b64decode(b64_string)

# Convert back to bit array
bit_string = bin(int.from_bytes(byte_array, byteorder='big'))[2:]
bit_array = [int(b) for b in bit_string.zfill(2000)]  # zfill to ensure full length


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
    .option("truncate","false") \
    .start()

query.awaitTermination()
