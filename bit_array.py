# -*- coding: utf-8 -*-
"""
Created on Wed Apr  9 23:19:20 2025

@author: KateClancy
"""
import base64
import requests

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

def encode_bit_array(bit_array):
    # Convert list of bits (0 or 1) into a binary string like '010110...'
    bit_string = ''.join(str(bit) for bit in bit_array)
    
    # Convert binary string to bytes
    byte_array = int(bit_string, 2).to_bytes((len(bit_string) + 7) // 8, byteorder='big')
    
    # Base64 encode the bytes
    b64_encoded = base64.b64encode(byte_array).decode('utf-8')
    return b64_encoded

bad_words = get_badwords()
bit_array = build_bloom_filter(bad_words)
b64_string = encode_bit_array(bit_array)

with open("bloom_filter.txt", "w") as f:
    f.write(b64_string)