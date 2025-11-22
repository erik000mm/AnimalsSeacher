# src/part_one/indexer.py
# Script to create an inverted index from JSON data

import json
import re
import math

from collections import defaultdict, Counter

DATA_PATH = "data/az_animals_dataset.json"
OUTPUT_PATH = "src/part_one/index.json"

def clean_text(text):
    """Clean text by converting to lowercase and removing non-alphabetic characters."""
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def tokenize(text):
    """Tokenize text into words."""
    tokens = text.split()
    tokens = [t for t in tokens if len(t) > 1 or t.isdigit()]
    return tokens


def format_key(key):
    """Formats a JSON key into a clean token prefix."""
    key = key.lower()
    key = re.sub(r'[^a-z0-9_]', '_', key)
    key = re.sub(r'_+', '_', key).strip('_')
    return key


def fuse_terms(doc_id, node, inverted_index, current_key = ""):
    """
    Fuses the current key with the terms in the node and updates the inverted index.
    """
    if isinstance(node, dict):
        for key, value in node.items():
            fuse_terms(doc_id, value, inverted_index, key)

    elif isinstance(node, list):
        for item in node:
            fuse_terms(doc_id, item, inverted_index, current_key)

    elif isinstance(node, (str, int, float)):
        text = str(node)
        cleaned_text = clean_text(text)
        tokens = tokenize(cleaned_text)
        
        if not tokens:
            return

        term_frequencies = Counter(tokens)
        formatted_key = format_key(current_key) if current_key else ""
        
        for term, tf in term_frequencies.items():
            if doc_id not in inverted_index[term]["docID/freq"]:
                inverted_index[term]["freq"] += 1
            inverted_index[term]["docID/freq"][doc_id] = inverted_index[term]["docID/freq"].get(doc_id, 0) + tf

        IS_LONG_TEXT = len(text) > 100
        if formatted_key and not IS_LONG_TEXT:
            for term, tf in term_frequencies.items():
                fused_term = f"{formatted_key}_{term}"
                if doc_id not in inverted_index[fused_term]["docID/freq"]:
                    inverted_index[fused_term]["freq"] += 1
                inverted_index[fused_term]["docID/freq"][doc_id] = (inverted_index[fused_term]["docID/freq"].get(doc_id, 0) + tf)


def idf_standard(df, N):
    """IDF = log10(N / df)"""
    return math.log10(N / df) if df else 0.0


def idf_smooth(df, N):
    """Smoothed IDF = log10(N / (1 + df)) + 1"""
    return (math.log10(N / (1 + df)) + 1) if df else 0.0


def compute_idf(index, N):
    """Adds IDF values to the index."""
    for term, data in index.items():
        df = data["freq"]
        index[term]["idf_standard"] = idf_standard(df, N)
        index[term]["idf_smooth"] = idf_smooth(df, N)
    return index


def create_index(data):
    """Creates a set of inverted indexes for different keys."""
    inverted_index = defaultdict(lambda: {"freq": 0, "docID/freq": {}})
    
    for doc_id, attributes in data.items():
        fuse_terms(doc_id, attributes, inverted_index)
    
    index = {}
    for term in sorted(inverted_index.keys()):
        index[term] = {
            "freq": inverted_index[term]["freq"],
            "docID/freq": dict(sorted(inverted_index[term]["docID/freq"].items()))
        }
    
    return index


if __name__ == "__main__":
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    index = create_index(data)
    N = len(data)
    index = compute_idf(index, N)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(index, f, indent=2, ensure_ascii=False)

    print(f"Terms indexed: {len(index)}")