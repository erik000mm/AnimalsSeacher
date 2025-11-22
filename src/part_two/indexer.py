# src/part_two/indexer.py
# Script to create a Lucene index from a JSON dataset of animals

import json
import os
import re
import sys
import lucene
from typing import List

from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, StringField, TextField, StoredField
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.store import FSDirectory

DATA_PATH = "../../data/az_animals_dataset.json"
INDEX_DIR = "index"
CLEAN = True


def remove_json_comments(text):
    # Remove // and /* */
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    text = re.sub(r"(^|\s)//.*$", "", text, flags=re.M)
    return text

def load_animals_json(path):
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        cleaned = remove_json_comments(raw)
        return json.loads(cleaned)

def flatten_content(value):
    if value is None:
        return []
    if isinstance(value, (str, bytes)):
        try:
            s = value.decode("utf-8") if isinstance(value, bytes) else value
        except Exception:
            s = str(value)
        if s:
            return [s]
        return []
    if isinstance(value, (int, float, bool)):
        return [str(value)]
    if isinstance(value, dict):
        out: List[str] = []
        for k, v in value.items():
            if str(k).lower() in {"match_type", "id", "uuid"}:
                continue
            out.extend(flatten_content(v))
        return out
    if isinstance(value, (list, tuple, set)):
        out: List[str] = []
        for v in value:
            out.extend(flatten_content(v))
        return out
    return [str(value)]

def create_document(animal_name, details_obj, source_path):
    doc = Document()
    doc.add(StringField("exact_name", animal_name, Field.Store.YES))
    doc.add(TextField("name", animal_name, Field.Store.YES))

    parts: List[str] = []
    parts.extend(flatten_content(details_obj))
    content = "\n".join(p for p in parts if p).strip()
    if content:
        doc.add(TextField("content", content, Field.Store.YES))
    else:
        doc.add(TextField("content", animal_name, Field.Store.YES))

    doc.add(StoredField("source", source_path))

    return doc


def create_index(index_dir, data_path):
    if not lucene.getVMEnv() or not lucene.getVMEnv().isCurrentThreadAttached():
        lucene.initVM(vmargs=["-Djava.awt.headless=true"])
    os.makedirs(index_dir, exist_ok=True)

    directory = FSDirectory.open(Paths.get(index_dir))
    analyzer = StandardAnalyzer()
    config = IndexWriterConfig(analyzer)
    if CLEAN:
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    else:
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)

    animals = load_animals_json(data_path)
    if not isinstance(animals, dict):
        raise ValueError("animals_dataset.json must be a JSON object")

    count = 0
    writer = IndexWriter(directory, config)
    try:
        for name, details in animals.items():
            try:
                doc = create_document(str(name), details, os.path.abspath(data_path))
                writer.addDocument(doc)
                count += 1
            except Exception as e:
                print(f"Skipping '{name}': {e}", file=sys.stderr)
        writer.commit()
    finally:
        writer.close()
    return count, index_dir


if __name__ == "__main__":
    total, idx_dir = create_index(INDEX_DIR, DATA_PATH)
    print(f"Indexed {total} animals into '{idx_dir}'")