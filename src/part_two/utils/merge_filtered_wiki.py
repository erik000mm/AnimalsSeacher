# src/part_two/utils/merge_filtered_wiki.py
# Script to merge filtered Wikipedia XML dump chunks into a single XML file

import os
import re
import sys
import xml.etree.ElementTree as ET

PATTERN = re.compile(r"chunk_(\d+)_filtered\.xml$")

INPUT_DIR = "../../../filtered_wiki/"
OUTPUT_FILE = "../../../filtered_wiki/merged_filtered_wiki.xml"

def is_page(tag):
    return tag.endswith("page")

def is_siteinfo(tag):
    return tag.endswith("siteinfo")

def sort_key(name):
    m = PATTERN.search(name)
    return int(m.group(1)) if m else -1

def find_namespace(root):
    if root.tag.startswith("{"):
        return root.tag.split("}")[0].strip("{")
    return ""

def merge_chunks(src_dir, out_path):
    if not os.path.isdir(src_dir):
        print(f"Input directory not found: {src_dir}")
        sys.exit(1)

    files = [f for f in os.listdir(src_dir) if PATTERN.match(f)]
    if not files:
        print(f"No chunk files matching pattern in {src_dir}")
        sys.exit(1)

    files.sort(key=sort_key)

    base_tree = None
    base_root = None
    total_pages = 0
    files_merged = 0

    for idx, fname in enumerate(files):
        fpath = os.path.join(src_dir, fname)
        try:
            tree = ET.parse(fpath)
        except Exception as e:
            print(f"Error parsing {fpath}: {e}", file=sys.stderr)
            continue

        root = tree.getroot()

        if idx == 0:
            base_tree = tree
            base_root = root
            for child in list(base_root):
                if is_page(child.tag):
                    total_pages += 1
            files_merged += 1
            continue

        # Append <page> elements from this tree to the base tree
        added_pages = 0
        for child in list(root):
            if is_page(child.tag):
                base_root.append(child)
                added_pages += 1
        total_pages += added_pages
        files_merged += 1

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    base_tree.write(out_path, encoding="utf-8", xml_declaration=True)

    print(f"Merged {files_merged} files -> {out_path}")
    print(f"Total <page> elements: {total_pages}")


if __name__ == "__main__":
    merge_chunks(INPUT_DIR, OUTPUT_FILE)
