# src/part_two/wiki_dump_filter.py
# Script to filter Wikipedia XML dumps for pages containing Speciesbox templates

import os
import sys
import xml.etree.ElementTree as ET
from typing import List

INPUT_PATH = "../../../wiki_chunks/"
OUTPUT_PATH = "../../../filtered_wiki/"


def find_first_by_localname(elem: ET.Element, local: str):
    """Find first descendant whose tag ends with the given local name."""
    for e in elem.iter():
        if isinstance(e.tag, str) and e.tag.endswith(local):
            return e
    return None


def itererate_xml_pages(xml_path: str):
    """(page_element, text_content) for each <page> in the XML file."""
    try:
        context = ET.iterparse(xml_path, events=("end",))
        for _, elem in context:
            if isinstance(elem.tag, str) and elem.tag.endswith("page"):
                text_elem = find_first_by_localname(elem, "text")
                text_content = text_elem.text if text_elem is not None and text_elem.text else ""
                yield elem, text_content
                elem.clear()
    except Exception as e:
        print(f"Error processing {xml_path}: {e}", file=sys.stderr)


def page_has_speciesbox(text: str):
    """Return True if the page text contains a Speciesbox template."""
    return "{{speciesbox" in text.lower()


def copy_element(elem: ET.Element):
    """Copy an Element."""
    data = ET.tostring(elem, encoding="utf-8")
    return ET.fromstring(data)


def filter_file(xml_path: str, output_path: str):
    """Write a new XML containing only pages that include {{Speciesbox ...}}."""
    retained: List[ET.Element] = []
    total_pages = 0
    matched_pages = 0

    for page_elem, text in itererate_xml_pages(xml_path):
        total_pages += 1
        if page_has_speciesbox(text):
            title_el = page_elem.find("title")
            title = title_el.text if title_el is not None else "(no title)"
            print(f"[MATCH] {title}")
            retained.append(copy_element(page_elem))
            matched_pages += 1

    if matched_pages == 0:
        return {"file": xml_path, "total_pages": total_pages, "matched_pages": 0, "output": None}

    root = ET.Element("mediawiki")
    for p in retained:
        root.append(p)
    tree = ET.ElementTree(root)
    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    tree.write(output_path, encoding="utf-8", xml_declaration=True)
    return {"file": xml_path, "total_pages": total_pages, "matched_pages": matched_pages, "output": output_path}


def find_xml_files(path: str):
    """Return a list of XML files from the given path."""
    if os.path.isdir(path):
        return [os.path.join(path, f) for f in os.listdir(path) if f.lower().endswith(".xml")]
    return [path]


if __name__ == "__main__":
    xml_files = find_xml_files(INPUT_PATH)
    os.makedirs(OUTPUT_PATH, exist_ok=True)

    all_stats: List[dict] = []
    for xml_file in xml_files:
        base = os.path.basename(xml_file)
        output_file = os.path.join(OUTPUT_PATH, base.replace(".xml", "_filtered.xml"))
        stats = filter_file(xml_file, output_file)
        all_stats.append(stats)
        base_name = os.path.basename(stats.get("file", "unknown"))
        if stats.get("output"):
            print(f"Processed {base_name}: {stats['matched_pages']}/{stats['total_pages']} pages retained -> {stats['output']}")
        else:
            print(f"No pages matched in {base_name}, no output file created.")

    total_pages = sum(s.get("total_pages", 0) for s in all_stats)
    matched_pages = sum(s.get("matched_pages", 0) for s in all_stats)
    output_files = sum(1 for s in all_stats if s.get("output"))
    print(f"Summary: Retained {matched_pages} of {total_pages} pages across {output_files} output files (skipped empty results).")