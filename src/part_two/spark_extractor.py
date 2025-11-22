# src/part_two/spark_extractor.py
# Spark-based MediaWiki XML extractor for animal species data

import os
import json
import html
import re
from typing import Dict, List

from pyspark.sql import SparkSession

# XML chunk files
INPUT_DIR = "../../wiki_chunks/"
OUTPUT_PATH = "../../data/dataset_wiki.json"

PAGE_RE = re.compile(r"<page>(.*?)</page>", re.DOTALL | re.IGNORECASE)
TITLE_RE = re.compile(r"<title>(.*?)</title>", re.DOTALL | re.IGNORECASE)
PAGE_ID_RE = re.compile(r"<id>(\d+)</id>", re.IGNORECASE)
TEXT_RE = re.compile(r"<text[^>]*?>(.*?)</text>", re.DOTALL | re.IGNORECASE)
BINOMIAL_RE = re.compile(r"\b([A-Z][a-z]+\s+[a-z][a-z\-]+(?:\s+[a-z][a-z\-]+)?)\b")


def find_speciesbox_block(text):
    """Find the {{Speciesbox ...}} block"""
    # Locate template start
    m = re.search(r"\{\{\s*[Ss]peciesbox\b", text)
    if not m:
        return None
    start = m.start()

    # Brace scan counting '{{' and '}}'
    depth = 0
    i = start
    n = len(text)
    while i < n - 1:
        if text[i] == "{" and text[i + 1] == "{":
            depth += 1
            i += 2
            continue
        if text[i] == "}" and text[i + 1] == "}":
            depth -= 1
            i += 2
            if depth == 0:
                return start, i
            continue
        i += 1
    return None


def parse_file_to_json(input_path, output_path):
    """Parse a single XML file and write a JSON array."""
    with open(input_path, "r", encoding="utf-8") as f:
        xml = f.read()
    records = parse_mediawiki_dump(xml)
    mapping = build_animal_mapping(records)
    write_json_mapping(mapping, output_path)


def parse_file_content(pc):
    """Map (path, content)"""
    _, xml_text = pc
    return parse_mediawiki_dump(xml_text)


def parse_xml(spark, input_glob, dedupe):
    """Read XML files and parse pages"""
    sc = spark.sparkContext
    # Load pairs for all XML files
    files_rdd = sc.wholeTextFiles(input_glob)
    # Parse each file into page 
    records_rdd = files_rdd.flatMap(parse_file_content)
    if dedupe:
        records_rdd = records_rdd.map(
            lambda d: ((str(d.get("wiki_id")) if d.get("wiki_id") is not None else d.get("animal_name")), d)
        ).reduceByKey(lambda a, _b: a).values()
    return records_rdd


def write_json_array(records, output_path):
    """Write a single JSON file."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)


def remopve_null_keys(record):
    """Remove keys with None values while preserving order (wiki_id stays first if present)."""
    return {
        k: v
        for k, v in record.items()
        if v is not None and (not isinstance(v, list) or len(v) > 0)
    }


def build_animal_mapping(records):
    """Transform a list of records into a mapping keyed by animal_name with null keys removed."""
    out: Dict[str, Dict] = {}
    for rec in records:
        if not rec:
            continue
        name = rec.get("animal_name")
        if not name:
            continue
        cleaned = remopve_null_keys(rec)
        out[name] = cleaned
    return out


def write_json_mapping(mapping, output_path):
    """Write mapping {animal_name: {...}} to a JSON file."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)


def extract_field(block, name):
    """Extract a single-line field from the Speciesbox block."""
    # Match start of line or beginning after a newline
    pattern = re.compile(r"\|\s*" + re.escape(name) + r"\s*=\s*(.*)")
    for line in block.splitlines():
        m = pattern.match(line)
        if m:
            val = m.group(1).strip()
            # Trim trailing comments like <!-- ... -->
            val = re.sub(r"<!--.*?-->", "", val).strip()
            # Remove wrapping quotes and wiki italics markers
            val = val.strip("'\"")
            return val or None
    return None


def extract_scientific_names(raw_name):
    """Extract one or more scientific names (binomial) from a blob."""
    results: List[str] = []
    seen = set()
    if not raw_name:
        return results
    un = html.unescape(raw_name)

    # Prefer all title=... values
    for tm in re.finditer(r"\btitle\s*=\s*([^|{}\n\r]+)", un, flags=re.IGNORECASE):
        candidate = tm.group(1).strip().strip("'\"")
        candidate = candidate.replace("'''''", "").replace("'''", "").replace("''", "")
        for mb in BINOMIAL_RE.finditer(candidate):
            name = mb.group(1)
            if name not in seen:
                seen.add(name)
                results.append(name)

    # Italic wiki markup
    for im in re.finditer(r"''+([^']{1,200})''+", un):
        frag = im.group(1)
        for mb in BINOMIAL_RE.finditer(frag):
            name = mb.group(1)
            if name not in seen:
                seen.add(name)
                results.append(name)

    return results


def parse_synonyms_from_speciesbox(block):
    """Parse synonyms listed inside a Speciesbox."""
    synonyms: List[str] = []
    seen = set()

    # Explicit parameter
    param_val = extract_field(block, "synonyms_ref")
    if param_val:
        for n in extract_scientific_names(param_val):
            if n not in seen:
                seen.add(n)
                synonyms.append(n)

    # Title=Synonymy section
    lines = block.splitlines()
    # Find a title where value mentions Synonymy/Synonyms
    title_pat = re.compile(r"\|\s*title\s*=\s*(.*)", re.IGNORECASE)
    start_idx = None
    for idx, line in enumerate(lines):
        m = title_pat.match(line)
        if not m:
            continue
        val = html.unescape(m.group(1)).strip()
        # Check raw value and a simplified value
        val_simple = re.sub(r"\{\{[^|{}]*\|([^{}]*)\}\}", r"\1", val)
        if re.search(r"\bsynonym\w*\b", val, flags=re.IGNORECASE) or \
           re.search(r"\bsynonym\w*\b", val_simple, flags=re.IGNORECASE):
            start_idx = idx + 1
            break

    if start_idx is not None:
        for j in range(start_idx, len(lines)):
            ln = lines[j].strip()
            if ln == "" or ln.startswith("}}"):  # end of block
                break
            if not ln.startswith("|"):
                # not a parameter/entry line
                continue
            # Stop when we hit another named parameter
            if re.match(r"\|\s*\w+\s*=", ln):
                break
            # Extract names from the entry line
            for n in extract_scientific_names(ln):
                if n not in seen:
                    seen.add(n)
                    synonyms.append(n)

    return synonyms


def extract_title_alternatives(text):
    """Extract alternative titles from {{Redirect|...}} templates."""
    alts: List[str] = []
    seen_keys = set()
    # Match {{Redirect|...}} allowing whitespace/newlines inside until closing }}
    for m in re.finditer(r"\{\{\s*[Rr]edirect\s*\|([^}]+)\}\}", text, flags=re.DOTALL):
        params_raw = m.group(1)
        parts = params_raw.split('|')
        for raw in parts:
            item = html.unescape(raw).strip()
            if not item:
                continue
            # Remove wiki links [[target|text]] -> text, [[Page]] -> Page
            item = re.sub(r"\[\[[^\]|]+\|([^\]]+)\]\]", r"\\1", item)
            item = re.sub(r"\[\[([^\]]+)\]\]", r"\\1", item)
            # Remove wiki italics/bold
            item = item.replace("'''''", "").replace("'''", "").replace("''", "")
            # Filter rules
            if re.search(r"disambiguation", item, flags=re.IGNORECASE):
                continue
            low = item.lower()
            if low in {"other uses", "and"}:
                continue
            # Keep unique by case-insensitive key
            key = item.casefold()
            if key not in seen_keys:
                seen_keys.add(key)
                alts.append(item)

    return alts


def remove_file_links(string):
    """Remove embedded media/file links like [[File:...]] or [[Image:...]]."""
    return re.sub(r"\[\[(?:File|Image):[^\]]+\]\]", "", string, flags=re.IGNORECASE)


def replace_wiki_links(string):
    """Simplify wiki links to text."""
    # Piped links [[target|text]] -> text
    s = re.sub(r"\[\[[^\]|]+\|([^\]]+)\]\]", r"\1", string)
    # Simple links [[Afrikaans]] -> Afrikaans
    s = re.sub(r"\[\[([^\]]+)\]\]", lambda m: m.group(1).split('|')[-1], s)
    return s


def strip_templates(string):
    """Remove shallow {{...}} templates."""
    prev = None
    while prev != string:
        prev = string
        string = re.sub(r"\{\{[^{}]*\}\}", "", string)
    return string


def strip_refs_and_tags(string):
    """Remove reference tags and HTML tags from content."""
    # Remove <ref .../> and <ref>...</ref>
    string = re.sub(r"<ref[^>]*?/\s*>", "", string, flags=re.IGNORECASE)
    string = re.sub(r"<ref[^>]*?>.*?</ref>", "", string, flags=re.IGNORECASE | re.DOTALL)
    # Remove any other HTML tags
    string = re.sub(r"<[^>]+>", "", string)
    return string


def clean_text(string):
    """Clean wikitext to text suitable for JSONt."""
    string = html.unescape(string)
    string = re.sub(r"\[\[Category:[^\]]+\]\]", "", string, flags=re.IGNORECASE)
    string = remove_file_links(string)
    string = strip_refs_and_tags(string)
    string = strip_templates(string)
    string = replace_wiki_links(string)
    string = re.sub(r"[ \t]+", " ", string)
    string = re.sub(r"\n{3,}", "\n\n", string)

    return string.strip()


def extract_sections_after_species(text, speciesbox_end):
    """Split content after the Speciesbox into abstract and a list of H2 headings."""
    after = text[speciesbox_end:]
    sections_list: List[str] = []
    seen = set()

    # H2 sections
    h2_iter = list(re.finditer(r"^\s*==\s*[^=\n].*?\s*==\s*$", after, flags=re.MULTILINE))

    # Abstract = text until first H2
    if h2_iter:
        first_h2_start = h2_iter[0].start()
        abstract = clean_text(after[:first_h2_start])
    else:
        abstract = clean_text(after)

    if not h2_iter:
        return abstract, sections_list

    # Ignore certain headings
    ignore = {h.lower(): True for h in [
        "see also", "external links", "references", "sources", "further reading", "notes"
    ]}

    for m in h2_iter:
        line = m.group(0)
        hm = re.match(r"^\s*==\s*(.*?)\s*==\s*$", line)
        heading = hm.group(1).strip() if hm else ''
        heading = heading.strip('= ').strip()
        key = heading.lower()
        if heading and key not in ignore and key not in seen:
            seen.add(key)
            sections_list.append(heading)

    return abstract, sections_list


def extract_categories(text):
    """Extract category names"""
    cats = re.findall(r"\[\[Category:([^\]]+)\]\]", text, flags=re.IGNORECASE)
    cleaned: List[str] = []
    for c in cats:
        name = c.split('|', 1)[0].strip()
        if name:
            cleaned.append(name)
    return cleaned


def parse_page(page_xml):
    """Parse a single <page>...</page> XML"""
    title_m = TITLE_RE.search(page_xml)
    if not title_m:
        return None
    title = html.unescape(title_m.group(1)).strip()
    pid_m = PAGE_ID_RE.search(page_xml)
    page_id = int(pid_m.group(1)) if pid_m else None
    text_m = TEXT_RE.search(page_xml)
    if not text_m:
        return {"wiki_id": page_id, "animal_name": title}
    text = text_m.group(1)

    sb_bounds = find_speciesbox_block(text)
    fields = {"status": None, "genus": None, "species": None, "taxon": None}
    synonyms_list: List[str] = []
    sections: List[str] = []
    title_alternative: List[str] = extract_title_alternatives(text)

    if sb_bounds:
        sb_start, sb_end = sb_bounds
        block = text[sb_start:sb_end]
        # Extract fields
        for key in ("status", "genus", "species", "taxon"):
            val = extract_field(block, key)
            fields[key] = val

        # Gather synonyms from various sources
        # Explicit synonyms in Speciesbox
        for n in parse_synonyms_from_speciesbox(block):
            if n not in synonyms_list:
                synonyms_list.append(n)

        # From status_ref citation title/italics
        status_ref_val = extract_field(block, "status_ref")
        for n in extract_scientific_names(status_ref_val or ""):
            if n not in synonyms_list:
                synonyms_list.append(n)

        # From genus + species
        if fields.get("genus") and fields.get("species"):
            constructed = f"{fields['genus']} {fields['species']}"
            if constructed not in synonyms_list:
                synonyms_list.append(constructed)
        # Extract text with H2 headings after speciesbox
        abstract, sections = extract_sections_after_species(text, sb_end)
    else:
        abstract, sections = extract_sections_after_species(text, 0)

    categories = extract_categories(text)

    rec: Dict = {
        "wiki_id": page_id,
        "animal_name": title,
        "status": fields.get("status"),
        "genus": fields.get("genus"),
        "species": fields.get("species"),
        "taxon": fields.get("taxon"),
        "scientific_names": synonyms_list,
        "animal_name_alternative": title_alternative,
        "abstract": abstract,
        "text_sections": sections,
        "categories": categories,
    }
    return rec


def parse_mediawiki_dump(xml_text):
    """Parse an XML file content containing multiple <page> entries."""
    pages: List[Dict] = []
    for m in PAGE_RE.finditer(xml_text):
        page_xml = m.group(1)
        rec = parse_page(page_xml)
        if rec:
            pages.append(rec)
    return pages


def build_spark(app_name: str = "Spark"):
    """Local SparkSession"""
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.executor.memory", "12g")
        .config("spark.driver.memory", "12g")
        .getOrCreate()
    )


if __name__ == "__main__":
    spark = build_spark()
    try:
        pattern = os.path.join(INPUT_DIR, "*.xml") if os.path.isdir(INPUT_DIR) else INPUT_DIR
        print(f"Parsing: {pattern}")
        rdd = parse_xml(spark, pattern, dedupe=True)
        records = rdd.collect()
        print(f"Total unique pages: {len(records)}")
        mapping = build_animal_mapping(records)
        print(f"Total unique animal_name keys: {len(mapping)}")
        write_json_mapping(mapping, OUTPUT_PATH)
        print(f"Wrote: {OUTPUT_PATH}")
    finally:
        spark.stop()
