# src/part_one/extractor.py
# A script to extract animal names and descriptions from downloaded HTML files

import re
import os
import json

RE_PATTERNS = {
    # Extract animal name from title tag
    "page_title": re.compile(r'<title>.*?-\s*([^<]+?)\s*-\s*A-Z Animals</title>', re.IGNORECASE),

    # Common name and scientific name
    "common_name": re.compile(r'<div\s+id="title-header"[^>]*>.*?<h1[^>]*>([^<]+)</h1>', re.IGNORECASE | re.DOTALL),
    "common_name_alternative": re.compile(r'<h1[^>]*>([^<]+)</h1>', re.IGNORECASE),
    "scientific_name": re.compile(r'<div\s+id="title-header"[^>]*>.*?<p[^>]*class="[^"]*text-white[^"]*"[^>]*>\s*([A-Z][a-z]+\s+[a-z]+)\s*</p>', re.IGNORECASE | re.DOTALL),

    # Taxonomy
    "kingdom": re.compile(r'<dt[^>]*>.*?Kingdom.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "phylum": re.compile(r'<dt[^>]*>.*?Phylum.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "class": re.compile(r'<dt[^>]*>.*?Class.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "order": re.compile(r'<dt[^>]*>.*?Order.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "family": re.compile(r'<dt[^>]*>.*?Family.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "genus": re.compile(r'<dt[^>]*>.*?Genus.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),

    # Location
    "habitat": re.compile(r'<dt[^>]*>\s*<a[^>]*>Habitat</a>\s*</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE),
    "location": re.compile(r'<dt[^>]*>\s*<a[^>]*>Location</a>\s*</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE),
    "locations_list": re.compile(r'<h2[^>]*>.*?Locations</h2>.*?<ul[^>]*>(.*?)</ul>', re.IGNORECASE | re.DOTALL),

    # Characteristics
    "color": re.compile(r'<dt[^>]*>\s*<a[^>]*>Color</a>\s*</dt>\s*<dd[^>]*>(.*?)</dd>', re.IGNORECASE | re.DOTALL),
    "skin_type": re.compile(r'<dt[^>]*>.*?Skin Type.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "top_speed": re.compile(r'<dt[^>]*>.*?Top Speed.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "lifespan": re.compile(r'<dt[^>]*>.*?Lifespan.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "weight": re.compile(r'<dt[^>]*>.*?Weight.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "length": re.compile(r'<dt[^>]*>.*?Length.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "height": re.compile(r'<dt[^>]*>.*?Height.*?</dt>\\s*<dd[^>]*>(.*?)</dd>', re.IGNORECASE | re.DOTALL),
    "wingspan": re.compile(r'<dt[^>]*>.*?Wingspan.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "distinctive_features": re.compile(r'<dt[^>]*>.*?Most Distinctive Feature.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    
    # Behavior
    "venomous": re.compile(r'<dt[^>]*>.*?Venomous.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "aggression": re.compile(r'<dt[^>]*>.*?Aggression.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),

    # Facts
    "diet": re.compile(r'<dt[^>]*>.*?Diet.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "prey": re.compile(r'<dt[^>]*>.*?Prey.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "predators": re.compile(r'<dt[^>]*>.*?Predators.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "life_style": re.compile(r'<dt[^>]*>.*?Life Style.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "group_behavior": re.compile(r'<dt[^>]*>\s*<a[^>]*>Group Behavior</a>\s*</dt>\s*<dd[^>]*>(.*?)</dd>', re.IGNORECASE | re.DOTALL),
    "gestation_period": re.compile(r'<dt[^>]*>.*?Gestation Period.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "incubation_period": re.compile(r'<dt[^>]*>.*?Incubation Period.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "average_litter_size": re.compile(r'<dt[^>]*>.*?Average Litter Size.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "average_clutch_size": re.compile(r'<dt[^>]*>.*?Average Clutch Size.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "name_of_young": re.compile(r'<dt[^>]*>.*?Name of Young.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "age_of_sexual_maturity": re.compile(r'<dt[^>]*>.*?Age of Sexual Maturity.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "age_of_weaning": re.compile(r'<dt[^>]*>.*?Age of Weaning.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "age_of_fledging": re.compile(r'<dt[^>]*>.*?Age of Fledging.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "type": re.compile(r'<dt[^>]*>.*?<a[^>]*>Type</a>.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),

    # Conservation
    "conservation_status": re.compile(r'<h2[^>]*>.*?Conservation Status</h2>.*?<li><a[^>]*>([^<]+)</a></li>', re.IGNORECASE | re.DOTALL),
    "biggest_threat": re.compile(r'<dt[^>]*>.*?Biggest Threat.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "estimated_population": re.compile(r'<dt[^>]*>.*?Estimated Population Size.*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),

    # Other
    "fun_facts": re.compile(r'<dt[^>]*>\s*<a[^>]*>Fun Fact</a>\s*</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE),
    "number_of_species": re.compile(r'<dt[^>]*>\s*<a[^>]*>Number of Species</a>\s*</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "other_names": re.compile(r'<dt[^>]*>.*?Other Name\(s\).*?</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE | re.DOTALL),
    "group_name": re.compile(r'<dt[^>]*>\s*<a[^>]*>Group</a>\s*</dt>\s*<dd[^>]*>([^<]+)</dd>', re.IGNORECASE),
    
    # Metadata
    "url": re.compile(r'<link\s+rel="canonical"\s+href="([^"]+)"\s*/>', re.IGNORECASE),
}


def decode_html_entities(text): 
    """Decodes HTML entities."""
    if not isinstance(text, str):
        return text

    # A dictionary of common HTML entities
    entities = {
        "&amp;": "&",
        "&lt;": "<",
        "&gt;": ">",
        "&quot;": '"',
        "&#39;": "'",
        "&nbsp;": "",
        "&copy;": "©",
        "&reg;": "®",
        "&trade;": "™",
        "&ndash;": "-",
        "&mdash;": "—",
        "&lsquo;": "`",
        "&rsquo;": "`",
        "&ldquo;": "“",
        "&rdquo;": "”",
        "&#8217;": "'",  
        "&#8211;": "-",  
        "&#8212;": "—", 
    }
    
    pattern = re.compile("|".join(map(re.escape, entities.keys())))
    
    return pattern.sub(lambda m: entities[m.group(0)], text)


def read_html_file(file_path):
    """Reads HTML content from a file."""
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            html = file.read()
        return html
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None
    

def extract_descriptive_sections(html):
    """Extract all <h2> sections and text content"""
    sections = {}

    main_text_match = re.search(r'<div[^>]+id=["\']single-animal-text["\'][^>]*>(.*?)<div[^>]+id=["\']single-animal-sidebar["\']', html, re.DOTALL | re.IGNORECASE)
    if not main_text_match:
        main_text_match = re.search(
            r'<div[^>]+id=["\']single-animal-text["\'][^>]*>(.*)</div>\s*</div>\s*</div>',
            html,
            re.DOTALL | re.IGNORECASE
        )
        if not main_text_match:
            return sections

    content = main_text_match.group(1)

    pattern = re.compile(
        r'<h2[^>]*>(.*?)</h2>(.*?)(?=<h2[^>]*>|$)',
        re.DOTALL | re.IGNORECASE
    )

    for match in pattern.finditer(content):
        header_html = match.group(1)
        text_html = match.group(2)
        
        # Clean header
        header_text = re.sub(r'<[^>]+>', '', header_html).strip()
        header = decode_html_entities(header_text)

        # Skip FAQ
        if "faq" in header.lower():
            continue

        # Clean paragraph text
        paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', text_html, re.DOTALL | re.IGNORECASE)
        paragraph_texts = []
        for p in paragraphs:
            clean_p_html = re.sub(r'<[^>]+>', '', p).strip()
            clean_p = decode_html_entities(clean_p_html)
            clean_p = re.sub(r'\s+', ' ', clean_p)
            if clean_p:
                paragraph_texts.append(clean_p)

        if header and paragraph_texts:
            sections[header] = " ".join(paragraph_texts)

    return sections
    

def read_directory(dir_path, files_pat = "*.html"):
    """Loads html files from a directory."""
    results = {}
    files = []

    try:
        entries = os.listdir(dir_path)
        pattern_reg = files_pat.split("*")
        file_prefix = pattern_reg[0]
        file_suffix = pattern_reg[-1]

        for entry in entries:
            if entry.startswith(file_prefix) and entry.endswith(file_suffix):
                full_path = os.path.join(dir_path, entry)
                if os.path.isfile(full_path):
                    files.append(full_path)
    except Exception as e:
        print(f"Error reading directory {dir_path}: {e}")
        return results
    
    print(f"Found {len(files)} files in {dir_path}")

    # Process each file
    for file_path in files:
        filename = os.path.basename(file_path)
        file = os.path.splitext(filename)[0]
        html_content = read_html_file(file_path)

        data = extract_facts(html_content, file)
        if data:
            key = data.get("common_name", file)
            if key:
                results[key] = data
                print(f"Extracted facts for {key} from {file_path}")
            else:
                print(f"Skipped file {file_path} due to missing common name")
        else:
            print(f"Failed to extract facts from {file_path}")
    
    return results


def extract_facts(html, input_file):
    """Gets animal facts from HTML content."""
    # Base name extraction
    data = {
        "source_file": f"{input_file}.html",
        "common_name": None,
        "common_name_alternative": None,
        "scientific_name": None,
        "taxonomy": {},
        "behavior": {},
        "physical_characteristics": {},
        "biological_facts": {},
        "habitat_distribution": {},
        "conservation": {},
        "descriptions": {},
        "other": {},
        "meta": {}
    }

    # Extract common fields
    common_fields = ["common_name", "common_name_alternative", "scientific_name"]
    for field in common_fields:
        match = RE_PATTERNS[field].search(html)
        if match:
            data[field] = decode_html_entities(match.group(1).strip())

    # Extract taxonomy fields
    taxonomy_fields = ["kingdom", "phylum", "class", "order", "family", "genus"]
    for field in taxonomy_fields:
        match = RE_PATTERNS[field].search(html)
        if match:
            data["taxonomy"][field] = decode_html_entities(match.group(1).strip()) 

    # Extract location fields
    location_fields = ["habitat", "location"]
    for field in location_fields:
        match = RE_PATTERNS[field].search(html)
        if match:
            data["habitat_distribution"][field] = decode_html_entities(match.group(1).strip())

    # Extract locations list
    locations_match = RE_PATTERNS["locations_list"].search(html)
    if locations_match:
        location_html = decode_html_entities(locations_match.group(1).strip())
        locations = re.findall(r'<li><a[^>]*>([^<]+)</a></li>', location_html, re.IGNORECASE)
        data["habitat_distribution"]["locations"] = [loc.strip() for loc in locations]
    else:
        data["habitat_distribution"]["locations"] = None

    # Extract physical characteristics
    physical_fields = ["color", "skin_type", "type", "top_speed", "lifespan", "weight", "length", "height", "wingspan", "distinctive_features"]
    for field in physical_fields:
        match = RE_PATTERNS[field].search(html)
        if match:
            content = decode_html_entities(match.group(1).strip())
            if field == "color" and "<ul" in content:
                colors = re.findall(r'<li[^>]*class="[^"]*list-item[^"]*"[^>]*>([^<]+)</li>', content, re.IGNORECASE)
                data["physical_characteristics"][field] = [c.strip() for c in colors] if colors else content
            else:
                data["physical_characteristics"][field] = content

    # Extract behavior fields
    behavior_fields = ["venomous", "aggression", "life_style", "group_behavior"]
    for field in behavior_fields:
        match = RE_PATTERNS[field].search(html)
        if match:
            content = decode_html_entities(match.group(1).strip())
            # Handle list items
            if "<ul" in content or "<li" in content:
                items = re.findall(r'<li[^>]*class="[^"]*list-item[^"]*"[^>]*>([^<]+)</li>', content, re.IGNORECASE)
                data["behavior"][field] = [item.strip() for item in items] if items else content
            else:
                data["behavior"][field] = content

    # Extract biological facts
    biological_fields = ["diet", "prey", "predators", "gestation_period", "incubation_period",
                        "average_litter_size", "average_clutch_size", "name_of_young",
                        "age_of_sexual_maturity", "age_of_weaning", "age_of_fledging"]
    for field in biological_fields:
        match = RE_PATTERNS[field].search(html)
        if match:   
            data["biological_facts"][field] = decode_html_entities(match.group(1).strip())

    # Extract conservation fields
    conservation_fields = ["conservation_status", "biggest_threat", "estimated_population"]
    for field in conservation_fields:
        match = RE_PATTERNS[field].search(html)
        if match:   
            data["conservation"][field] = decode_html_entities(match.group(1).strip())

    data["descriptions"] = extract_descriptive_sections(html)

    # Extract other fields
    other_fields = ["fun_facts", "number_of_species", "other_names", "group_name"]
    for field in other_fields:
        match = RE_PATTERNS[field].search(html)
        if match:
            data["other"][field] = decode_html_entities(match.group(1).strip())

    # Extract metadata
    url_match = RE_PATTERNS["url"].search(html)
    if url_match:
        data["meta"]["url"] = decode_html_entities(url_match.group(1).strip())

    # Post-processing

    # Move source_file into meta
    if data.get("source_file"):
        data.setdefault("meta", {})["source_file"] = data["source_file"]
        del data["source_file"]

    # common_name_alternative is a list and merge other.other_names
    alt_list = []
    if isinstance(data.get("common_name_alternative"), str) and data["common_name_alternative"].strip():
        alt_list.append(data["common_name_alternative"].strip())
    # Merge other_names if present
    other_names_val = data.get("other", {}).get("other_names")
    if isinstance(other_names_val, str) and other_names_val.strip():
        # Split by commas and common separators
        pieces = re.split(r",|/|\||;|\band\b", other_names_val, flags=re.IGNORECASE)
        for p in pieces:
            name = p.strip()
            if name:
                alt_list.append(name)
        # Remove other_names from other after merging
        try:
            del data["other"]["other_names"]
        except Exception:
            pass
    # Dedupe while preserving order
    seen = set()
    alt_list_dedup = []
    for n in alt_list:
        key = n.lower()
        if key not in seen:
            seen.add(key)
            alt_list_dedup.append(n)
    if alt_list_dedup:
        data["common_name_alternative"] = alt_list_dedup
    else:
        data["common_name_alternative"] = []

    # Normalize conservation status to IUCN two-letter codes
    def normalize_conservation_status(text: str):
        if not isinstance(text, str) or not text.strip():
            return text
        t = text.strip().lower()
        # Already a code
        code_map = {
            "lc": "LC", "nt": "NT", "vu": "VU", "en": "EN", "cr": "CR",
            "ew": "EW", "ex": "EX", "dd": "DD", "ne": "NE"
        }
        if t in code_map:
            return code_map[t]
        # Common phrases and typos
        mappings = [
            ("least concer", "LC"),
            ("least concern", "LC"),
            ("near threatened", "NT"),
            ("vulnerable", "VU"),
            ("endangered", "EN"),
            ("critically endangered", "CR"),
            ("extinct in the wild", "EW"),
            ("extinct", "EX"),
            ("data deficient", "DD"),
            ("not evaluated", "NE")
        ]
        for needle, code in mappings:
            if needle in t:
                return code
        return text

    if data.get("conservation", {}).get("conservation_status"):
        data["conservation"]["conservation_status"] = normalize_conservation_status(
            data["conservation"]["conservation_status"]
        )

    # Move color to physical_color
    if data.get("physical_characteristics", {}).get("color") is not None:
        data["physical_color"] = data["physical_characteristics"].get("color")
        try:
            del data["physical_characteristics"]["color"]
        except Exception:
            pass

    # Move behavior.group_behavior to root group_behavior
    if data.get("behavior", {}).get("group_behavior") is not None:
        data["group_behavior"] = data["behavior"].get("group_behavior")
        try:
            del data["behavior"]["group_behavior"]
        except Exception:
            pass

    # Move habitat_distribution.locations to root locations_range
    if data.get("habitat_distribution", {}).get("locations") is not None:
        data["locations_range"] = data["habitat_distribution"].get("locations")
        try:
            del data["habitat_distribution"]["locations"]
        except Exception:
            pass

    # Flatten remaining `other` keys to root
    if isinstance(data.get("other"), dict):
        for k, v in list(data["other"].items()):
            if k == "other_names":
                continue
            data[k] = v
        try:
            del data["other"]
        except Exception:
            pass

    # Remove the empty dictionaries from data
    keys_to_delete = []
    for key, value in data.items():
        if isinstance(value, dict) and not value:
            keys_to_delete.append(key)
    
    for key in keys_to_delete:
        del data[key]

    # Order output keys
    def order_output_keys(d):
        preferred_sequence = [
            "common_name",
            "common_name_alternative",
            "scientific_name",
            "taxonomy",
            "number_of_species",
            "behavior",
            "physical_characteristics",
            "physical_color",
            "group_behavior",
            "biological_facts",
            "locations_range",
            "habitat_distribution",
            "conservation",
            "descriptions",
            "fun_facts",
            "group_name",
        ]
        ordered = {}
        for k in preferred_sequence:
            if k in d and k != "meta":
                ordered[k] = d[k]
        for k in d.keys():
            if k not in ordered and k != "meta":
                ordered[k] = d[k]
        if "meta" in d:
            ordered["meta"] = d["meta"]
        return ordered

    data = order_output_keys(data)

    return data


def save_json(facts, output_file):
    """Saves extracted facts to a JSON file."""
    try:
        with open(output_file, "w", encoding="utf-8") as file:
            json.dump(facts, file, indent=2, ensure_ascii=False)
        print(f"Saved facts to {output_file}")
    except Exception as e:
        print(f"Error saving to {output_file}: {e}")


if __name__ == "__main__":
    fact_data = read_directory("animals_data", files_pat="*.html")

    if fact_data:
        save_json(fact_data, output_file="data/facts_dataset.json")
    else:
        print("No facts extracted.")