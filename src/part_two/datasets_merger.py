# src/part_two/datasets_merger.py
# Script to merge two animal datasets based on name and scientific name matching

import json
import html
import re

from pyspark.sql import SparkSession, functions as F, types as T, Window

ANIMALS_DATASET = "data/az_animals_dataset.json"
WIKI_DATASET = "data/dataset_wiki.json"
OUTPUT_PATH = "data/all_animals_dataset.json"


def norm_str(string):
    if string is None:
        return None
    string = string.lower().strip()
    string = re.sub(r"[^\w\s]", "", string)  
    string = re.sub(r"\s+", " ", string) 

    return string


def build_rows(data):
    rows = []
    for key, val in data.items():
        rows.append({
            "key": key,
            "common_name": val.get("common_name"),
            "common_name_alternative": val.get("common_name_alternative", []),
            "scientific_name": val.get("scientific_name")
        })
    return rows


def build_wiki_rows(data):
    rows = []
    for key, val in data.items():
        rows.append({
            "wiki_key": key,
            "animal_name": val.get("animal_name"),
            "animal_name_alternative": val.get("animal_name_alternative", []),
            "scientific_names": val.get("scientific_names", []),
            "wiki_id": val.get("wiki_id"),
            "taxon": val.get("taxon"),
            "genus": val.get("genus"),
            "species": val.get("species")
        })
    return rows


def create_dataframe_from_list(spark: SparkSession, rows, schema, num_partitions: int = 64):
    rdd = spark.sparkContext.parallelize(rows or [], numSlices=num_partitions)
    return spark.createDataFrame(rdd, schema=schema)


def get_schemas():
    animals_schema = T.StructType([
        T.StructField("key", T.StringType()),
        T.StructField("common_name", T.StringType()),
        T.StructField("common_name_alternative", T.ArrayType(T.StringType())),
        T.StructField("scientific_name", T.StringType())
    ])
    wiki_schema = T.StructType([
        T.StructField("wiki_key", T.StringType()),
        T.StructField("animal_name", T.StringType()),
        T.StructField("animal_name_alternative", T.ArrayType(T.StringType())),
        T.StructField("scientific_names", T.ArrayType(T.StringType())),
        T.StructField("wiki_id", T.LongType()),
        T.StructField("taxon", T.StringType()),
        T.StructField("genus", T.StringType()),
        T.StructField("species", T.StringType())
    ])

    return animals_schema, wiki_schema


def norm_column(col):
    return F.trim(F.lower(F.regexp_replace(F.regexp_replace(col, r"[^\w\s]", ""), r"\s+", " ")))


def norm_array(col):
    return F.transform(col, lambda x: norm_column(x))


def merge_datasets(test_path, wiki_path, output_path):
    spark = SparkSession.builder \
        .appName("DatasetMerger") \
        .master("local[*]") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .config("spark.python.worker.faulthandler.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "64") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()


    with open(test_path, "r", encoding="utf-8") as f:
        test_data = json.load(f)
    with open(wiki_path, "r", encoding="utf-8") as f:
        wiki_data = json.load(f)


    if isinstance(test_data, list):
        keyed = {}
        for i, rec in enumerate(test_data):
            if not isinstance(rec, dict):
                continue
            key = rec.get("common_name") or rec.get("name") or rec.get("key") or f"row_{i}"
            keyed[str(key)] = rec
        test_data = keyed

    if isinstance(wiki_data, list):
        keyed = {}
        for i, rec in enumerate(wiki_data):
            if not isinstance(rec, dict):
                continue
            key = (rec.get("animal_name")
                   or rec.get("taxon")
                   or (f"{rec.get('genus')} {rec.get('species')}" if rec.get("genus") and rec.get("species") else None)
                   or (str(rec.get("wiki_id")) if rec.get("wiki_id") is not None else None)
                   or f"row_{i}")
            keyed[str(key)] = rec
        wiki_data = keyed

    test_rows = build_rows(test_data)
    wiki_rows = build_wiki_rows(wiki_data)

    test_schema, wiki_schema = get_schemas()

    df_test = create_dataframe_from_list(spark, test_rows, test_schema)
    df_wiki = create_dataframe_from_list(spark, wiki_rows, wiki_schema)

    # Normalized columns
    df_test = (df_test
                .withColumn("common_name_norm", norm_column(F.col("common_name")))
               .withColumn("scientific_name_norm", norm_column(F.col("scientific_name")))
               .withColumn("common_name_alts_norm", norm_array(F.col("common_name_alternative"))))

    df_wiki = (df_wiki
               .withColumn("animal_name_norm", norm_column(F.col("animal_name")))
               .withColumn("scientific_names_norm", norm_array(F.col("scientific_names")))
               .withColumn("scientific_pairs", F.arrays_zip("scientific_names", "scientific_names_norm"))
               .withColumn("animal_name_alternative", F.coalesce(F.col("animal_name_alternative"), F.array(F.lit(""))))
               .withColumn("animal_name_alternative_norm", norm_array(F.col("animal_name_alternative")))
               .withColumn("animal_alt_pairs", F.arrays_zip("animal_name_alternative", "animal_name_alternative_norm"))
               .withColumn("taxon_norm", norm_column(F.col("taxon")))
               .withColumn("genus_species_raw", F.concat_ws(" ", F.col("genus"), F.col("species")))
               .withColumn("genus_species_norm", norm_column(F.col("genus_species_raw")))
               .withColumn("animal_name_base", F.when(F.instr(F.col("animal_name"), F.lit("(")) > 0, F.trim(F.substring_index(F.col("animal_name"), "(", 1))).otherwise(F.col("animal_name")))
               .withColumn("animal_name_base_norm", norm_column(F.col("animal_name_base")))
               )

    # Join condition
    cond_common = df_test.common_name_norm == df_wiki.animal_name_norm
    cond_common_in_wiki_alts = F.array_contains(df_wiki.animal_name_alternative_norm, df_test.common_name_norm)
    cond_common_parenthetical = df_test.common_name_norm == df_wiki.animal_name_base_norm
    cond_alt_equals_animal_name = F.array_contains(df_test.common_name_alts_norm, df_wiki.animal_name_norm)
    cond_alt_equals_animal_name_base = F.array_contains(df_test.common_name_alts_norm, df_wiki.animal_name_base_norm)
    cond_alt_lists = F.size(F.array_intersect(df_test.common_name_alts_norm, df_wiki.animal_name_alternative_norm)) > 0
    cond_scientific_in_list = F.array_contains(df_wiki.scientific_names_norm, df_test.scientific_name_norm)
    cond_scientific_equals_taxon = (df_test.scientific_name_norm == df_wiki.taxon_norm)
    cond_scientific_equals_genus_species = (df_test.scientific_name_norm == df_wiki.genus_species_norm)
    cond_scientific_equals_animal_name = (df_test.scientific_name_norm == df_wiki.animal_name_norm)
    cond_scientific_equals_animal_name_base = (df_test.scientific_name_norm == df_wiki.animal_name_base_norm)

    df_join = (df_test.join(
        df_wiki,
        cond_common | cond_common_in_wiki_alts | cond_common_parenthetical |
        cond_alt_equals_animal_name | cond_alt_equals_animal_name_base | cond_alt_lists |
        cond_scientific_in_list | cond_scientific_equals_taxon | cond_scientific_equals_genus_species |
        cond_scientific_equals_animal_name | cond_scientific_equals_animal_name_base,
        how="left").withColumn("match_type",
                    F.when(cond_common, F.lit(1))
                    .when(cond_common_in_wiki_alts, F.lit(2))
                    .when(cond_common_parenthetical, F.lit(3))
                    .when(cond_alt_equals_animal_name, F.lit(4))
                    .when(cond_alt_lists, F.lit(5))
                    .when(cond_alt_equals_animal_name_base, F.lit(6))
                    .when(cond_scientific_in_list, F.lit(7))
                    .when(cond_scientific_equals_taxon, F.lit(8))
                    .when(cond_scientific_equals_genus_species, F.lit(9))
                    .when(cond_scientific_equals_animal_name, F.lit(10))
                    .when(cond_scientific_equals_animal_name_base, F.lit(11)))
                    .withColumn(
                       "matched_wiki_scientific",
                       F.when(
                       cond_scientific_in_list,
                       F.element_at(F.expr("filter(scientific_pairs, x -> x.scientific_names_norm = scientific_name_norm)"), 1)
                       .getField("scientific_names")
                   )
               )
               .withColumn(
                   "matched_wiki_alt_any",
                   F.when(
                       cond_alt_lists,
                       F.element_at(
                           F.expr("filter(animal_alt_pairs, x -> array_contains(common_name_alts_norm, x.animal_name_alternative_norm))"), 1
                       ).getField("animal_name_alternative")
                   )
               )
               .withColumn(
                   "matched_wiki_alt_from_common",
                   F.when(
                       cond_common_in_wiki_alts,
                       F.element_at(
                           F.expr("filter(animal_alt_pairs, x -> x.animal_name_alternative_norm = common_name_norm)"),1
                       ).getField("animal_name_alternative")
                   )
               )
    )

    # Rank matches per row 
    # 1 common,
    # 2 common in wiki alts,
    # 3 common parenthetical,
    # 4 alt==animal_name,
    # 5 alt list overlap,
    # 6 alt==animal_name_base,
    # 7 scientific list,
    # 8 taxon,
    # 9 genus+species,
    # 10 scientific==animal_name,
    # 11 scientific==animal_name_base
    window = Window.partitionBy("key").orderBy(F.col("match_type"), F.col("wiki_id"))
    df_ranked = df_join.withColumn("rn", F.row_number().over(window))

    # Keep the first ranked row for every key from animal dataset
    df_matched = df_ranked.filter(F.col("rn") == 1)

    # Choose final matched_name
    df_result = df_matched.withColumn(
        "matched_name",
        F.when(F.col("match_type") == 7, F.col("matched_wiki_scientific"))                  # scientific in list
         .when(F.col("match_type") == 8, F.col("taxon"))                                    # taxon match
         .when(F.col("match_type") == 9, F.col("genus_species_raw"))                        # genus+species match
         .when(F.col("match_type") == 10, F.col("animal_name"))                             # scientific equals animal_name
         .when(F.col("match_type") == 11, F.col("animal_name_base"))                        # scientific equals animal_name_base
         .when(F.col("match_type") == 5, F.col("matched_wiki_alt_any"))                     # alt list overlap
         .when(F.col("match_type") == 2, F.col("matched_wiki_alt_from_common"))             # common in wiki alt list
         .when(F.col("match_type") == 6, F.col("animal_name_base"))                         # alt equals animal_name_base
         .otherwise(F.col("animal_name"))                                                   # 1 common == animal_name, 3 common parenthetical, 4 alt==animal_name
    )

    # Collect results
    rows = df_result.select("key", "wiki_key", "matched_name", "match_type", "wiki_id").collect()

    output_dict = {}
    # Initialize stats
    stats = {
        "total_records": len(test_data),
        "matched_records": 0,
        "unmatched_records": 0,
        "match_type_counts": {},
        "appended_scientific": 0,
        "wiki_alt_names_merged": 0,
        "with_meta_wiki_id": 0,
        "wiki_only_records": 0
    }
    for r in rows:
        test_original = test_data.get(r['key']) or {}
        wiki_original = wiki_data.get(r['wiki_key']) if r['wiki_key'] is not None else {}
        merged = {}
        merged.update(test_original)
        # Add wiki keys that do not exist yet or extend if different
        for k, v in (wiki_original or {}).items():
            if k in merged and merged[k] != v and merged[k] is not None and v is not None:
                # If conflicting and both scalar, store unique values as list
                if isinstance(merged[k], (str, int, float)) and isinstance(v, (str, int, float)):
                    if merged[k] != v:
                        merged[k] = [merged[k], v]
                # If both lists, merge unique
                elif isinstance(merged[k], list) and isinstance(v, list):
                    merged[k] = list(dict.fromkeys(merged[k] + v))
            else:
                merged[k] = v
        # Attach match metadata at top-level
        merged["matched_name"] = r["matched_name"]
        # Stats: matched/unmatched and match type counts
        if r["match_type"] is None:
            stats["unmatched_records"] += 1
        else:
            stats["matched_records"] += 1
            mt_key = str(int(r["match_type"]))
            stats["match_type_counts"][mt_key] = stats["match_type_counts"].get(mt_key, 0) + 1
        merged["match_type"] = int(r["match_type"]) if r["match_type"] is not None else None
        merged["wiki_id"] = int(r["wiki_id"]) if r["wiki_id"] is not None else None

        # Post merge cleanup
        # Add scientific_name to scientific_names and remove scientific_name
        sci_name = merged.get("scientific_name")
        sci_names = merged.get("scientific_names")
        if sci_names is None:
            sci_names = []
        if isinstance(sci_names, str):
            sci_names = [sci_names]
        if sci_name and sci_name not in sci_names:
            sci_names.append(sci_name)
            stats["appended_scientific"] += 1
        if sci_names:
            # ensure unique order-preserving
            merged["scientific_names"] = list(dict.fromkeys(sci_names))
        if "scientific_name" in merged:
            del merged["scientific_name"]

        # Merge animal_name_alternative into common_name_alternative and remove animal_name_alternative
        cna = merged.get("common_name_alternative")
        ana = merged.get("animal_name_alternative")
        if cna is None:
            cna = []
        if isinstance(cna, str):
            cna = [cna]
        if isinstance(ana, str):
            ana = [ana]
        # Count unique wiki alts added
        def _clean_str_list(lst):
            out = []
            for x in lst:
                if isinstance(x, str):
                    v = x.strip()
                    if v:
                        out.append(v)
            return list(dict.fromkeys(out))
        cna_before_set = set(_clean_str_list(cna))
        ana_clean = _clean_str_list(ana if isinstance(ana, list) else [])
        new_from_wiki = [x for x in ana_clean if x not in cna_before_set]
        if new_from_wiki:
            stats["wiki_alt_names_merged"] += len(list(dict.fromkeys(new_from_wiki)))
        if isinstance(ana, list):
            cna = cna + ana
        # filter blanks and dedupe
        cna = _clean_str_list(cna)
        if cna:
            merged["common_name_alternative"] = cna
        if "animal_name_alternative" in merged:
            del merged["animal_name_alternative"]

        # Add wiki_id to meta
        meta = merged.get("meta")
        if meta is None or not isinstance(meta, dict):
            meta = {} if meta is None else {"original_meta": meta}
        meta["wiki_id"] = merged.get("wiki_id")
        merged["meta"] = meta
        if meta.get("wiki_id") is not None:
            stats["with_meta_wiki_id"] += 1

        # Remove top-level wiki_id
        if "wiki_id" in merged:
            del merged["wiki_id"]
        # Remove redundant top-level fields
        for redundant_key in ["animal_name", "status", "taxon", "matched_name", "genus"]:
            if redundant_key in merged:
                del merged[redundant_key]

        # Clean abstract from wiki styling and combine with text_sections
        abs_text = merged.get("abstract")
        if isinstance(abs_text, str):
            try:
                abs_clean = html.unescape(abs_text)
            except Exception:
                abs_clean = abs_text
            # Remove wiki bold/italic markup
            abs_clean = re.sub(r"''+", "", abs_clean)
            # Normalize whitespace
            abs_clean = re.sub(r"\s+", " ", abs_clean).strip()
        else:
            abs_clean = None

        ts = merged.get("text_sections")
        if isinstance(ts, list) and ts:
            ts_join = " ".join([t.strip() for t in ts if isinstance(t, str) and t.strip()])
            if ts_join:
                abs_clean = f"{abs_clean} {ts_join}".strip() if abs_clean else ts_join
        if abs_clean is not None:
            merged["abstract"] = abs_clean
        # Remove text_sections 
        if "text_sections" in merged:
            del merged["text_sections"]

        output_dict[r["key"]] = merged

    # Include wiki-only records
    matched_wiki_keys = set([r["wiki_key"] for r in rows if r["wiki_key"] is not None])
    for wk, wval in wiki_data.items():
        if wk in matched_wiki_keys:
            continue
        merged = {}
        merged.update(wval or {})
        # Map wiki fields to animals schema equivalents
        # common_name from animal_name
        if merged.get("animal_name") and not merged.get("common_name"):
            merged["common_name"] = merged.get("animal_name")
        # common_name_alternative from animal_name_alternative
        ana = merged.get("animal_name_alternative")
        if ana is not None and not merged.get("common_name_alternative"):
            merged["common_name_alternative"] = ana if isinstance(ana, list) else [ana]
        # Ensure scientific_names exists and includes taxon/genus+species if present
        sci_names = merged.get("scientific_names")
        if sci_names is None:
            sci_names = []
        if isinstance(sci_names, str):
            sci_names = [sci_names]
        taxon = merged.get("taxon")
        gs_raw = None
        if merged.get("genus") and merged.get("species"):
            gs_raw = f"{merged.get('genus')} {merged.get('species')}".strip()
        for candidate in [taxon, gs_raw]:
            if candidate and candidate not in sci_names:
                sci_names.append(candidate)
        if sci_names:
            merged["scientific_names"] = list(dict.fromkeys(sci_names))
        # Wiki_id into meta and remove top-level wiki_id
        meta = merged.get("meta")
        if meta is None or not isinstance(meta, dict):
            meta = {} if meta is None else {"original_meta": meta}
        meta["wiki_id"] = merged.get("wiki_id")
        merged["meta"] = meta
        if meta.get("wiki_id") is not None:
            stats["with_meta_wiki_id"] += 1
        if "wiki_id" in merged:
            del merged["wiki_id"]
        # Clean abstract and combine text_sections
        abs_text = merged.get("abstract")
        if isinstance(abs_text, str):
            try:
                abs_clean = html.unescape(abs_text)
            except Exception:
                abs_clean = abs_text
            abs_clean = re.sub(r"''+", "", abs_clean)
            abs_clean = re.sub(r"\s+", " ", abs_clean).strip()
        else:
            abs_clean = None
        ts = merged.get("text_sections")
        if isinstance(ts, list) and ts:
            ts_join = " ".join([t.strip() for t in ts if isinstance(t, str) and t.strip()])
            if ts_join:
                abs_clean = f"{abs_clean} {ts_join}".strip() if abs_clean else ts_join
        if abs_clean is not None:
            merged["abstract"] = abs_clean
        if "text_sections" in merged:
            del merged["text_sections"]
        # Remove redundant fields
        for redundant_key in ["animal_name", "status", "taxon", "matched_name", "genus"]:
            if redundant_key in merged:
                del merged[redundant_key]
        output_dict[str(wk)] = merged
        stats["wiki_only_records"] += 1

    with open(output_path, "w", encoding="utf-8") as out_f:
        json.dump(output_dict, out_f, ensure_ascii=False, indent=2)

    # Additional stats
    total = stats.get("total_records", 0)
    matched = stats.get("matched_records", 0)
    stats["match_rate"] = (matched / total) if total else 0.0
    match_type_labels = {
        "1": "common_name == animal_name",
        "2": "common_name in wiki alternatives",
        "3": "common_name == parenthetical base",
        "4": "alt == animal_name",
        "5": "alt lists overlap",
        "6": "alt == animal_name_base",
        "7": "scientific in list",
        "8": "scientific == taxon",
        "9": "scientific == genus+species",
        "10": "scientific == animal_name",
        "11": "scientific == animal_name_base"
    }
    stats["match_type_labels"] = match_type_labels
    stats_path = output_path.replace(".json", "_stats.json")
    with open(stats_path, "w", encoding="utf-8") as sf:
        json.dump(stats, sf, ensure_ascii=False, indent=2)

    spark.stop()


if __name__ == "__main__":
    merge_datasets(ANIMALS_DATASET, WIKI_DATASET, OUTPUT_PATH)