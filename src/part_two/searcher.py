import argparse
import os
import sys
import lucene
from typing import List, Tuple

from java.nio.file import Paths
from org.apache.lucene.analysis.core import KeywordAnalyzer
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.search import IndexSearcher, BooleanClause, BooleanQuery
from org.apache.lucene.store import FSDirectory


def search_index(index_dir, query_text, top_k: int = 5):
    if not lucene.getVMEnv() or not lucene.getVMEnv().isCurrentThreadAttached():
        lucene.initVM(vmargs=["-Djava.awt.headless=true"])
    directory = FSDirectory.open(Paths.get(index_dir))
    reader = DirectoryReader.open(directory)
    searcher = IndexSearcher(reader)
    
    # Analyzers
    standard_analyzer = StandardAnalyzer()
    keyword_analyzer = KeywordAnalyzer() 
    
    # Map fields to analyzers
    field_analyzers = {
        "name": standard_analyzer,
        "content": standard_analyzer, 
        "exact_name": keyword_analyzer,
    }
    fields = list(field_analyzers.keys())
    
    try:
        builder = BooleanQuery.Builder()
        for field in fields:
            analyzer = field_analyzers[field]
            sub_parser = QueryParser(field, analyzer)
            sub_query = sub_parser.parse(query_text)
            builder.add(sub_query, BooleanClause.Occur.SHOULD)
        query = builder.build()
    except Exception as e:
        reader.close()
        directory.close()
        print(f"Error parsing query '{query_text}': {e}")
        
    hits = searcher.search(query, top_k).scoreDocs
    def _retrieve_document(doc_id: int):
        attempts = []
        attempts.append(lambda: searcher.doc(doc_id)) 
        if hasattr(reader, "document"):
            attempts.append(lambda: reader.document(doc_id))
        try:
            ir = searcher.getIndexReader()
            if hasattr(ir, "document"):
                attempts.append(lambda ir=ir: ir.document(doc_id))
        except Exception:
            pass
        if hasattr(searcher, "storedFields"):
            try:
                sf = searcher.storedFields()
                if hasattr(sf, "document"):
                    attempts.append(lambda sf=sf: sf.document(doc_id))
            except Exception:
                pass
        last_err = None
        for attempt in attempts:
            try:
                return attempt()
            except Exception as e:
                last_err = e
        raise RuntimeError(f"Unable to retrieve stored fields for docID {doc_id}. Attempts={len(attempts)} last_error={last_err}")

    results: List[Tuple[str, float, str]] = []
    for sd in hits:
        doc = _retrieve_document(sd.doc)
        results.append((doc.get("exact_name"), sd.score, doc.get("content") or ""))
    reader.close()
    directory.close()
    return results


def searcher(argv: List[str] = None):
    argv = argv if argv is not None else sys.argv[1:]
    parser = argparse.ArgumentParser(
        description="Searcher",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("query", help="Query string")
    parser.add_argument("--index", default=os.path.join("index", "animals"), help="Lucene index")
    parser.add_argument("--top", type=int, default=5, help="Number of top results to return")
    
    args = parser.parse_args(argv)
    
    results = search_index(args.index, args.query, top_k=args.top)
    if not results:
        print("No results")
        return 0
    # Truncate long content snippets
    for i, (name, score, content) in enumerate(results, 1):
        snippet = (content or "").replace("\n", " ")
        if len(snippet) > 160:
            snippet = snippet[:157] + "..."
        print(f"{i:2d}. {name}  (score={score:.3f})\n      {snippet}")
    return 0


if __name__ == "__main__":
    raise SystemExit(searcher())
