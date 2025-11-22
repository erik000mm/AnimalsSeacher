# src/part_one/search.py
# Script to perform boolean search queries on an inverted index

import json
import re
from collections import defaultdict
from indexer import clean_text, tokenize, format_key

OPERATORS = {"&", "||", "(",")"}
INDEX_PATH = "index.json"


def tokenize_query(query):
    """Tokenizes the boolean query string."""
    # If user uses AND/OR, replace them with & and ||
    query = re.sub(r'\bAND\b', '&', query, flags=re.IGNORECASE)
    query = re.sub(r'\bOR\b', '||', query, flags=re.IGNORECASE)
    # Add spaces around operators and parentheses
    query = re.sub(r'(\(|\)|\|\||&)', r' \1 ', query)

    tokens = query.split()
    processed_tokens = []

    for token in tokens:
        if token in OPERATORS:
            processed_tokens.append(token)
            continue
        # If token has ':' its a specific field search
        if ":" in token:
            # Split only on the first ':'
            split = token.split(":", 1)
            if len(split) == 2 and split[0] and split[1]:
                field = format_key(split[0])
                value_cleaned = clean_text(split[1])
                value_tokens = tokenize(value_cleaned)
                if value_tokens:
                    value_term = value_tokens[0]
                    fused_term = f"{field}_{value_term}"
                    processed_tokens.append(fused_term)
            else:
                # Treat as normal token if malformed
                cleaned_token = clean_text(token.replace(":", " "))
                index_tokens = tokenize(cleaned_token)
                if index_tokens:
                    processed_tokens.append(index_tokens[0])
        # Clean and tokenize normally
        else:
            cleaned_token = clean_text(token)
            index_tokens = tokenize(cleaned_token)
            if index_tokens:
                processed_tokens.append(index_tokens[0])

    return processed_tokens


def postfix(tokens):
    """Shunting Yard algorithm to convert infix to postfix."""
    precedence = {"&": 2, "||": 1}
    output = []
    stack = []

    for token in tokens:
        if token not in OPERATORS:
            output.append(token)
        elif token == "(":
            stack.append(token)
        elif token == ")":
            while stack and stack[-1] != "(":
                output.append(stack.pop())
            stack.pop()
        else:
            while (stack and stack[-1] != "(" and
                   precedence.get(token, 0) <= precedence.get(stack[-1], 0)):
                output.append(stack.pop())
            stack.append(token)

    while stack:
        output.append(stack.pop())

    return output


def get_docs_from_index(index):
    """Collect all document IDs from the inverted index."""
    all_doc_ids = set()

    for term_data in index.values():
        doc_freq_map = term_data.get("docID/freq", {})
        
        # Add all doc IDs to the set
        all_doc_ids.update(doc_freq_map.keys())
        
    return {str(doc_id) for doc_id in all_doc_ids}


def get_matching_docs(tokens, index):
    """Evaluates the postfix expression to get matching document IDs."""
    stack = []
    postings = {}

    for token in tokens:
        if token not in OPERATORS and token not in postings:
            if token in index:
                postings[token] = set(index[token]["docID/freq"].keys())
            else:
                postings[token] = set()
            stack.append(postings[token])
        elif token not in OPERATORS:
            stack.append(postings[token].copy())
        elif token == "&":
            right = stack.pop()
            left = stack.pop()
            stack.append(left.intersection(right))
        elif token == "||":
            right = stack.pop()
            left = stack.pop()
            stack.append(left.union(right))
    
    return stack[0] if stack else set()


def tfidf(query_terms, matching_docs, index, idf_key):
    """TF-IDF ranking function. Uses precomputed IDF values."""
    scores = defaultdict(float)
    idfs = {}

    for term in query_terms:
        term_data = index.get(term)
        freq = term_data.get("freq", 0) if term_data else 0
        if term_data and freq > 0:
            if idf_key in term_data:
                idfs[term] = term_data[idf_key]
        else:
            idfs[term] = 0.0

    # Score documents
    for doc_id in matching_docs:
        doc_id_str = str(doc_id)
        for term in query_terms:
            tf = index.get(term, {}).get("docID/freq", {}).get(doc_id_str, 0)
            if tf:
                try:
                    tf_val = float(tf)
                except Exception:
                    tf_val = 0.0
                scores[doc_id_str] += tf_val * idfs.get(term, 0.0)

    return sorted(scores.items(), key=lambda item: item[1], reverse=True)


def rank_tfidf(query_terms, matching_docs, index):
    """Ranks documents using TF-IDF raw TF * Standard IDF."""
    return tfidf(query_terms, matching_docs, index, idf_key="idf_standard")


def rank_tfidf_smooth(query_terms, matching_docs, index):
    """Ranks documents using raw TF * Smoothed IDF."""
    return tfidf(query_terms, matching_docs, index, idf_key="idf_smooth")


def process_query(index):
    """Processes user input queries in a loop."""
    while True:
        try:
            query = input("\nEnter boolean query (example: term1 & field:term2): ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting search.")
            break

        if not query:
            print("Empty query. Try again.")
            continue

        try:
            # Tokenize the query
            query_tokens = tokenize_query(query)
            if not query_tokens:
                print("No valid tokens found in query. Try again.")
                continue
            print(f"Tokenized Query: {query_tokens}")

            # Convert to postfix
            postfix_query = postfix(query_tokens)
            print(f"Postfix: {postfix_query}")

            matching_docs = get_matching_docs(postfix_query, index)

            if not matching_docs:
                print("No documents matched the query.")

        except Exception as e:
            print(f"Error processing query: {e}")
            continue

        print(f"\nFound {len(matching_docs)} documents matching the query: {', '.join(sorted(matching_docs))}")
        ranking_terms = sorted({
            t for t in query_tokens
            if t not in {'&', '||', '(', ')'}
        })

        # Rank using TF-IDF
        ranked_tfidf = rank_tfidf(ranking_terms, matching_docs, index)
        print("\nTop 5 results using TF-IDF:")
        for doc_id, score in ranked_tfidf[:5]:
            print(f"Document ID: {doc_id}, Score: {score}")
        print("="*40)

        # Rank using TF-IDF with smoothing
        ranked_tfidf_smooth = rank_tfidf_smooth(ranking_terms, matching_docs, index)
        print("\nTop 5 results using TF-IDF with smoothing:")
        for doc_id, score in ranked_tfidf_smooth[:5]:
            print(f"Document ID: {doc_id}, Score: {score}")

        print("\n" + "-"*40)


if __name__ == "__main__":
    with open(INDEX_PATH, "r", encoding="utf-8") as f:
        index = json.load(f)
    print(f"Loaded index from {INDEX_PATH} with {len(index)} terms.")

    process_query(index)