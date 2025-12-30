from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import os
from collections import defaultdict

load_dotenv()

es_db = Elasticsearch([os.getenv('ELASTICSEARCH_DB_URL')], request_timeout=100)

query = {
    'bool': {
        'must': [
            {'terms': {'type': ['facebook', 'tiktok', 'youtube', 'voz', 'xamvn', 'otofun', 'media', 'reddit']}},
            {'terms': {'topic_id': ['all']}},
            {'terms': {'tenant_id': ['A05']}},
            {'range': {'date': {'gte': '11/24/2025', 'lte': '12/01/2025'}}}
        ]
    }
}

response = es_db.search(index='key-trend-current-v2', body={'query': query, 'size': 100})
hits = response['hits']['hits']

print(f'Total hits: {len(hits)}')
print()

# Group by date and type
date_type_count = defaultdict(int)
for hit in hits:
    doc = hit['_source']
    date = doc.get('date', 'unknown')
    doc_type = doc.get('type', 'unknown')
    date_type_count[(date, doc_type)] += 1

print('Documents per date and type:')
for (date, doc_type), count in sorted(date_type_count.items()):
    print(f'  {date} - {doc_type}: {count} documents')

print()

# Check a specific keyword across all hits
target_keyword = 'a 321'
print(f'Checking keyword "{target_keyword}" across all documents:')
for hit in hits:
    doc = hit['_source']
    date = doc.get('date', 'unknown')
    doc_type = doc.get('type', 'unknown')
    keywords_trend = doc.get('keywords_trend', [])
    
    for i, kw in enumerate(keywords_trend):
        if kw.get('keyword') == target_keyword:
            print(f'  Found in {date} - {doc_type} at position {i}: score={kw.get("score")}, record={kw.get("record")}, isTrend={kw.get("isTrend")}')

print()

# Check how many keywords appear in multiple days
keyword_occurrences = defaultdict(list)
for hit in hits:
    doc = hit['_source']
    date = doc.get('date', 'unknown')
    doc_type = doc.get('type', 'unknown')
    keywords_trend = doc.get('keywords_trend', [])
    
    for i, kw in enumerate(keywords_trend):
        if kw.get('isTrend', False):
            keyword = kw.get('keyword')
            keyword_occurrences[keyword].append({
                'date': date,
                'type': doc_type,
                'position': i,
                'score': kw.get('score'),
                'record': kw.get('record')
            })

# Find keywords that appear more than once
multi_occurrence = {k: v for k, v in keyword_occurrences.items() if len(v) > 1}
print(f'Keywords appearing in multiple documents (with isTrend=True): {len(multi_occurrence)}')

if multi_occurrence:
    print('\nTop 5 keywords appearing most frequently:')
    sorted_multi = sorted(multi_occurrence.items(), key=lambda x: len(x[1]), reverse=True)[:5]
    for keyword, occurrences in sorted_multi:
        print(f'\n  "{keyword}" appears {len(occurrences)} times:')
        for occ in occurrences:
            print(f'    - {occ["date"]} / {occ["type"]} at position {occ["position"]}')
