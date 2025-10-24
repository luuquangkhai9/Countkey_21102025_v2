from elasticsearch import Elasticsearch
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
import urllib3
import time
# es = Elasticsearch(["http://172.168.200.202:9200"] , request_timeout=100)
import os

import re
from tqdm import tqdm
import py_vncorenlp
from langchain_text_splitters import RecursiveCharacterTextSplitter

load_dotenv()

# Lấy URL Elasticsearch từ biến môi trường
elasticsearch_url = os.getenv("ELASTICSEARCH_URL")
# Khởi tạo Elasticsearch client
es = Elasticsearch([elasticsearch_url], request_timeout=100)
logger = logging.getLogger("run")  # Sử dụng logger đã được cấu hình
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.getLogger("urllib3").propagate = False

# Giảm log cho Elasticsearch (nếu cần thiết)
logging.getLogger("elasticsearch").setLevel(logging.CRITICAL)
# Remove all handlers from urllib3
class ExcludeHttpLogsFilter(logging.Filter):
    def filter(self, record):
        # Lọc bỏ các log chứa "POST" hoặc "HEAD"
        return "POST" not in record.getMessage() and "HEAD" not in record.getMessage()

# Thêm bộ lọc vào logger
logging.getLogger("elasticsearch").addFilter(ExcludeHttpLogsFilter())
logging.getLogger("urllib3").addFilter(ExcludeHttpLogsFilter())

def query_keyword_with_topic(es, start_date_str, end_date_str, type):
    try:
        logger.info(f"Starting query_keyword_with_topic for type: {type}, from {start_date_str} to {end_date_str}")

        # Hàm chung để truy vấn Elasticsearch với search_after
        def fetch_records(index, query_body):
            records = []
            search_after_value = None  # Lưu giá trị search_after

            try:
                while True:
                    # Nếu đã có search_after_value, thêm vào body
                    if search_after_value:
                        query_body["search_after"] = search_after_value

                    # Thực hiện truy vấn
                    result = es.options(request_timeout=2000).search(index=index, body=query_body)

                    hits = result['hits']['hits']
                    if not hits:
                        logger.info(f"No more records found in index: {index}")

                        break  # Kết thúc khi không còn dữ liệu

                    for hit in hits:
                        # Xử lý dữ liệu cho từng chỉ mục
                        if index == 'posts':
                            if 'topic_id' not in hit['_source']:
                                hit['_source']['topic_id'] = []
                            # if 'hashtag' not in hit['_source']:
                            #     hit['_source']['hashtag'] = []

                            keywords = hit['_source'].get('keyword', [])
                            keywords = keywords if isinstance(keywords, list) else []

                            key_words = hit['_source'].get('key_word', [])
                            key_words = key_words if isinstance(key_words, list) else []

                            # Lấy danh sách từ theo điều kiện từ trường key_word_type
                            key_word_type = hit['_source'].get('key_word_type', [])
                            filtered_words = []

                            if isinstance(key_word_type, list) and key_word_type:
                                for item in key_word_type:
                                    if isinstance(item, list) and len(item) == 2:  # Đảm bảo item là danh sách có 2 phần tử
                                        w, pos = item
                                        # if (pos in ['Np', 'Ny', 'Nb', 'Vb'] or (pos in ['N'] and '_' in w and not w.startswith('_'))):
                                        if (pos in ['Np', 'Ny', 'Nb', 'Vb'] or (pos in ['N'] and '_' in w and not w.startswith('_'))):

                                            filtered_words.append(w.lower())
                                # Nếu key_word_type không rỗng, chỉ lấy các từ từ đây
                                combined_keywords = list(set(filtered_words))
                            else:
                                # Nếu key_word_type rỗng, kết hợp keywords và key_word
                                combined_keywords = list(set(keywords + key_words))

                            # Cập nhật lại keyword trong hit['_source']
                            hit['_source']['keyword'] = combined_keywords

                            # Xóa các trường không cần thiết
                            if 'key_word' in hit['_source']:
                                del hit['_source']['key_word']
                            if 'key_word_type' in hit['_source']:
                                del hit['_source']['key_word_type']

                            

                            hashtag = hit['_source'].get('hashtag', [])
                            hashtag = hashtag if isinstance(hashtag, list) else []

                            hit['_source']['hashtag'] = hashtag

                        elif index == 'comments':
                            if 'topic_id' not in hit['_source']:
                                hit['_source']['topic_id'] = []

                            keywords = hit['_source'].get('keywords', [])
                            keywords = keywords if isinstance(keywords, list) else []

                            key_words = hit['_source'].get('key_word', [])
                            key_words = key_words if isinstance(key_words, list) else []
                            
                            key_word_type = hit['_source'].get('key_word_type', [])
                            filtered_words = []

                            if isinstance(key_word_type, list) and key_word_type:
                                for item in key_word_type:
                                    if isinstance(item, list) and len(item) == 2:  # Đảm bảo item là danh sách có 2 phần tử
                                        w, pos = item
                                        if (pos in ['Np', 'Ny', 'Nb', 'Vb'] or (pos in ['N'] and '_' in w and not w.startswith('_'))):
                                            filtered_words.append(w.lower())
                                # Nếu key_word_type không rỗng, chỉ lấy các từ từ đây
                                combined_keywords = list(set(filtered_words))
                            else:
                                combined_keywords = list(set(keywords + key_words))
                            hit['_source']['keywords'] = combined_keywords

                            hashtags = hit['_source'].get('hashtags', [])
                            hashtags = hashtags if isinstance(hashtags, list) else []

                            hashtag = hit['_source'].get('hashtag', [])
                            hashtag = hashtag if isinstance(hashtag, list) else []

                            combined_hashtag = list(set(hashtags + hashtag))
                            hit['_source']['hashtags'] = combined_hashtag
                            if 'key_word' in hit['_source']:
                                del hit['_source']['key_word']
                            if 'key_word_type' in hit['_source']:
                                del hit['_source']['key_word_type']

                            if 'hashtag' in hit['_source']:
                                del hit['_source']['hashtag']

                        records.append(hit)

                    # Lấy giá trị sort cuối cùng từ kết quả để sử dụng cho search_after
                    search_after_value = hits[-1]['sort']
                    # logger.info(f"Fetched {len(hits)} records from index: {index}, continuing...")

            except Exception as e:
                logger.error(f"Error fetching records from {index}: {e}")

            return records  # Đảm bảo trả về danh sách (có thể rỗng)

        # Định nghĩa truy vấn Elasticsearch cho cả hai chỉ mục
        body_posts = {
            "query": {
                "bool": {
                    "must": [
                        {"match_phrase": {"type": type}},
                        {"range": {"created_time": {"gte": start_date_str, "lte": end_date_str}}}
                    ]
                }
            },
            "sort": [{"created_time": {"order": "asc"}}, {"time_crawl.keyword": {"order": "asc"}}],
            "_source": ["keyword", "hashtag", "created_time", "topic_id", "key_word", "key_word_type","tenancy_ids"],
            "size": 4000
        }

        body_comments = {
            "query": {
                "bool": {
                    "must": [
                        {"match_phrase": {"type": type}},
                        {"range": {"created_time": {"gte": start_date_str, "lte": end_date_str}}}
                    ]
                }
            },
            "sort": [{"created_time": {"order": "asc"}}, {"time_crawl.keyword": {"order": "asc"}}],
            "_source": ["keywords", "hashtags", "created_time", "topic_id", "key_word", "hashtag", "key_word_type","tenancy_ids"],
            "size": 4000
        }

        # Lấy dữ liệu từ cả hai chỉ mục
        records_posts = fetch_records('posts', body_posts)
        logger.info(f"Fetched {len(records_posts)} records from posts index.")

        records_comments = fetch_records('comments', body_comments)
        logger.info(f"Fetched {len(records_comments)} records from comments index.")

        # Gộp dữ liệu từ cả hai chỉ mục
        records = records_posts + records_comments
        logger.info(f"Total records fetched: {len(records)}")

        # Lưu dữ liệu vào file JSON
        # with open(query_data_file, 'w', encoding='utf-8') as f:
        #     json.dump(records, f, ensure_ascii=False, indent=4)

        return records

    except Exception as e:
        logger.error(f"An error occurred during query_keyword_with_topic: {e}")

        return []
    

# def query_keyword_with_trend(es,INDEX, body):
#     try:


#         # Hàm chung để truy vấn Elasticsearch với search_after
#         def fetch_records(index, query_body):
#             records = []
#             search_after_value = None  # Lưu giá trị search_after

#             try:
#                 while True:
#                     # Nếu đã có search_after_value, thêm vào body
#                     if search_after_value:
#                         query_body["search_after"] = search_after_value

#                     # Thực hiện truy vấn
#                     result = es.options(request_timeout=2000).search(index=index, body=query_body)

#                     hits = result['hits']['hits']
#                     if not hits:
#                         logger.info(f"No more records found in index: {index}")

#                         break  # Kết thúc khi không còn dữ liệu

#                     for hit in hits:
#                         # Xử lý dữ liệu cho từng chỉ mục
#                         records.append(hit["_source"])

#                     # Lấy giá trị sort cuối cùng từ kết quả để sử dụng cho search_after
#                     search_after_value = hits[-1]["sort"]
#                     # logger.info(f"Fetched {len(hits)} records from index: {index}, continuing...")

#             except Exception as e:
#                 logger.error(f"Error fetching records from {index}: {e}")

#             return records  # Đảm bảo trả về danh sách (có thể rỗng)

#         # Định nghĩa truy vấn Elasticsearch cho cả hai chỉ mục


#         # Lấy dữ liệu từ cả hai chỉ mục


#         records = fetch_records(INDEX, body)
#         logger.info(f"Total records fetched: {len(records)}")

#         # Lưu dữ liệu vào file JSON
#         # with open(query_data_file, 'w', encoding='utf-8') as f:
#         #     json.dump(records, f, ensure_ascii=False, indent=4)

#         return records
#     except Exception as e:
#         logger.error(f"An error occurred during query_keyword_with_topic: {e}")

#         return []



VNCORE_MODEL_DIR = r'C:\Users\Administrator\Documents\Intern_Source\Countkey_21102025_v2\vncorenlp'
# BLACKLIST_FILE_PATH = r"C:\Users\Administrator\Documents\Fixed_key_countkey\blacklist_keywords.txt"

logger.info(f"Đang khởi tạo VnCoreNLP từ: {VNCORE_MODEL_DIR} (chỉ một lần)...")
print(f"Đang khởi tạo VnCoreNLP từ: {VNCORE_MODEL_DIR} (chỉ một lần)...")
try:
    VNCORE_MODEL = py_vncorenlp.VnCoreNLP(save_dir=VNCORE_MODEL_DIR)
    logger.info("✅ VnCoreNLP đã khởi tạo thành công.")
    print("✅ VnCoreNLP đã khởi tạo thành công.")
except Exception as e:
    logger.error(f"❌ KHÔNG THỂ KHỞI TẠO VNCORENLP. Lỗi: {e}")
    print(f"❌ KHÔNG THỂ KHỞI TẠO VNCORENLP. Lỗi: {e}")
    # exit()
    
# TEXT_SPLITTER = RecursiveCharacterTextSplitter(chunk_size=10000, chunk_overlap=50)

# # --- 1.2: Các hàm và quy tắc trích xuất ---

# def load_stopwords(file_path):
#     """Tải stopwords từ file."""
#     stopwords = []
#     try:
#         with open(file_path, "r", encoding="utf-8") as f:
#             for line in f:
#                 word = line.strip().strip("'")
#                 if word:
#                     stopwords.append(word.lower())
#         logger.info(f"✅ Đã tải {len(stopwords)} từ khóa blacklist từ: {file_path}")
#         return tuple(stopwords)
#     except FileNotFoundError:
#         logger.error(f"❌ Không tìm thấy file blacklist: {file_path}. Sẽ tiếp tục mà không có blacklist.")
#         return tuple()

# BLACKLISTED_START_WORDS = load_stopwords(BLACKLIST_FILE_PATH)

# CHUNK_GRAMMAR = [
#     ("LEGAL_DOC_RULE_2", r"(?:<Np>|<N>)(?:\s+<N>)*(?:\s+<M>)+(?:\s+(?:<Np>|<N>|<Ny>))+"),
#     ("LEGAL_DOC_RULE", r"(?:<Np>|<N>)(?:\s+<N>)*(\s+<M>)+"),
#     ("NOUN_RULE", r"(?:<Np>|<N>|<Ny>)(?:\s+(?:<Np>|<N>|<Ny>))*"),
# ]


# def compile_rules(rules):
#     """Biên dịch các quy tắc regex."""
#     return [
#         (name, re.compile(rule.replace("<", r"(?:\\b").replace(">", r"\\b)")))
#         for name, rule in rules
#     ]

# COMPILED_CHUNK_RULES = compile_rules(CHUNK_GRAMMAR)

# def clean_text(text):
#     """Dọn dẹp văn bản cơ bản."""
#     if not text:
#         return ""
#     text = re.sub(r'[-–—]+', ' ', text)
#     text = ' '.join(text.split())
#     return text

# def is_valid_single_word_keyword(chunk_word, rule_name):
#     """Kiểm tra xem từ đơn có hợp lệ không."""
#     tag = chunk_word.get('posTag')
#     word_form = chunk_word.get('wordForm', '')
#     if tag == 'Np' and len(word_form) > 5:
#         return True
#     if tag == 'Ny':
#         if word_form.isupper() and len(word_form) >= 2:
#             return True
#     if rule_name == "VERB_RULE" and tag == 'V' and len(word_form) > 5:
#         return True
#     return False

# def extract_keywords_2(annotated_data):
#     """Hàm trích xuất từ khóa chính từ dữ liệu đã được VnCoreNLP gán nhãn."""
#     seen_keywords_lower = set()
#     ordered_keywords = []
#     if not isinstance(annotated_data, dict):
#         return []
#     for sentence_words in annotated_data.values():
#         if not sentence_words:
#             continue
#         pos_sequence = " ".join([word.get('posTag', '') for word in sentence_words])
#         claimed_indices = [False] * len(sentence_words)
#         for rule_name, rule_pattern in COMPILED_CHUNK_RULES:
#             for match in rule_pattern.finditer(pos_sequence):
#                 start_char, end_char = match.span()
#                 start_word = len(pos_sequence[:start_char].strip().split()) if start_char > 0 else 0
#                 end_word = len(pos_sequence[:end_char].strip().split())
#                 if any(claimed_indices[i] for i in range(start_word, end_word)):
#                     continue
#                 chunk_words = sentence_words[start_word:end_word]
#                 phrase_text = ' '.join(w.get('wordForm', '') for w in chunk_words).replace("_", " ")
#                 is_valid = False
#                 if len(chunk_words) > 1:
#                     is_valid = True
#                 elif len(chunk_words) == 1:
#                     if is_valid_single_word_keyword(chunk_words[0], rule_name):
#                         is_valid = True
#                 if is_valid and phrase_text:
#                     lower_phrase = phrase_text.lower()
#                     if lower_phrase not in seen_keywords_lower:
#                         is_blacklisted = False
#                         for blacklisted_word in BLACKLISTED_START_WORDS:
#                             if blacklisted_word in lower_phrase:
#                                 is_blacklisted = True
#                                 break
#                         if is_blacklisted:
#                             continue
#                         seen_keywords_lower.add(lower_phrase)
#                         ordered_keywords.append(phrase_text)
#                         for i in range(start_word, end_word):
#                             claimed_indices[i] = True
#     return ordered_keywords

# def get_keywords_for_document(title, content):
#     """
#     Hàm tổng hợp: Nhận title và content, trả về danh sách keywords cuối cùng.
#     """
#     combined_text = f"{title}. {content}" if title else content
#     if not combined_text.strip() or combined_text.strip() == '.':
#         return []

#     chunks = TEXT_SPLITTER.split_text(combined_text)

#     if len(chunks) > 3:
#         logger.warning(f"  Bài viết có {len(chunks)} chunks. Chỉ xử lý chunk đầu tiên.")
#         chunks = chunks[:1] 
    
#     final_keywords_for_article = []
#     seen_keywords_for_article = set()
    
#     for chunk in chunks:
#         cleaned_chunk = clean_text(chunk)
#         if not cleaned_chunk:
#             continue
        
#         annotated_chunk = VNCORE_MODEL.annotate_text(cleaned_chunk)
#         keywords_from_chunk = extract_keywords_2(annotated_chunk)
        
#         for keyword in keywords_from_chunk:
#             lower_keyword = keyword.lower()
#             if lower_keyword not in seen_keywords_for_article:
#                 seen_keywords_for_article.add(lower_keyword)
#                 final_keywords_for_article.append(keyword)
    
#     return final_keywords_for_article

def upgrade_extract_keyword_record(es, target_index_alias):
    # """
    # Truy vấn 'posts' trong ngày, trích xuất từ khóa và đẩy sang index mới.
    # Sử dụng global 'es' client.
    # """
    # index_root = "posts"
    # batch_size = 500
    # bulk_batch_size = 1000

    # time_current = datetime.now()
    # start_of_day = time_current.replace(hour=0, minute=0, second=0, microsecond=0)
    # end_of_day = time_current
    
    # time_format = "MM/dd/yyyy HH:mm:ss"
    
    # fields_to_extract = [
    #     "id", "created_time", "time_crawl", "type", "field_classify", 
    #     "link", "author", "topic_id", "tenancy_ids", "title", "content"
    # ]

    # query_body = {
    #     "size": batch_size,
    #     "_source": fields_to_extract,
    #     "query": {
    #         "bool": {
    #             "filter": [
    #                 {"range": {"created_time": {"gte": start_of_day.strftime(time_format), "lte": end_of_day.strftime(time_format), "format": time_format}}},
    #                 {"term": {"type.keyword": "electronic media"}}
    #             ]
    #         }
    #     },
    #     "sort": [{"created_time": "asc"}, {"_id": "asc"}]
    # }

    # search_after_value = None
    # records_to_bulk = []
    # total_processed = 0
    
    # logger.info(f"Bắt đầu trích xuất từ index '{index_root}' (Loại: 'electronic media')")
    # logger.info(f"Phạm vi thời gian: {start_of_day} TỚI {end_of_day}")

    # try:
    #     while True:
    #         if search_after_value:
    #             query_body["search_after"] = search_after_value

    #         result = es.search(index=index_root, body=query_body, request_timeout=120)
    #         hits = result['hits']['hits']
    #         if not hits:
    #             logger.info("Không còn dữ liệu nào khớp với truy vấn.")
    #             break

    #         logger.info(f"  Đã lấy được {len(hits)} bản ghi. Bắt đầu xử lý trích xuất từ khóa...")

    #         for hit in tqdm(hits, desc="  Đang xử lý batch", leave=False):
    #             doc = hit['_source']
    #             keywords = get_keywords_for_document(doc.get('title', ''), doc.get('content', ''))
    #             keywords = [kw.lower() for kw in keywords]
    #             # keywords = [kw.lower() for kw in get_keywords_for_document(doc.get('title', ''), doc.get('content', ''))]

    #             new_doc = {}
    #             for field in fields_to_extract:
    #                 if field in doc:
    #                     new_doc[field] = doc[field]
                
    #             new_doc['key_word_extract'] = keywords
    #             records_to_bulk.append(new_doc)
    #             total_processed += 1

    #         if len(records_to_bulk) >= bulk_batch_size:
    #             logger.info(f"  Đang đẩy {len(records_to_bulk)} bản ghi lên '{target_index_alias}'...")
    #             bulk_data_to_elasticsearch_kw_a(es, records_to_bulk, target_index_alias)
    #             records_to_bulk = []

    #         search_after_value = hits[-1]['sort']
        
    #     if records_to_bulk:
    #         logger.info(f"  Đẩy {len(records_to_bulk)} bản ghi cuối cùng lên '{target_index_alias}'...")
    #         bulk_data_to_elasticsearch_kw_a(es, records_to_bulk, target_index_alias)

    #     logger.info(f"🎉 HOÀN TẤT! Đã xử lý và đẩy tổng cộng {total_processed} bản ghi.")

    # except Exception as e:
    #     logger.error(f"❌ Lỗi nghiêm trọng trong vòng lặp xử lý: {e}", exc_info=True)
    
    return
