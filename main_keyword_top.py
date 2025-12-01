import json
from collections import defaultdict
from datetime import datetime , timedelta
# from langdetect import detect 
import math
import numpy as np
import string
import re
# import matplotlib.pyplot as plt
from keyword_save_es import  load_data_to_elasticsearch_kw_a, bulk_data_to_elasticsearch_kw_a
from elasticsearch import Elasticsearch
from time import sleep
import os
from dotenv import load_dotenv
# from main_query_es import  query_keyword_with_trend, upgrade_extract_keyword_record
from main_query_es import  upgrade_extract_keyword_record, load_stopwords

from collections import defaultdict

from get_topic_kw import query_topic_id_grouped_by_tenant

# Định nghĩa đường dẫn base directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def is_not_blackword(word):
    
    blacklist_path = os.path.join(BASE_DIR, 'black_list.txt')
    with open(blacklist_path, 'r', encoding='utf-8') as f:
        black_words = f.read().splitlines()

    if word in black_words:
        return False
    
    if 'ảnh' in word :
        return False
    
    return True

def is_keyword_selected(keyword, historical_percentages, daily_keywords, check_date_str):
    percentage_on_check_date = next((item['percentage'] for item in daily_keywords if item['keyword'] == keyword), 0)    
    
    # other_dates_percentages = [
    #     historical_percentages.get(i, 0)  # Ensure a default value of 0 if the topic key is missing
    #     for i in range(6)  # Chỉ lấy 6 ngày trước đó, bỏ qua ngày hiện tại
    # ]
    for key, value in historical_percentages.items():
        if isinstance(value, list):
            other_dates_percentages = value[:6]  # Chỉ lấy 6 ngày trước đó, bỏ qua ngày hiện tại
            break
    
    if not other_dates_percentages:
        other_dates_percentages = [0] * 6

    count_higher_09 = sum(perc >= 0.88 for perc in other_dates_percentages)
    count_higher_11 = sum(perc >= 1.1 for perc in other_dates_percentages)
    count_higher_14 = sum(perc >= 1.4 for perc in other_dates_percentages)

    if count_higher_09 >= 3 and count_higher_11 >= 2:
        min_other_percentage = min(other_dates_percentages) if other_dates_percentages else 0
        if percentage_on_check_date > 3 +  min_other_percentage and count_higher_14 <= 5:
            return True
        else:
            return False
    else:
        return True

# def is_subkeyword(keyword, other_keyword):
#     """
#     Check if any word in 'keyword' is present in 'other_keyword' after splitting both by underscores.
#     """
#     # Splitting the keywords into lists of words
#     keyword_words = keyword.lower().split('_')
#     other_keyword_words = other_keyword.lower().split('_')

#     # Checking if any word in keyword is present in other_keyword
#     return all(word in other_keyword_words for word in keyword_words) or all(word in keyword_words for word in other_keyword_words)
def is_subkeyword(keyword, other_keyword):
    """
    Check if any word in 'keyword' is present in 'other_keyword' after splitting both by underscores.
    Additionally, return true if there are at least four common words between the two keywords.
    """
    # Splitting the keywords into lists of words
    keyword_words = set(keyword.lower().split('_'))
    other_keyword_words = set(other_keyword.lower().split('_'))

    # Checking if any word in keyword is present in other_keyword
    basic_check = all(word in other_keyword_words for word in keyword_words) or all(word in keyword_words for word in other_keyword_words)

    # Checking for at least four common words
    common_words = keyword_words.intersection(other_keyword_words)
    four_common_words_check = len(common_words) >= 4

    return basic_check or four_common_words_check


def filter_keywords_all_words_no_sort(keyword_list):
    """
    Filter the keywords based on the all-words subkeyword relation without sorting.
    Each keyword in the list is a tuple of (keyword, percentage).
    """
    filtered_keywords = []
    
    for keyword, percentage in keyword_list:
        if(keyword =='thanh_hoá'):
            print(1)
        # Check if the current keyword is a subkeyword of any keyword in the filtered list
        if not any(is_subkeyword(keyword, existing_keyword) for existing_keyword, _ in filtered_keywords):
            filtered_keywords.append((keyword, percentage))

    return filtered_keywords



# def calculate_top_keywords_with_topic_2_es(es, input_date, data, index_name, platform):
#     try:
#         with open('blacklist_hashtag.txt', 'r', encoding='utf-8') as f:
#             blacklist = set(line.strip() for line in f if line.strip())
#     except Exception as e:
#         print(f"Error reading blacklist file: {e}")
#         blacklist = set()

#     # Định dạng ngày
#     date_format = "%m/%d/%Y"
#     # Chuyển input_date thành datetime object
#     date_obj = datetime.strptime(input_date, date_format)
#     date_str = date_obj.strftime("%m_%d_%Y")

#     # Khởi tạo các biến để lưu trữ số liệu thống kê
#     date_counts = defaultdict(int)
#     topic_article_counts = defaultdict(int)
#     keyword_counts = defaultdict(lambda: defaultdict(int))
#     hashtag_counts = defaultdict(lambda: defaultdict(int))
#     topic_ids_set = set()
    
#     # Duyệt qua từng item trong dữ liệu
#     for item in data:
#         keywords_field = 'keyword' if item['_index'] == 'posts' else 'keywords'
#         hashtags_field = 'hashtag' if item['_index'] == 'posts' else 'hashtags'
        
#         # keywords_field = 'keyword'
#         # hashtags_field = 'hashtag'

#         # Lấy ngày tạo từ _source và chuyển đổi thành chuẩn định dạng ngày
#         item_date_str = datetime.strptime(item['_source']['created_time'], '%m/%d/%Y %H:%M:%S').strftime(date_format)
        
#         # Nếu ngày tạo trùng khớp với input_date, tiến hành thống kê
#         if item_date_str == input_date:
#             date_counts[item_date_str] += 1
#             for topic_id in item['_source'].get('topic_id', []):
#                 topic_article_counts[topic_id] += 1
#                 topic_ids_set.add(topic_id)
#             topic_article_counts["all"] += 1  # Tăng tổng số bài viết cho "all"

#             for keyword in item['_source'].get(keywords_field, []):
#                 if len(keyword) > 2: 
#                     # if keyword == "vạn_thịnh_phát":
#                     #     pass
#                     for topic_id in item['_source'].get('topic_id', []):
#                         keyword_counts[topic_id][keyword] += 1
#                     keyword_counts["all"][keyword] += 1
#                     # if keyword == "vạn_thịnh_phát":
#                     #     print(keyword_counts["all"][keyword])
#             # print(keyword_counts["all"]["vạn_thịnh_phát"])

#             for hashtag in item['_source'].get(hashtags_field, []):
#                 if len(hashtag) > 2 and hashtag not in blacklist:  # Lọc các hashtag có độ dài lớn hơn 2
#                     for topic_id in item['_source'].get('topic_id', []):
#                         hashtag_counts[topic_id][hashtag] += 1
#                     hashtag_counts["all"][hashtag] += 1
#     # if (len(keyword_counts)>0):        
#     #     print(keyword_counts["all"]["vạn_thịnh_phát"])

#     # Tính toán tỉ lệ phần trăm của các từ khóa
#     topic_data = {}
#     topic_ids = list(topic_ids_set) + ["all"]

#     for topic_id in topic_ids:
#         # if topic_id == "all":
#         #     pass
#         total_articles = topic_article_counts[topic_id]
#         # keyword_percentages = []
#         # for keyword, count in keyword_counts[topic_id].items():
#         #     if keyword == "vạn_thịnh_phát":
#         #         pass
#         #     keyword_percentages.append({"keyword": keyword, "percentage": (count / total_articles) * 100, "record": count})
#         keyword_percentages = [
#             {"keyword": keyword, "percentage": (count / total_articles) * 100, "record": count}
#             for keyword, count in keyword_counts[topic_id].items()
#         ]
#         keyword_percentages = sorted(keyword_percentages, key=lambda x: x['percentage'], reverse=True)[:600]

#         hashtag_percentages = [
#             {"hashtag": hashtag, "percentage": (count / total_articles) * 100, "record": count}
#             for hashtag, count in hashtag_counts[topic_id].items()
#         ]
#         hashtag_percentages = sorted(hashtag_percentages, key=lambda x: x['percentage'], reverse=True)[:600]

#         if keyword_percentages or hashtag_percentages or topic_id == "all":
#             topic_data[topic_id] = {
#                 "keywords_top": keyword_percentages,
#                 "hashtags_top": hashtag_percentages
#             }
#     # Lưu kết quả vào Elasticsearch
#     if topic_data.get("all", {}).get("keywords_top") or topic_data.get("all", {}).get("hashtags_top"):
#         # Lưu kết quả vào Elasticsearch
#         data = {
#             "date": date_str,
#             "type": platform,
#             "topic_ids": topic_data
#         }
#         sleep(1.5)
#         load_data_to_elasticsearch_kw_a(es, data, index_name)

#     # Trả về kết quả
#     return {
#         "date": date_str,
#         "type": platform,
#         "topic_ids": topic_data
#     }
    
def calculate_top_keywords_with_topic_2_es(es, input_date, data, index_name, platform):

    try:

        list_topic =  query_topic_id_grouped_by_tenant()
        
        #for item in list_topic:
        #    tenant = item["tenant"]
        #    for topic in item["topic_id"]:
        #        topic_to_tenants.setdefault(topic, set()).add(tenant)

        # Lấy các topic có mặt ở >= 2 tenant
        #duplicates = {topic: tenants for topic, tenants in topic_to_tenants.items() if len(tenants) > 1}

        #print(duplicates)
    except:
        print(f"error get topic from Mongodb")    
        return 

    try:
        blacklist_path = os.path.join(BASE_DIR, 'vncorenlp', 'blacklist_hashtag.txt')
        with open(blacklist_path, 'r', encoding='utf-8') as f:
            blacklist = set(line.strip() for line in f if line.strip())
    except Exception as e:
        print(f"Error reading blacklist file: {e}")
        blacklist = set()





    date_format = "%m/%d/%Y"
    date_obj = datetime.strptime(input_date, date_format)
    date_str = date_obj.strftime("%m_%d_%Y")

    keyword_counts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    hashtag_counts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    topic_article_counts = defaultdict(lambda: defaultdict(int))

    test_topic = []
    for item in data:
        keywords_field = 'keyword' if item['_index'] == 'posts' else 'keywords'
        hashtags_field = 'hashtag' if item['_index'] == 'posts' else 'hashtags'

        item_date_str = datetime.strptime(item['_source']['created_time'], '%m/%d/%Y %H:%M:%S').strftime(date_format)
        if item_date_str != input_date:
            continue

        tenancy_ids = item['_source'].get('tenancy_ids', [])
        topic_ids = item['_source'].get('topic_id', [])
        #if not tenancy_ids or not topic_ids:
        #    continue
        test_topic.extend(topic_ids)
        for tenancy_id in tenancy_ids:
            topic_article_counts[tenancy_id]["all"] += 1
            result_topic = []
            for itemc in list_topic:
                if itemc["tenant"] == tenancy_id:
                    result_topic.extend(itemc["topic_id"])
                    break

            for topic_id in topic_ids:
                if topic_id in result_topic:
                    topic_article_counts[tenancy_id][topic_id] += 1

            for keyword in item['_source'].get(keywords_field, []):
                if len(keyword) > 2:
                    keyword_counts[tenancy_id]["all"][keyword] += 1
                    for topic_id in topic_ids:
                        if topic_id in result_topic:
                            keyword_counts[tenancy_id][topic_id][keyword] += 1

            for hashtag in item['_source'].get(hashtags_field, []):
                if len(hashtag) > 2 and hashtag not in blacklist:
                    hashtag_counts[tenancy_id]["all"][hashtag] += 1
                    for topic_id in topic_ids:
                        if topic_id in result_topic:
                            hashtag_counts[tenancy_id][topic_id][hashtag] += 1

    print(f"Total topics found: {len(set(test_topic))}")
    arr_key_hash = []

    for tenancy_id in topic_article_counts:
        for topic_id in topic_article_counts[tenancy_id]:
            #ndc
            #total_articles = topic_article_counts[tenancy_id][topic_id]
            total_articles = topic_article_counts[tenancy_id]["all"]
            keyword_percentages = [
                {"keyword": keyword, "percentage": (count / total_articles) * 100, "record": count}
                for keyword, count in keyword_counts[tenancy_id][topic_id].items()
            ]
            keyword_percentages = sorted(keyword_percentages, key=lambda x: x['percentage'], reverse=True)[:100]

            hashtag_percentages = [
                {"hashtag": hashtag, "percentage": (count / total_articles) * 100, "record": count}
                for hashtag, count in hashtag_counts[tenancy_id][topic_id].items()
            ]
            hashtag_percentages = sorted(hashtag_percentages, key=lambda x: x['percentage'], reverse=True)[:100]
            if keyword_percentages:
                document_id = f"key_{date_str}_{platform}_{tenancy_id}_{topic_id}"
                result_key = {
                    "id": document_id,
                "date": date_str,
                "type": platform,
                "tenant_id": tenancy_id,
                "topic_id":topic_id,
                "keywords_top": keyword_percentages
            }
                arr_key_hash.append(result_key)
            if hashtag_percentages:
                document_id = f"hashtag_{date_str}_{platform}_{tenancy_id}_{topic_id}"
                result_hashtag = {
                    "id": document_id,
                "date": date_str,
                "type": platform,
                "tenant_id": tenancy_id,
                "topic_id":topic_id,
                "hashtags_top": hashtag_percentages
            }
                arr_key_hash.append(result_hashtag)
    if arr_key_hash :
        try:
            bulk_data_to_elasticsearch_kw_a(es, arr_key_hash, index_name)
        except Exception as e:
            print(f"Error uploading to Elasticsearch: {e}")

    return 

# def calculate_top_keywords_with_trend_logic_topic(input_date, es, historical_data_index, platform):
#     input_datetime = datetime.strptime(input_date, "%m/%d/%Y")
#     input_date_str = input_datetime.strftime("%m_%d_%Y")
#     start_date_str = (input_datetime - timedelta(days=6)).strftime("%m_%d_%Y")
#     end_date_str = input_date_str
#     sleep(1.5)

#     # Truy vấn dữ liệu 7 ngày gần nhất
#     '''
#     daily_keywords = es.search(index=historical_data_index, body={
#         "query": {
#             "bool": {
#                 "filter": [
#                     {"term": {"type": platform}},
#                     {
#                         "range": {
#                             "date": {
#                                 "gte": start_date_str,
#                                 "lte": end_date_str,
#                                 "format": "MM_dd_yyyy"
#                             }
#                         }
#                     }
#                 ]
#             }
#         },
#         "_source": ["date",  "tenant_id","topic_id","keywords_top"],
#         "size": 100,
#         "timeout": "60s"
#     })['hits']['hits']
#     '''

#     body={
#         "query": {
#             "bool": {
#                 "filter": [
#                     {"term": {"type": platform}},
#                     {
#                         "range": {
#                             "date": {
#                                 "gte": start_date_str,
#                                 "lte": end_date_str,
#                                 "format": "MM_dd_yyyy"
#                             }
#                         }
#                     }
#             ]
#             }
#         },
#         "sort": [{"date": {"order": "asc"}}, {"topic_id": {"order": "asc"}}],
#         "_source": ["date",  "tenant_id","topic_id","keywords_top"],
#         "size":4000
        
#     }
#     #current_day_keywords = [hit['_source'] for hit in daily_keywords if hit['_source']['date'] == input_date_str]
#     #daily_keywords = [hit['_source'] for hit in daily_keywords]
#     daily_keywords = query_keyword_with_trend(es,historical_data_index,body)
#     # Nhóm theo date
#     grouped = defaultdict(list)
#     for keyword_day in daily_keywords:
#         if "keywords_top" not in keyword_day:
#             continue
#         grouped[keyword_day["date"]].append(keyword_day)
#     daily_keywords = [grouped[date] for date in sorted(grouped.keys())]
    
#     for keyword_day in daily_keywords:
#         if keyword_day[0]["date"] == input_date_str:
#             current_day_keywords   = keyword_day
#             break


#     # Truy vấn dữ liệu 20 ngày cho check_big2
#     start_date_str_big = (input_datetime - timedelta(days=9)).strftime("%m_%d_%Y")

#     body={
#     "query": {
#         "bool": {
#             "filter": [
#                 {"term": {"type": platform}},
#                 {
#                     "range": {
#                         "date": {
#                             "gte": start_date_str_big,
#                             "lte": end_date_str,
#                             "format": "MM_dd_yyyy"
#                         }
#                     }
#                 }
#             ]
#         }
#     },
#     "sort": [{"date": {"order": "asc"}}, {"topic_id": {"order": "asc"}}],
#     "_source": ["date",  "tenant_id","topic_id","keywords_top"],
#     "size":4000
    
# }
#     #ndc
#     results = query_keyword_with_trend(es,historical_data_index,body)
#     results2 = []

#     for keyword_day in results:
#         if "keywords_top" not in keyword_day:
#             continue
#         results2.append(keyword_day)
#     #results= []


#     results_by_tenancy = {}
#     arr_key_trend = []
#     if len(daily_keywords) == 7:
#         historical_percentages = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: [0]*7)))
#         for day_keyword in daily_keywords:
#             for keywords in day_keyword:
#                 tenant_id_trend = keywords['tenant_id']
#                 topic_id_trend = keywords['topic_id']
#                 for keyword_info in keywords.get('keywords_top', [])[:100]:
#                     keyword = keyword_info['keyword'] 
#                     if keyword not in historical_percentages[tenant_id_trend][topic_id_trend]:
#                         historical_percentages[tenant_id_trend][topic_id_trend][keyword] = [0] * 7
#                     index = (input_datetime - datetime.strptime(day_keyword[0]['date'], "%m_%d_%Y")).days
#                     historical_percentages[tenant_id_trend][topic_id_trend][keyword][6 - index] = keyword_info['percentage']
                

#         for keywords in current_day_keywords:
#             keywordtop_for_check = [
#                 {
#                     "keyword": keyword_info['keyword'],
#                     "percentage": historical_percentages[keywords['tenant_id']][keywords['topic_id']][keyword_info['keyword']],
#                     "record": keyword_info["record"],
#                     "score": keyword_info.get("score", 0),
#                     # "isTrend": keyword_info.get("isTrend", False)
#                 }
#                 for keyword_info in keywords.get('keywords_top', [])[:100]
#             ]

#             sorted_keywords = Check(keywordtop_for_check)
#             top_keywords, top_keywords_big = [], []
#             un_top_keywords, un_top_keywords_2, black_keywords = [], [], []
#             top_10_current_day_keywords = [kw_info['keyword'] for kw_info in keywords.get('keywords_top', [])[:4]]

#             for kw_dict in sorted_keywords:
#                 if is_not_blackword(kw_dict['keyword']):
#                     topic_specific_percentages = historical_percentages[keywords['tenant_id']][keywords['topic_id']][kw_dict['keyword']]
#                     if check_big2(results2, kw_dict['keyword'], keywords['topic_id']):
#                         top_keywords_big.append(kw_dict)
#                         kw_dict['isTrend'] = True
#                     elif is_keyword_selected(kw_dict['keyword'], {keywords['topic_id']: topic_specific_percentages}, sorted_keywords, input_date_str):
#                         kw_dict['isTrend'] = True
#                         if '_' in kw_dict['keyword'] or kw_dict['keyword'] in top_10_current_day_keywords:
#                             top_keywords.append(kw_dict)
#                         else:
#                             un_top_keywords.append(kw_dict)
#                     else:
#                         un_top_keywords_2.append(kw_dict)
#                         kw_dict['isTrend'] = False
#                 else:
#                     black_keywords.append(kw_dict)
#                     kw_dict['isTrend'] = False

#             final_keywords = (
#                     top_keywords[:8] + top_keywords_big + top_keywords[8:400] +
#                     un_top_keywords + un_top_keywords_2 + top_keywords[400:] + black_keywords
#                     if len(top_keywords) > 400
#                     else top_keywords[:8] + top_keywords_big + top_keywords[8:] +
#                          un_top_keywords + un_top_keywords_2 + black_keywords
#                 )
#             document_id = f"trend_{keywords['date']}_{platform}_{keywords['tenant_id']}_{keywords['topic_id']}"
#             result_key = {
#                 "id": document_id,
#                 "date": keywords['date'],
#                 "type": platform,
#                 "tenant_id": keywords['tenant_id'],
#                 "topic_id":keywords['topic_id'],
#                 "keywords_trend": final_keywords
#             }
#             arr_key_trend.append(result_key)


#     else:
#         last_day_data = daily_keywords[-1] if daily_keywords else {}
#         for keywords_data in  last_day_data:
#             default_keywords = [
#                 {
#                     "keyword": kw_info['keyword'],
#                     "percentage": kw_info['percentage'],
#                     "record": kw_info['record'],
#                     "score": 0,
#                     "isTrend": False
#                 }
#                 for kw_info in keywords_data.get('keywords_top', [])[:100]
#             ]

#             document_id = f"trend_{keywords_data['date']}_{platform}_{keywords_data['tenant_id']}_{keywords_data['topic_id']}"
#             result_key = {
#                 "id": document_id,
#                 "date": keywords_data['date'],
#                 "type": platform,
#                 "tenant_id": keywords_data['tenant_id'],
#                 "topic_id":keywords_data['topic_id'],
#                 "keywords_trend": default_keywords
#             }
#             arr_key_trend.append(result_key)




#     if arr_key_trend :
#         try:
#             bulk_data_to_elasticsearch_kw_a(es, arr_key_trend, "key-trend-current")
#         except Exception as e:
#             print(f"Error uploading to Elasticsearch: {e}")
#         print("update trend thanh cong tu khoa xu huong ", f"{input_datetime.strftime('%m_%d_%Y')}_{platform}")
#     return 

    
def calculate_top_keywords_with_trend_logic_topic(es_db, current_day, initial_index, index_name_trend_v2, collection_type):
    
    now = datetime.now()
    if isinstance(current_day, str):
        # Chạy cho ngày cũ, xét cả ngày
        current_day_dt = datetime.strptime(current_day, "%m/%d/%Y")
        trending_start_time = current_day_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        trending_end_time = current_day_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
        baseline_end_time = trending_start_time
        baseline_start_time = baseline_end_time - timedelta(days=14)
    else: # is a datetime object
        # Chạy cho ngày hiện tại, xét 24h gần nhất
        trending_end_time = current_day.replace(minute=0, second=0, microsecond=0)
        trending_start_time = trending_end_time - timedelta(hours=24)
        baseline_end_time = trending_start_time
        baseline_start_time = baseline_end_time - timedelta(days=14)

    try:
        list_topic = query_topic_id_grouped_by_tenant()
    except Exception as e:
        print(f"Error getting topics from MongoDB: {e}")
        return

    all_docs_to_index = []

    for tenancy in list_topic:
        tenant_id = tenancy["tenant"]
        topic_ids_for_tenant = tenancy["topic_id"]
        
        # Add "all" to calculate trend for the whole tenancy
        topics_to_process = topic_ids_for_tenant + ["all"]

        for topic_id in topics_to_process:
            # 1. Build the main query
            bool_filter = [
                {"term": {"type": collection_type}},
                {"range": {"created_time": {
                    "gte": baseline_start_time.strftime("%m/%d/%Y %H:%M:%S"),
                    "lte": trending_end_time.strftime("%m/%d/%Y %H:%M:%S"),
                    "format": "MM/dd/yyyy HH:mm:ss"
                }}}
            ]
            # Add tenancy and topic filters
            bool_filter.append({"term": {"tenancy_ids.keyword": tenant_id}})
            if topic_id != "all":
                bool_filter.append({"term": {"topic_id.keyword": topic_id}})

            query = {
                "size": 0,
                "query": {"bool": {"filter": bool_filter}},
                "aggs": {
                    "keywords_agg": {
                        "terms": {"field": "key_word_extract", "size": 4000, "shard_size": 10000},
                        "aggs": {
                            "trending_period": {
                                "filter": {
                                    "range": {"created_time": {
                                        "gte": trending_start_time.strftime("%m/%d/%Y %H:%M:%S"),
                                        "lte": trending_end_time.strftime("%m/%d/%Y %H:%M:%S"),
                                        "format": "MM/dd/yyyy HH:mm:ss"
                                    }}
                                }
                            },
                            "baseline_period": {
                                "filter": {
                                     "range": {"created_time": {
                                        "gte": baseline_start_time.strftime("%m/%d/%Y %H:%M:%S"),
                                        "lt": baseline_end_time.strftime("%m/%d/%Y %H:%M:%S"),
                                        "format": "MM/dd/yyyy HH:mm:ss"
                                    }}
                                },
                                "aggs": {
                                    "daily_counts": {
                                        "date_histogram": {
                                            "field": "created_time",
                                            "fixed_interval": "1d",
                                            "min_doc_count": 0,
                                            "extended_bounds": {
                                                "min": baseline_start_time.strftime("%m/%d/%Y %H:%M:%S"),
                                                "max": (baseline_end_time - timedelta(seconds=1)).strftime("%m/%d/%Y %H:%M:%S")
                                            },
                                            "format": "MM/dd/yyyy HH:mm:ss"
                                        }
                                    }
                                }
                            },
                            "baseline_stats": {
                                "extended_stats_bucket": {
                                    "buckets_path": "baseline_period>daily_counts._count",
                                    "sigma": 1 
                                }
                            }
                        }
                    },
                    "total_articles_trending": {
                        "filter": {
                            "range": {"created_time": {
                                "gte": trending_start_time.strftime("%m/%d/%Y %H:%M:%S"),
                                "lte": trending_end_time.strftime("%m/%d/%Y %H:%M:%S"),
                                "format": "MM/dd/yyyy HH:mm:ss"
                            }}
                        },
                        "aggs": {
                            "unique_articles": {"cardinality": {"field": "id.keyword"}}
                        }
                    }
                }
            }

            try:
                response = es_db.search(index=initial_index, body=query, request_timeout=120)
            except Exception as e:
                print(f"Error querying Elasticsearch for tenant {tenant_id}, topic {topic_id}: {e}")
                continue

            keyword_buckets = response.get('aggregations', {}).get('keywords_agg', {}).get('buckets', [])
            total_articles = response.get('aggregations', {}).get('total_articles_trending', {}).get('unique_articles', {}).get('value', 0)

            if not keyword_buckets or total_articles == 0:
                continue

            keywords_trend_data = []
            for bucket in keyword_buckets:
                keyword = bucket.get('key')
                if not keyword or len(keyword) <= 2:
                    continue

                record = bucket.get('trending_period', {}).get('doc_count', 0)
                if record == 0:
                    continue

                stats = bucket.get('baseline_stats', {})
                avg_baseline = stats.get('avg', 0)
                std_dev_baseline = stats.get('std_deviation', 0)

                if std_dev_baseline is None or not math.isfinite(std_dev_baseline):
                    std_dev_baseline = 0
                
                # Z-score calculation
                if std_dev_baseline == 0:
                    score = 10 + record if avg_baseline == 0 and record > 0 else 0
                else:
                    score = (record - avg_baseline) / std_dev_baseline

                percentage = (record / total_articles) * 100 if total_articles > 0 else 0

                keywords_trend_data.append({
                    "keyword": keyword,
                    "percentage": percentage,
                    "record": record,
                    "score": score,
                    "avg_paper_count_baseline": avg_baseline,
                    "stddev_paper_count_baseline": std_dev_baseline
                })

            if not keywords_trend_data:
                continue

            # Sort by score and take top 100 keywords
            keywords_trend_data.sort(key=lambda x: x['score'], reverse=True)
            keywords_trend_data = keywords_trend_data[:100]
            
            # Set isTrend for top 50 only
            for i, item in enumerate(keywords_trend_data):
                item['isTrend'] = i < 50

            # Prepare document for indexing
            date_str = trending_end_time.strftime("%m_%d_%Y")
            doc_id = f"trend_{date_str}_{collection_type}_{tenant_id}_{topic_id}"
            
            document = {
                "id": doc_id,
                "date": trending_end_time.strftime("%m/%d/%Y"),
                "type": collection_type,
                "tenant_id": tenant_id,
                "topic_id": topic_id,
                "keywords_trend": keywords_trend_data
            }
            all_docs_to_index.append(document)

    if all_docs_to_index:
        try:
            print(f"Indexing {len(all_docs_to_index)} trend documents for {collection_type}...")
            bulk_data_to_elasticsearch_kw_a(es_db, all_docs_to_index, index_name_trend_v2)
            print("Successfully indexed trend data.")
        except Exception as e:
            print(f"Error bulk indexing trend data: {e}")

    return