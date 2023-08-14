# 国家突发事件预警信息发布网
import time
import feedparser
import hashlib
import json
from textrank4zh import TextRank4Keyword, TextRank4Sentence
from crawlab import save_item


def main():
    rss_url = 'http://192.168.238.128:1200/12379'

    feed = feedparser.parse(rss_url)

    # 遍历解析后的条目
    for entry in feed.entries:
        data = {}

        data['title'] = entry.title
        data['link'] = entry.link
        data['post_content'] = entry.description
        data['post_time'] = entry.published
        data['important_keywords'] = []

        tr4w = TextRank4Keyword()
        tr4w.analyze(text=entry.description, lower=True, window=2)

        keywords = {}
        for item in tr4w.get_keywords(20, word_min_len=2):
            keywords[item.word] = item.weight
        data['keywords'] = json.dumps(keywords, ensure_ascii=False)

        important_keywords = []
        for phrase in tr4w.get_keyphrases(keywords_num=5, min_occur_num=2):
            important_keywords.append(phrase)
        data['important_keywords'] = json.dumps(important_keywords, ensure_ascii=False)

        tr4s = TextRank4Sentence()
        tr4s.analyze(text=entry.description, lower=True, source = 'all_filters')

        temp_abstract = {}
        for item in tr4s.get_key_sentences(num=10):
            temp_abstract[item.sentence] = item.weight
        data['abstract'] = json.dumps(max(temp_abstract, key=lambda k: temp_abstract[k]), ensure_ascii=False)
        data['gather_time'] = time.time()
        data['deduplication_id'] = hashlib.md5((str(data['post_time'])+str(data['post_content'])+str(data['title'])).encode('utf-8')).hexdigest()
        

        print(f'添加: {data}')
        save_item(data)


main()
