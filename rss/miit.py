# 中国工业和信息化部信息收集

import feedparser
import hashlib
import json
import argparse
from datetime import datetime
from textrank4zh import TextRank4Keyword, TextRank4Sentence
from crawlab import save_item
from kafka import KafkaProducer
from bs4 import BeautifulSoup

if __name__ == '__main__':
    #region 参数解析
    parser = argparse.ArgumentParser(description='中国工业和信息化部信息收集')

    subparsers = parser.add_subparsers(dest='subcommand', help='类型')

    # 政策解读
    zcjd_parser = subparsers.add_parser('zcjd', help='政策解读')

    # 文件发布
    wjfb_parser = subparsers.add_parser('wjfb', help='文件发布')
    wjfb_parser.add_argument('--ministry', type=str, default='ghs', required=True, help='必选-部门缩写，可以在对应 URL 中获取')

    # 意见征集
    yjzj_parser = subparsers.add_parser('yjzj', help='意见征集')

    # 文件公示
    wjgs_parser = subparsers.add_parser('wjgs', help='文件公示')

    # 政策文件
    zcwj_parser = subparsers.add_parser('zcwj', help='政策文件')

    args = parser.parse_args()
    #endregion

    # 默认政策解读
    
    if args.subcommand == 'wjfb':
        rss_url = f'http://192.168.238.128:1200/gov/miit/wjfb/{args.ministry}/'
    elif args.subcommand == 'yjzj':
        rss_url = f'http://192.168.238.128:1200/gov/miit/yjzj/'
    elif args.subcommand == 'wjgs':
        rss_url = f'http://192.168.238.128:1200/gov/miit/wjgs/'
    elif args.subcommand == 'zcwj':
        rss_url = f'http://192.168.238.128:1200/gov/miit/zcwj/'
    else:
        rss_url = f'http://192.168.238.128:1200/gov/miit/zcjd/'
    feed = feedparser.parse(rss_url)
    
    #region Kafka配置
    bootstrap_servers = 'kafka-server:9092'
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    topic = 'rss_handle'
    #endregion

    # 遍历解析后的条目
    for entry in feed.entries:
        data = {}

        data['title'] = entry.title
        data['link'] = entry.link
        data['post_content'] = entry.description
        if not entry.published == 'Invalid Date':
            data['post_time'] = datetime.strptime(entry.published, '%a, %d %b %Y %H:%M:%S %Z').strftime('%Y-%m-%d %H:%M:%S')
        else:
            data['post_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data['important_keywords'] = []

        tr4w = TextRank4Keyword()
        tr4w.analyze(text=BeautifulSoup(entry.description, 'html.parser').get_text(), lower=True, window=2)

        keywords = {}
        for item in tr4w.get_keywords(20, word_min_len=2):
            keywords[item.word] = item.weight
        data['keywords'] = json.dumps(keywords, ensure_ascii=False)

        important_keywords = []
        for phrase in tr4w.get_keyphrases(keywords_num=5, min_occur_num=2):
            important_keywords.append(phrase)
        data['important_keywords'] = important_keywords

        tr4s = TextRank4Sentence()
        tr4s.analyze(text=BeautifulSoup(entry.description, 'html.parser').get_text(), lower=True, source = 'all_filters')

        temp_abstract = {}
        for item in tr4s.get_key_sentences(num=10):
            temp_abstract[item.sentence] = item.weight
        if temp_abstract:
            data['abstract'] = json.dumps(max(temp_abstract, key=lambda k: temp_abstract[k]), ensure_ascii=False)
        data['gather_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data['deduplication_id'] = hashlib.md5((str(data['post_time'])+str(data['post_content'])+str(data['title'])).encode('utf-8')).hexdigest()
        
        # other meta
        data['table_type'] = 'rss'
        data['platform'] = '中国工业和信息化部'
        data['rss_type'] = '政务消息'
        data['meta'] = ['官方信息']

        #region 推送kafka
        push_kafka_success = False
        try:
            future = producer.send(topic, json.dumps(data).encode('utf-8'))
            # future.get(timeout=100)
            producer.flush()
            push_kafka_success = True
        except Exception as e:
            print('kafka推送报错', str(e))
            push_kafka_success = False
        #endregion

        data['push_kafka_success'] = push_kafka_success
        print(f'添加: {data}')
        save_item(data)
    producer.close()
