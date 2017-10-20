from elasticsearch import Elasticsearch, helpers, exceptions
from datetime import *
from tqdm import tqdm
import gzip
import glob
import json
import logging
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"

class TweetsIndex():

    def __init__(self, host, port, index_name):
        self.es = Elasticsearch([("%s:%s" % (host,port))])
        self.index = index_name
        settings = {
            "mappings": {
                "tweets": {
                    "properties": {
                        "in_reply_to_screen_name": {
                            "type": "keyword",
                        },
                        "author_quoted": {
                            "type": "keyword"
                        },
                        "author_retweeted": {
                            "type": "keyword"
                        },
                        "tags": {
                            "type": "keyword"
                        },
                        "tweet_retweeted": {
                            "type": "keyword"
                        },
                        "publicationDate": {
                            "type": "date",
                            "format": "date_hour_minute_second"
                        },
                        "collection_date": {
                            "type": "date",
                            "format": "date_hour_minute_second"
                        },
                        "created_at": {
                            "type": "date",
                            "format": "EEE MMM dd HH:mm:ss +0000 yyyy" # Tue May 31 16:08:45 +0000 2016
                        }
                    }
                },
                "events": {
                    "properties": {
                        "publicationDate": {
                            "type": "date",
                            "format": "date_hour_minute_second"
                        }
                    }
                }
            }
        }

        exists = self.es.indices.exists(self.index)
        if not exists:
            self.es.indices.create(index=self.index, body=settings)

    def load_last_tweet(self, query):
        query = {
            "sort" : { "created_at" : {"order" : "desc"}},
            "query": {
                "match": {
                    "tags": query
                }

            }
        }
        return self.es.search(index=self.index, doc_type = "tweets", body=query, size=1)["hits"]["hits"]

    def load_previous_tweets(self, query):
        query = {
            "query": {
                "match": {
                    "tags": query
                }

            }
        }
        return self.helpers.scan(self.es, index=self.index, doc_type = "tweets", query=query)

    def format_tweets(self, tweets, query, event):
        for tweet in tweets:
            if "id" in tweet:
                if tweet["entities"]["hashtags"] != [] :
                    hashtags = tweet["entities"]["hashtags"]
                    tweet["hashtags_list"] = sorted([hashtag["text"].lower() for hashtag in hashtags])
                    tweet["hashtags"] = " ".join(tweet["hashtags_list"])
                tweet["collection_date"] = datetime.now().strftime(DATE_FORMAT)
                tweet["publicationDate"] = datetime \
                    .strptime(tweet["created_at"],"%a %b %d %H:%M:%S +0000 %Y") \
                    .strftime(DATE_FORMAT)
                if "retweeted_status" in tweet:
                    tweet["author_retweeted"] = tweet["retweeted_status"]["user"]["screen_name"]
                    tweet["is_retweet"] = True
                    tweet["tweet_retweeted"] = tweet["retweeted_status"]["id_str"]
                    tweet.pop("retweeted_status")
                else:
                    tweet["is_retweet"] = False
                    tweet["tweet_retweeted"] = tweet["id_str"]
                if "quoted_status" in tweet:
                    tweet["author_quoted"] = tweet["quoted_status"]["user"]["screen_name"]
                    tweet.pop("user")
                tweet["expanded_urls"] = []
                for url in tweet["entities"]["urls"]:
                     tweet["expanded_urls"].append(url["expanded_url"])
                tweet["tags"] = [query]
                tweet["events"] = [event]
                yield tweet
                

    def storeTweetsWithTag(self, tweets, query, event=""):
        tweets_not_created = []

        to_update = (
        {
        '_op_type': 'update',
        '_type':'tweets',
        '_index':self.index,
        '_id': tweet["id"],

        'script': {
            'lang': "painless",
            "inline" : "ctx._source.tags.contains(params.query) ? (ctx.op = \"none\") : ctx._source.tags.add(params.query)",
            "params": {
                "query": query,
                "event": event
            }
        },
        'upsert': tweet
        }
              for tweet in self.format_tweets(tweets, query, event) if "entities" in tweet)

        errors = []
        for res, item in helpers.parallel_bulk(self.es,to_update,chunk_size=chunk_size, thread_count=thread_count, raise_on_error=False):
            if not res:
                errors.append(item)
        return errors


if __name__ == "__main__":
    path_to_files = "/home/bmazoyer/Dev/Twitter_OTM/peak_detection/data_vegas/*"
    host = "localhost"
    port = 9200
    index_name = "test"
    index = TweetsIndex(host, port, index_name)
    thread_count = 5
    chunk_size = 200
    logging.basicConfig(format='%(asctime)s - %(levelname)s : %(message)s', level=logging.ERROR)

    for file_name in tqdm(glob.glob(path_to_files)):
        with gzip.open(file_name, mode='rt', encoding="utf-8") as f:
            try:
                tweets = (json.loads(line)[2] for line in f.readlines())
            except Exception as e:
                logging.error(str(e) + " " + file_name)
                continue
            res = index.storeTweetsWithTag(tweets, query=file_name[len(path_to_files):])
            if res != []:
                print(res)