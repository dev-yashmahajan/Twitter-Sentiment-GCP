import tweepy
from tweepy.streaming import Stream

from google.cloud import pubsub_v1

import datetime
import json
import time


# Config
PROJECT_ID = "vibrant-map-337323"
TOPIC_ID = "tweets"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# Twitter API Keys
CONSUMER_KEY = 'XXXXXXXXXXXXXXXXXX'
CONSUMER_SECRET = 'XXXXXXXXXXXXXXXXXX'
ACCESS_TOKEN = 'XXXXXXXXXXXXXXXXXX'
ACCESS_TOKEN_SECRET = 'XXXXXXXXXXXXXXXXXX'


# Define the list of terms to listen to
hashtags_list = ["#SpiderMan"]


# Method to format a tweet from tweepy
def format_tweet(tweet):
    x = tweet

    processed_doc = {
        "id": x["id"],
        "lang": x["lang"],
        "retweeted_id": x["retweeted_status"]["id"] if "retweeted_status" in x else None,
        "favorite_count": x["favorite_count"] if "favorite_count" in x else 0,
        "retweet_count": x["retweet_count"] if "retweet_count" in x else 0,
        "coordinates_latitude": x["coordinates"]["coordinates"][0] if x["coordinates"] else 0,
        "coordinates_longitude": x["coordinates"]["coordinates"][0] if x["coordinates"] else 0,
        "place": x["place"]["country_code"] if x["place"] else None,
        "user_id": x["user"]["id"],
        "created_at": time.mktime(time.strptime(x["created_at"], "%a %b %d %H:%M:%S +0000 %Y"))
    }

    if x["entities"]["hashtags"]:
        processed_doc["hashtags"] = [{"text": y["text"], "startindex": y["indices"][0]} for y in
                                     x["entities"]["hashtags"]]
    else:
        processed_doc["hashtags"] = []

    if x["entities"]["user_mentions"]:
        processed_doc["usermentions"] = [{"screen_name": y["screen_name"], "startindex": y["indices"][0]} for y in
                                         x["entities"]["user_mentions"]]
    else:
        processed_doc["usermentions"] = []

    if "extended_tweet" in x:
        processed_doc["text"] = x["extended_tweet"]["full_text"]
    elif "full_text" in x:
        processed_doc["text"] = x["full_text"]
    else:
        processed_doc["text"] = x["text"]

    return processed_doc

# Method to push messages to pubsub
def write_to_pubsub(data):
    try:
        if data["lang"] == "en":
            publisher.publish(topic_path, data=json.dumps({
                "text": data["text"],
                "user_id": data["user_id"],
                "id": data["id"],
                "posted_at": datetime.datetime.fromtimestamp(data["created_at"]).strftime('%Y-%m-%d %H:%M:%S')
            }).encode("utf-8"), tweet_id=str(data["id"]).encode("utf-8"))
    except Exception as e:
        raise


# Custom listener class
class tweetListener(Stream):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just pushes tweets to pubsub
    """

    def on_status(self, data):
        write_to_pubsub(format_tweet(data._json))
        return True

    def on_error(self, status):
        print(status)


# Start listening
l = tweetListener(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
l.filter(track=hashtags_list)
