import tweepy
import Config
from datetime import datetime
import time
import json
import boto3
import re
import uuid

kinesis_client = boto3.client("kinesis", region_name="us-east-1")

bearer_token = Config.BEARER_TOKEN
api_key = Config.API_KEY
api_secret = Config.API_SECRET
access_token = Config.ACCESS_TOKEN
access_token_secret = Config.ACCESS_TOKEN_SECRET

TWEET_LOCATION = "United States"
TWEET_LANGUAGE = "en-US"

STREAM_NAME = "twitter-data-stream"


class MyStream(tweepy.StreamingClient):
    # This function gets called when the stream is working
    def on_connect(self):
        print("Connected")

    def on_data(self, data):
        status = json.loads(data)

        #Tweet cleanup steps
        def cleaner(tweet):
            tweet = re.sub("@[A-Za-z0-9]+","",tweet) #Remove @ sign
            tweet = re.sub(r"(?:\@|http?\://|https?\://|www)\S+", "", tweet) #Remove http links
            tweet = " ".join(tweet.split())
            tweet = tweet.replace("#", "").replace("_", " ") #Remove hashtag sign but keep the text
            return tweet

        cleaned_tweet = cleaner(status["data"]["text"])

        data = {
            "tweet_id": status["data"]["id"],
            "tweet_text": cleaned_tweet,
            "tweet_location": TWEET_LOCATION,
            "tweet_language": TWEET_LANGUAGE,
            "date_time": str(datetime.utcnow().timestamp())
        }
        
        response = kinesis_client.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(data),
            PartitionKey=str(uuid.uuid4())
        )

        print("Status: " +
              json.dumps(response["ResponseMetadata"]["HTTPStatusCode"]))

        print(data)

        time.sleep(1)

    def on_error(self, error):
        print(error)




stream = MyStream(bearer_token=bearer_token)
stream.add_rules(tweepy.StreamRule("place_country:US lang:en")) #adding the rules
# stream.delete_rules(ids=1609658825212321792)
# print(stream.get_rules())
stream.filter() #runs the stream

