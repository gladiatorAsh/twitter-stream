from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient
import json

access_token = "add token"
access_token_secret =  "add token"
consumer_key =  "add token"
consumer_secret =  "add token"

class StdOutListener(StreamListener):
    def on_data(self, data):
        resp=json.loads(data)
        lang=resp.get("lang")
        tweet=resp.get("text")
        if(tweet is not None and lang=="en" and "RT" not in tweet):
            producer.send_messages("breaking", tweet.encode('utf-8'))
            print (tweet)
        return True
    def on_error(self, status):
        print (status)
        
track=["obama","trump"]
kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
listener = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, listener)
stream.filter(track=track)