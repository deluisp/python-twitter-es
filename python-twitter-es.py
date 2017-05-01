import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from textblob import TextBlob
from elasticsearch import Elasticsearch
from datetime import datetime
import time

# import twitter keys and tokens
from config import *

# create instance of elasticsearch
es = Elasticsearch()

filtros = ['real madrid', 'barcelona']

class TweetStreamListener(StreamListener):

    # on success
    def on_data(self, data):

        # decode json
        dict_data = json.loads(data)

        # pass tweet into TextBlob
        tweet = TextBlob(dict_data["text"])
        #print(tweet.translate(to="es"))
        #print(tweet.tags)

        # output sentiment polarity
        print(tweet.sentiment.polarity)

        # determine if sentiment is positive, negative, or neutral
        if tweet.sentiment.polarity < 0:
            sentiment = "Negativo"
        elif tweet.sentiment.polarity == 0:
            sentiment = "Neutral"
        else:
            sentiment = "Positivo"

        # output sentiment
        print(sentiment)

        location = None
        if dict_data["coordinates"] != None:
            location = dict_data["coordinates"]["coordinates"]

        #temp_tags = tweet.tags
        #tags = [x for x,_ in temp_tags]

        # add text and sentiment info to elasticsearch
        #index= datetime.now().strftime("twitter-%Y.%m.%d"),
        es.index(index= "twitter",
                 doc_type="tweet",
                 body={"user id": dict_data["user"]["id"],
                       "user name": dict_data["user"]["name"],
                       "user screen name": dict_data["user"]["screen_name"],
                       "user location": dict_data["user"]["location"],
                       "user url": dict_data["user"]["url"],
                       "user description": dict_data["user"]["description"],
                       "user protected": dict_data["user"]["protected"],
                       "user verified": dict_data["user"]["verified"],
                       "user followers count": dict_data["user"]["followers_count"],
                       "user friends count": dict_data["user"]["friends_count"],
                       "user listed count": dict_data["user"]["listed_count"],
                       "user favourites count": dict_data["user"]["favourites_count"],
                       "user statuses count": dict_data["user"]["statuses_count"],
                       "user created at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.strptime(dict_data["user"]["created_at"],"%a %b %d %H:%M:%S +0000 %Y")),
                       "created at": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.strptime(dict_data['created_at'],'%a %b %d %H:%M:%S +0000 %Y')),
                       "id": dict_data["id"],
                       "text": dict_data["text"],
                       "source": dict_data["source"],
                       "truncated": dict_data["truncated"],
                       "favorited": dict_data["favorited"],
                       "favorite count": dict_data["favorite_count"],
                       "retweeted": dict_data["retweeted"],
                       "retweet count": dict_data["retweet_count"],
                       "lang": dict_data["lang"],
                       "filter level": dict_data["filter_level"],
                       "is quote status": dict_data["is_quote_status"],
                       "location": location,
                       "filtros": filtros,
                       "polaridad": tweet.sentiment.polarity,
                       "subjetividad": tweet.sentiment.subjectivity,
                       "sentimiento": sentiment,
                       #"text traducido": tweet.translate(to="es"),
                       #"tags": tags,
                       "tags": tweet.noun_phrases,
                       "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                       }
                 )
        
        return True

    # on failure
    def on_error(self, status):
        print(status)

if __name__ == '__main__':

    # create instance of the tweepy tweet stream listener
    listener = TweetStreamListener()

    # set twitter keys/tokens
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # create instance of the tweepy stream
    stream = Stream(auth, listener)

    # search twitter for "congress" keyword
    stream.filter(track=filtros)
    # stream.filter(track=['congress'], async=True)
