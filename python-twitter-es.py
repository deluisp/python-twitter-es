import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from textblob import TextBlob
from elasticsearch import Elasticsearch
from datetime import datetime
import time
import operator
from collections import Counter
import re
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import string
import unicodedata
 
emoticons_str = r"""
    (?:
        [:=;] # Eyes
        [oO\-]? # Nose (optional)
        [D\)\]\(\]/\\OpP] # Mouth
    )"""
 
regex_str = [
    emoticons_str,
    r'<[^>]+>', # HTML tags
    r'(?:@[\w_]+)', # @-mentions
    r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)", # hash-tags
    r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+', # URLs
 
    r'(?:(?:\d+,?)+(?:\.?\d+)?)', # numbers
    r"(?:[a-z][a-z'\-_]+[a-z])", # words with - and '
    r'(?:[\w_]+)', # other words
    r'(?:\S)' # anything else
]
    
tokens_re = re.compile(r'('+'|'.join(regex_str)+')', re.VERBOSE | re.IGNORECASE)
emoticon_re = re.compile(r'^'+emoticons_str+'$', re.VERBOSE | re.IGNORECASE)
 
def tokenize(s):
    return tokens_re.findall(s)
 
def preprocess(s, lowercase=False):
    tokens = tokenize(s)
    if lowercase:
        tokens = [token if emoticon_re.search(token) else token.lower() for token in tokens]
    return tokens

def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])

# import twitter keys and tokens
from config import *

# create instance of elasticsearch
es = Elasticsearch()

filtros = ['real madrid', 'atletico de madrid', 'champions']

class TweetStreamListener(StreamListener):

    # on success
    def on_data(self, data):

        # decode json
        dict_data = json.loads(data)

        # pass tweet into TextBlob
        tweet = TextBlob(remove_accents(dict_data["text"].lower()))

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

        terms_all = [term for term in preprocess(remove_accents(dict_data["text"].lower()), True)]
        punctuation = list(string.punctuation)
        stop = stopwords.words('english') + stopwords.words("spanish") + punctuation + ['rt', 'RT', 'via', '...']
        terms_hash = [term for term in preprocess(remove_accents(dict_data["text"].lower()), True) if term.startswith('#')]
        terms_mentions = [term for term in preprocess(remove_accents(dict_data["text"].lower()), True) if term.startswith('@')]
        terms_tags = [term for term in preprocess(remove_accents(dict_data["text"].lower()), True) if term not in stop and not term.startswith(('#', '@', 'http', '-', '...'))]
        terms_urls = [term for term in preprocess(remove_accents(dict_data["text"].lower()), True) if term.startswith(('http'))]
        
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
                       "tags": terms_tags,
                       "hastags": terms_hash,
                       "urls": terms_urls,
                       "mentions": terms_mentions,
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
