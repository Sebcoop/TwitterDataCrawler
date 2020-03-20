from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import API
from tweepy import Cursor

import datetime
import json
import tweepy
import twitter_credentials
import tweepy_streamer as ts
import pymongo


auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

myclient = pymongo.MongoClient('localhost', 27017)
mydb = myclient.TwitterDataCrawler
mydb.tweets.create_index("id", unique=True, dropDups=True)
collection = mydb.all_tweets

# location ID for united kingdom
uk_woeid = 23424975
	
# get top trending topcis from twitter
trends = ts.getTrends(uk_woeid)
top_trends = trends[:3]
# number of tweets to try and retrieve
count = 50

def convertDate(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()

for i in range(len(top_trends)):
	for tweet in tweepy.Cursor(api.search, q=top_trends[i], lang="en", count=1).items(count):
		#print(tweet)

		#formatted_tweet = json.loads(tweet)
		formatted_tweet = json.loads(json.dumps(tweet._json))
		convert_date = convertDate(formatted_tweet)
		try: 
			collection.add_objects(convert_date)
		except:
			print("duplicate found")

		with open("tweets.json", 'a') as tf:
				tf.write(json.dumps(formatted_tweet))
				#tf.write(json.dumps(formatted_tweet, default=convertDate))
