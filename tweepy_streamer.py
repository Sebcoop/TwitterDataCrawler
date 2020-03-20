from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import API
from tweepy import Cursor

import json
import tweepy
import twitter_credentials
import pymongo

'''
# database setup
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["TwitterDataCrawler"]
#mydb.tweets.create_index("id", unique=True, dropDups=True)
collection = mydb["tweets"]
'''

myclient = pymongo.MongoClient('localhost', 27017)
mydb = myclient.TwitterDataCrawler
mydb.tweets.create_index("id", unique=True, dropDups=True)
collection = mydb.all_tweets

# list of tweet texts to be clustered
tweet_list = []


##### refactor this function to be in twitter streamer class
def getTrends(worldID):

	'''
	Function to return a list containing the top twitter trends
	'''

	auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
	auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
	
	api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

	trends = api.trends_place(worldID)
	trends = json.loads(json.dumps(trends, indent=1))
	trends_list = []

	for trend in trends[0]["trends"]:
		trends_list.append(trend["name"].strip('#'))
	
	return trends_list



class TwitterAuthenticator():

	'''
	Class to authenticate user using twitter credentials
	'''

	def authenticate_twitter_app(self):
		# authentication token
		auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
		auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
		return auth



class TwitterStreamer():

	'''
	Class for streaming and processing live tweets
	'''

	def __init__(self):
		self.twitter_authenticator = TwitterAuthenticator()

	def stream_tweets(self, fetched_tweets_filename, keyword_list):

		uk = (-7.57216793459, 49.959999905, 1.68153079591, 58.6350001085)
		
		# create listener object
		listener = TwitterListener(fetched_tweets_filename)
		auth = self.twitter_authenticator.authenticate_twitter_app()

		#create a twitter stream
		stream = Stream(auth, listener)

		# filter tweets
		stream.filter(track=keyword_list, locations=uk, languages=["en"])



class TwitterListener(StreamListener):

	'''
	listener class tha prints tweets
	'''	

	def __init__(self, fetched_tweets_filename):
		self.fetched_tweets_filename = fetched_tweets_filename
		duplicateTweetCount = 0


	# takes in data, prints it and writes it to our json file
	def on_data(self, data):
		tweet = json.loads(data)

		try:
			collection.insert_one(tweet)
			print("User:", tweet["id_str"], " added Tweet:\n", tweet["text"], "\n")
			return True
		except:
			print("duplicate tweet found")

			self.duplicateTweetCount += 1

	# print error which is passed in through status parameter
	def on_error(self, status):
		if status == 420:
			# return false when rates limit is exceeded
			print("ERROR: RATE LIMIT EXCEEDED")
			return False
		else:
			print("ERROR: ", status)



if __name__ == "__main__":

	# file where we will save twitter data
	fetched_tweets_filename = "tweets.json"	

	# location ID for united kingdom
	uk_woeid = 23424975
	
	# get top trending topcis from twitter
	trends = getTrends(uk_woeid)

	# take only a select number of top trends so we don't exceed rate limit
	keyword_list = trends[:3]

	# define twitter streamer object
	twitter_streamer = TwitterStreamer()
	
	# stream tweets by keyword
	twitter_streamer.stream_tweets(fetched_tweets_filename, keyword_list)

	# close pymongo database
	myclient.close()