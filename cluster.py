import tweepy_streamer as ts
import pymongo
import collections

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.feature_extraction import DictVectorizer
from sklearn.metrics import adjusted_rand_score
import re
from collections import Counter

import collections
from nltk import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
from pprint import pprint

myclient = pymongo.MongoClient('localhost', 27017)
mydb = myclient.TwitterDataCrawler
tweets = mydb.all_tweets

tweet_list = []
processed_tweet_list = []
user_data_list = []
hashtag_list = []


hashtags = []
hashtag_dict = {}
user_count = {}



def remove_numbers(string):
	return re.sub('[0-9]', '', string)

def remove_special_characters(text):
	return re.sub(r"[^a-zA-Z0-9]+", ' ', text)

def remove_unicode(text):
	return (text.encode("ascii", "ignore")).decode("utf-8")

def remove_url(string):
	#return re.sub(r"http\S+", "", string)
	return re.sub("https?://\s+", '', text)
	
def remove_single_letters(string):
	return re.sub(r"\b[a-zA-Z]\b", "", string)


def process(text):
	remove_sc = remove_special_characters(text)
	remove_num = remove_numbers(remove_sc)
	remove_u = remove_url(remove_num)
	remove_uni = remove_unicode(remove_u)
	remove_letters = remove_single_letters(remove_uni)
	lower = remove_letters.lower()

	return lower




if __name__ == "__main__":

	for t in tweets.find():
		#print(t)
		try:				
			if t['truncated']:
				'''
				truncated_tweet = {
					t['_id']: t['extended_tweet']['full_text']
				}
				'''
				truncated_text = t['extended_tweet']['full_text']
				hashtags = t['entities']['hashtags']
				user_data = {
					'text': truncated_text,
					'_id' : t["id_str"],
					'hashtags': hashtags,					
					}
				truncated_tweet = process(truncated_text)
				tweet_list.append(text)				
				hashtag_list.append(hashtags)
				user_data_list.append(user_data)

			else:
				'''
				tweet = {
					t['_id']: t['text']
				}
				'''
				text = t['text']
				hashtags = t['entities']['hashtags']
				user_data = {
					'text': truncated_text,
					'_id' : t["id_str"],
					'hashtags': hashtags,					
					}
				truncated_tweet = process(text)
				tweet_list.append(text)
				hashtag_list.append(hashtags)
				user_data_list.append(user_data)
		except:
			pass


	for hashtag in hashtag_list:
		for i in hashtag:
			h = process(i['text']) # string
			if h in hashtag_dict:
				hashtag_dict[h] += 1
			else:
				hashtag_dict[h] = 1
	

	# get the top 10 hashtags
	#top_hashtags = sorted_hashtags[-5:]
	top_hashtags = sorted(hashtag_dict, key=hashtag_dict.get, reverse=True)[:10]
	
	print(user_data_list)


	nclusters = 30
	words_per_cluster = 20
	print("Clustering...\n")
	print("Number of clusters: ", nclusters)
	print("Keywords per cluster: ", words_per_cluster)
	vectorizer = TfidfVectorizer(stop_words='english')
	X = vectorizer.fit_transform(tweet_list)

	model = KMeans(n_clusters=nclusters, init='k-means++', max_iter=100, n_init=1)
	labels = model.fit(X)

	print("Printing Top terms per cluster...")
	order_centroids = model.cluster_centers_.argsort()[:, ::-1]
	terms = vectorizer.get_feature_names()

	for i in range(nclusters):
	    print("Cluster %d:" % i),
	    for ind in order_centroids[i, :words_per_cluster]:
	        print(' %s' % terms[ind])
	    print("\n")

	print("printing Clusters...")
	clusters = collections.defaultdict(list)
	for i, label in enumerate(model.labels_):
	    clusters[label].append(i)
	cl = dict(clusters)

	for c in range(nclusters):
	    print("Cluster %i:\n" % (c))
	    for i,s in enumerate(clusters[c]):
	        print("\tSentence %i: %s\n" % (i,tweet_list[s]))

