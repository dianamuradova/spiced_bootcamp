import config
from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
import json
import logging
import pymongo

#connect to MongoDB (global constants preferrably are in CAPITALS)
CLIENT = pymongo.MongoClient('mongodb') #all we need is the service name from docker_compose
DB = CLIENT.tweets #create and use a database called tweets

def authenticate():
    """Function for handling Twitter Authentication"""
    auth = OAuthHandler(config.CONSUMER_API_KEY, config.CONSUMER_API_SECRET)
    auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET)

    return auth

def load_tweet_into_mongo(tweet):
    DB.tweet_data.insert(tweet)
    #insert into a collection


class TwitterListener(StreamListener):

    # def __init__(self):
    #     ###DEFINE YOUR OWN CLASS VARIABLES HERE



    def on_data(self, data):

        """Whatever we put in this method defines what is done with
        every single tweet as it is intercepted in real-time"""

        t = json.loads(data) #t is just a regular python dictionary.



        if 'extended_tweet' in t:
            text = t['extended_tweet']['full_text']
        else:
            text = t['text']


        is_tweet_reply = t['in_reply_to_status_id'] == None
        is_quote = t['is_quote_status'] == False

        if 'RT' not in t['text'] and is_tweet_reply and is_quote:

            tweet = {'text': text, 'username' : t['user']['screen_name'],
            'number_of_followers' : t['user']['followers_count'],
            'location' : t['user']['location'], 'number_of_friends' : t['user']['friends_count'], 'retweet_count' :
            t['retweet_count']}


            logging.critical('\n\n\nNEW TWEET INCOMING: ' + tweet['text']) #eventually this will be some function that stores it
        #into MongoDB

        #insert_into_mongo(tweet) the goal for tomorrow
            load_tweet_into_mongo(tweet)
            logging.critical('\n\n\nSUCCESSFULLY DUMPED INTO MONGO!')


    def on_error(self, status):

        if status == 420: #if rate-limiting occurs, shut off.
            print(status)
            return False

if __name__ == '__main__':

    #Whatever we write down here will get executed when we run:
    # "python twitter_streamer.py"

    auth = authenticate()
    listener = TwitterListener()
    stream = Stream(auth, listener)
    stream.filter(track=['bitcoin'], languages=['en'])
