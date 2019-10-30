import json
import logging
import pymongo
from sqlalchemy import create_engine
import random
import time
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import requests


#connect to MongoDB (global constants preferrably are in CAPITALS)
CLIENT = pymongo.MongoClient('mongodb') #all we need is the service name from docker_compose
DB = CLIENT.tweets #create and use a database called tweets

#connect to postgrespg

pg = create_engine('postgres://postgres@postgres:5432/postgres')


create_table = """
CREATE TABLE IF NOT EXISTS tweets (
        id VARCHAR(64) UNIQUE NOT NULL,
        text TEXT,
        sentiment REAL,
        username TEXT,
        number_of_followers REAL,
        location TEXT,
        number_of_friends REAL,
        retweet_count REAL,
        time timestamp
    );"""



pg.execute(create_table)

if __name__ == '__main__':

    logging.critical('\n\n\nHELLO FROM THE ETL JOB!!!\n\n\n')

    n = 0
    # 1. load tweets from MongoDB
    while True:
        analyzer = SentimentIntensityAnalyzer()
        tweets = DB.tweet_data.find().skip(n).limit(60)
        n += 5
        for tw in tweets:
            logging.error(str(tw) + '\n\n')

    #2. transform the data

            id = tw['_id']
            text = tw['text']
            text = text.replace("'", '_').replace('"', '_')
            sentiment = analyzer.polarity_scores(text)["compound"]
            username = tw['username']
            number_of_followers  = tw['number_of_followers']
            location = tw['location']
            number_of_friends = tw['number_of_friends']
            retweet_count = tw['retweet_count']

    #3. store the data in Postgres

            query = f"INSERT INTO tweets VALUES ('{id}', '{text}', {sentiment}, '{username}', {number_of_followers}, '{location}', {number_of_friends}, {retweet_count}, now() );"
            try:
                pg.execute(query)
                logging.info('\nTweet written to Postgres\n')
            except:
                logging.error('\nFailed to write tweet\n')

        time.sleep(30)
