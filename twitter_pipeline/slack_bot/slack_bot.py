import os
import slack
import logging
from sqlalchemy import create_engine
import time





if __name__ == '__main__':

    while True:
        pg = create_engine('postgres://postgres@postgres:5432/postgres')

        query = 'SELECT text, sentiment, time FROM tweets ORDER BY time DESC LIMIT 1;'

        results = pg.execute(query)
        row = list(results)[0]
        text, sentiment, ts = row

        token = ""
        client = slack.WebClient(token=token)

        #response = client.chat_postMessage(channel='#slack_bot', text=f'Tweet:\n```{text}```\n*Sentiment:* {sentiment}\n*Time:* {ts}')

        time.sleep(6000)
