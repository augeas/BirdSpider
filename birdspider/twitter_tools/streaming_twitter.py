# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt

from datetime import datetime, timedelta
import json
import logging

from twython import TwythonStreamer, TwythonAuthError, TwythonError

from app import app
from db_settings import cache
from twitter_settings import *



class StreamingTwitter(TwythonStreamer):
    """Wrapper around the Twython class to handle streaming Twitter calls."""

    def __init__(self, stream_handler='twitter_tasks.push_stream_results', credentials=False, retry_count=None, stream_id='', batch_size=10, batch_wait_time=30):
        self.stream_handler = stream_handler
        self.stream_id = stream_id
        self.tweets = list()
        self.batch_size = batch_size
        self.batch_wait_time = batch_wait_time
        self.batch_start_time = None

        if credentials:
            creds=json.loads(credentials)
            oauth1_token = creds.get('oauth1_token')
            oauth1_secret = creds.get('oauth1_secret')
            logging.info("*** Calls to Twitter APIs will use user provided OAUTH1 user authentication token and secret ***")
            super(StreamingTwitter, self).__init__(CONSUMER_KEY, CONSUMER_SECRET, oauth1_token, oauth1_secret,
                                                   retry_count=retry_count)
            self.handle = 'user_'
        else:   # TODO handle this choice better
            logging.info("*** Calls to Twitter APIs will use preconfigured OAUTH1 user authentication token and secret ***")
            super(StreamingTwitter, self).__init__(CONSUMER_KEY, CONSUMER_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET,
                                                   retry_count=retry_count)
            self.handle = 'local_'

#    def _request(self, url, method='GET', params=None):
 #       do stuff?
        # super._request(url, method=method, params=params)

    def on_success(self, data):
        logging.info('*** tweets streamed ***')
        if not self.batch_start_time:
            self.batch_start_time = datetime.now()

        self.tweets.append(data)
        if len(self.tweets) >= self.batch_size or self.batch_start_time < datetime.now()-timedelta(seconds=self.batch_wait_time):
            logging.info('*** tweets streamed, pushing %d results ***', len(self.tweets))
            app.send_task(self.stream_handler, args=[self.tweets])
            self.tweets = list()
            self.batch_start_time = datetime.now()

    # Problem with the API
    def on_error(self, status_code, data):
        logging.error(status_code, data)
        self.disconnect()

    @property
    def connected(self):
        key = 'stream_' + self.stream_id + '_connected'
        return cache.get(key)

    @connected.setter
    def connected(self, val):
        key = 'stream_' + self.stream_id + '_connected'
        cache.set(key, val)