# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt

from datetime import datetime
import json
import logging

from twython import TwythonStreamer, TwythonAuthError, TwythonError

from app import app
from db_settings import cache
from twitter_settings import *



class StreamingTwitter(TwythonStreamer):
    """Wrapper around the Twython class to handle streaming Twitter calls."""

    def __init__(self, stream_handler='twitter_tasks.push_stream_results', credentials=False, retry_count=None, chunk_size=1):
        self.stream_handler = stream_handler

        if credentials:
            creds=json.loads(credentials)
            oauth1_token = creds.get('oauth1_token')
            oauth1_secret = creds.get('oauth1_secret')
            logging.info("*** Calls to Twitter APIs will use user provided OAUTH1 user authentication token and secret ***")
            super(StreamingTwitter, self).__init__(CONSUMER_KEY, CONSUMER_SECRET, oauth1_token, oauth1_secret,
                                                   retry_count=retry_count, chunk_size=chunk_size)
            self.handle = 'user_'
        else:   # TODO handle this choice better
            logging.info("*** Calls to Twitter APIs will use preconfigured OAUTH1 user authentication token and secret ***")
            super(StreamingTwitter, self).__init__(CONSUMER_KEY, CONSUMER_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET,
                                                   retry_count=retry_count, chunk_size=chunk_size)
            self.handle = 'local_'

    def on_success(self, data):
        logging.info('*** tweets streamed, pushing results ***')
        app.send_task(self.stream_handler, args=[data])

    # Problem with the API
    def on_error(self, status_code, data):
        print(status_code, data)
        self.disconnect()
