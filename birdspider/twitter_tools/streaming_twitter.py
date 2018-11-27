# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt

from datetime import datetime
import json
import logging

from twython import TwythonStreamer, TwythonAuthError, TwythonError

from db_settings import cache
from twitter_settings import *
from twitter_tasks import push_stream_results


class StreamingTwitter(TwythonStreamer):
    """Wrapper around the Twython class to handle streaming Twitter calls."""

    def __init__(self, credentials=False, chunk_size=1, retry_count=None):

        if credentials:
            creds=json.loads(credentials)
            oauth1_token = creds.get('oauth1_token')
            oauth1_secret = creds.get('oauth1_secret')
            logging.info("*** Calls to Twitter APIs will use user provided OAUTH1 user authentication token and secret ***")
            self.twitter = StreamingTwitter(CONSUMER_KEY, CONSUMER_SECRET, oauth1_token, oauth1_secret, chunk_size, retry_count)
            self.handle = 'user_'
        else:   # TODO handle this choice better
            logging.info("*** Calls to Twitter APIs will use preconfigured OAUTH1 user authentication token and secret ***")
            self.twitter = StreamingTwitter(CONSUMER_KEY, CONSUMER_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET, chunk_size, retry_count)
            self.handle = 'local_'

    def stream(self, filter, follow=None):
        logging.info('*** open twitter stream ***')
        self.twitter.statuses.filter(track=filter, follow=follow)

    def on_success(self, data):
        logging.info('*** tweets streamed, pushing results ***')
        push_stream_results.delay(data)

    # Problem with the API
    def on_error(self, status_code, data):
        print(status_code, data)
        self.disconnect()
