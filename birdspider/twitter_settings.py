# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt

from os import environ

CONSUMER_KEY = environ['CONSUMER_KEY']
CONSUMER_SECRET = environ['CONSUMER_SECRET']
OAUTH_TOKEN = environ['OAUTH_TOKEN']
OAUTH_TOKEN_SECRET = environ['OAUTH_TOKEN_SECRET']

ACCESS_TOKEN = environ['ACCESS_TOKEN']

API_TIMEOUT = 900

SUPERNODE_FOLLOWERS = 1000
SUPERNODE_FOLLOWING = 1000