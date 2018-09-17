# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt

from os import environ

CONSUMER_KEY = environ.get('CONSUMER_KEY', None)
CONSUMER_SECRET = environ.get('CONSUMER_SECRET', None)
OAUTH_TOKEN = environ.get('OAUTH_TOKEN', None)
OAUTH_TOKEN_SECRET = environ.get('OAUTH_TOKEN_SECRET', None)

ACCESS_TOKEN = environ.get('ACCESS_TOKEN', None)

API_TIMEOUT = 900

SUPERNODE_FOLLOWERS = 1000
SUPERNODE_FOLLOWING = 1000