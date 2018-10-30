# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt

from datetime import datetime
import json
import logging

from twython import Twython, TwythonAuthError, TwythonRateLimitError, TwythonError
from twython.endpoints import EndpointsMixin

from db_settings import cache
from twitter_settings import *

__twitter_methods__ = [m for m in dir(EndpointsMixin) if not m.startswith('__')]

class RatedTwitter(object):    
    """Wrapper around the Twython class that tracks whether API calls are rate-limited."""

    def __init__(self, use_local=True, oauth1_token=None, oauth1_secret=None):
        if use_local:
            self.twitter = Twython(CONSUMER_KEY, CONSUMER_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
            self.handle = 'local_'
        elif oauth1_token and oauth1_secret:
            self.twitter = Twython(CONSUMER_KEY, CONSUMER_SECRET, oauth1_token, oauth1_secret)
        else:
            self.twitter = Twython(CONSUMER_KEY, CONSUMER_SECRET, access_token=ACCESS_TOKEN)
            self.handle = 'app_'
            
    def can_we_do_that(self, method_name):
        """Check whether a given API call is rate-limited, return the estimated time to wait in seconds.
    
        Positional arguments:
        method_name -- the name of the API call to test    
        """      
        
        # Have we recorded how many calls remain in the current window?
        try:
            keyval = cache.get(self.handle+method_name).decode('utf-8')
        except:
            keyval = False
        # We've not made the call for these credentials. Assume all's well.
        if not keyval: 
            return 0
        else:
            history = json.loads(keyval)
            if history['limit'] > 0:
                # Still good to go.
                return 0
            reset = datetime.strptime(history['reset'].split('.')[0], "%Y-%m-%dT%H:%M:%S")
            rightNow = datetime.now()
            # No calls left and the window reset is in the future...
            if reset > rightNow:
                # ...return the time to wait.
                return (reset - rightNow).seconds + 30
            return 0

    def method_call(self, method_name, *args, **kwargs):
        """Make a Twitter API call via the underlying Twython object.
    
        Returns a tuple: (True,<API call return value>) | (False,<reason for failure>)
    
        Positional arguments:
        method_name -- the name of the API call to test

        """ 
        
        # Does Twython even know how to do that?
        try: 
            method = getattr(self.twitter,method_name)
        except:
            logging.error('*** NO SUCH TWITTER METHOD: '+method_name+' ***')
            return (False,'no_such_method')
        
        # Call the method of the Twython object.
        try:
            result = (True, method(*args, **kwargs))
        except TwythonAuthError:
            logging.error('*** TWITTER METHOD 401: '+method_name+' ***')
            result = (False, 'forbidden')
        except TwythonRateLimitError:
            logging.error('*** TWITTER METHOD LIMITED: '+method_name+' ***')
            result = (False, 'limited')
        except TwythonError as e:
            if str(e.error_code) == '404':
                logging.error('*** TWITTER METHOD 404: '+method_name+' ***')
                result = (False, '404')
            else:
                logging.error('*** TWITTER METHOD FAILED: '+method_name+' ***')
                result = (False, 'unknown')
            logging.error(args)

        # Have we been told how many calls remain in the current window?
        try: 
            xLimit = self.twitter.get_lastfunction_header('x-rate-limit-remaining')
            xReset = self.twitter.get_lastfunction_header('x-rate-limit-reset')
        except:
            xLimit = xReset = False
            
        if xLimit:
            limit = int(xLimit)        
        if xReset:
            reset = datetime.utcfromtimestamp(int(xReset)).isoformat()
        if xLimit and xReset:
            # Store the current number of remaining calls and time when the window resets.
            cache.set(self.handle+method_name, json.dumps({'limit':limit, 'reset':reset}))

        return result
                
"""
Attach the following API calls to the ratedTwitter class, so that <ratedTwitter>.<method>(*args, **kargs)
makes the call via <ratedTwitter.method_call and <ratedTwitter>.<method>_wait() makes the appropriate
call to can_we_do_that.
"""

def method_factory(name):
    def f(self, *args ,**kwargs):
        return self.method_call(name, *args, **kwargs)
    return f

for name in __twitter_methods__:
    setattr(RatedTwitter, name, method_factory(name))

def rated_method_factory(name):   
    def g(self):
        return self.can_we_do_that(name)
    return g

for name in __twitter_methods__:
    setattr(RatedTwitter, name+'_wait', rated_method_factory(name))


# appears to not be in use anywhere
def getTwitterAPI(credentials=False):
    """Return a Twitter API object from oauth credentials, defaulting to those in db_settings."""
    if not credentials:
        return Twython(CONSUMER_KEY, access_token=ACCESS_TOKEN)
