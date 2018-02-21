
from twython import Twython, TwythonAuthError, TwythonRateLimitError, TwythonError

from db_setting import cache
from twitter_settings import *

class ratedTwitter(object):
    """Wrapper around the Twython class that tracks whether API calls are rate-limited."""
    def __can_we_do_that__(self,methodName):
        """Check whether a given API call is rate-limited, return the estimated time to wait in seconds.
    
        Positional arguments:
        methodName -- the name of the API call to test    
        """      
        keyval = cache.get(self.handle+methodName) # Have we recorded how many calls remain in the current window?
        if not keyval: # We've not made the call for these credentials. Assume all's well.
            return 0
        else:
            history = json.loads(keyval)
            if history['limit'] > 0: # Still good to go.
                return 0
            reset = datetime.strptime( history['reset'].split('.')[0], "%Y-%m-%dT%H:%M:%S" )
            rightNow = datetime.now()
            if reset > rightNow: # No calls left and the window reset is in the future...
                return (reset - rightNow).seconds + 30 # ...return the time to wait.
            return 0

    def __method_call__(self,methodName,args):
        """Make a Twitter API call via the underlying Twython object.
    
        Returns a tuple: (True,<API call return value>) | (False,<reason for failure>)
    
        Positional arguments:
        methodName -- the name of the API call to test
        args -- dictionary of keyword arguments
        """ 
        try: # Does Twython even know how to do that?
            method = getattr(self.twitter,methodName)
        except:
            print '*** NO SUCH TWITTER METHOD: '+methodName+' ***'
            return (False,'no_such_method')
        
        try:
            result = (True,method(**args)) # Call the method of the Twython object.
        except TwythonAuthError:
            print '*** TWITTER METHOD 401: '+methodName+' ***'
            result = (False,'forbidden')
        except TwythonRateLimitError:
            print '*** TWITTER METHOD LIMITED: '+methodName+' ***'
            result = (False,'limited')
        except TwythonError as e:
            if str(e.error_code) == '404':
                print '*** TWITTER METHOD 404: '+methodName+' ***'
                result = (False,'404')
            else:
                print '*** TWITTER METHOD FAILED: '+methodName+' ***'
                result = (False,'unknown')
            print args

        try: # Have we been told how many calls remain in the current window?
            xLimit = self.twitter.get_lastfunction_header('x-rate-limit-remaining')
            xReset = self.twitter.get_lastfunction_header('x-rate-limit-reset')
        except:
            xLimit = xReset = False
            
        if xLimit:
            limit = int(xLimit)        
        if xReset:
            reset = datetime.utcfromtimestamp(int(xReset)).isoformat()
        if xLimit and xReset: # Store the current number of remaining calls and time when the window resets.
            cache.set(self.handle+methodName,json.dumps({'limit':limit, 'reset':reset})) 

        return result

    def __init__(self,credentials=False,useLocal=False):
        
        if not credentials:
            if useLocal:
                self.twitter = Twython(CONSUMER_KEY,CONSUMER_SECRET,OAUTH_TOKEN,OAUTH_TOKEN_SECRET)
                self.handle = 'local_'
            else:
                self.twitter = Twython(CONSUMER_KEY,access_token=ACCESS_TOKEN)
                self.handle = 'app_'
                
"""
Attach the following API calls to the ratedTwitter class, so that <ratedTwitter>.<method>(**args) makes the call via <ratedTwitter.__method_call__
and <ratedTwitter>.<method>_limited() makes the appropriate call to __can_we_do_that__.
"""
for name in ['lookup_user','get_friends_list','get_followers_list','get_user_timeline']:
    def f(self,name=name,**args): # http://math.andrej.com/2009/04/09/pythons-lambda-is-broken/
        return self.__method_call__(name,args)
    setattr(ratedTwitter,name,f)
    
    def g(self,name=name):
        return self.__can_we_do_that__(name)
    setattr(ratedTwitter,name+'_limited',g)    
            
def getTwitterAPI(credentials=False):
    """Return a Twitter API object from oauth credentials, defaulting to those in db_settings."""
    if not credentials:
        return Twython(CONSUMER_KEY,access_token=ACCESS_TOKEN)
