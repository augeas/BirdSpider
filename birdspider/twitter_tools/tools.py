
from datetime import datetime
import re

"""
Fields of interest from twitter users and tweets returned by Twython.
Extra fields provided by the various render methods are appended.
"""
twitterUserFields = [u'id', u'id_str', u'verified', u'profile_image_url_https', u'followers_count', u'listed_count',
u'utc_offset',u'statuses_count', u'description', u'friends_count', u'location', u'profile_image_url', u'geo_enabled',
u'screen_name', u'lang',u'favourites_count',u'name', u'url', u'created_at', u'time_zone', u'protected'] + [u'isotime',u'last_scraped']

tweetFields = [u'text',u'in_reply_to_status_id',u'id',u'favorite_count',u'source',u'retweeted',
    u'in_reply_to_screen_name',u'id_str',u'retweet_count',u'in_reply_to_user_id',u'favorited',
    u'in_reply_to_user_id_str',u'possibly_sensitive',u'lang',u'created_at',u'in_reply_to_status_id_str'] + [u'isotime',u'last_scraped']

def twitterTime(t):
    """Return Twitter's time format as isoformat."""
    return datetime.strptime(re.sub('[+\-][0-9]{4}\s','',t),'%a %b %d %X %Y').isoformat()

def renderTwitterUser(user):
    """Return a serializable dictionary of relevant fields for a Twitter user."""
    twit = {field:user[field] for field in twitterUserFields if user.get(field,False)}
    # Suplement Twitter's time field with something saner.
    if user.get('created_at',False):
        twit['isotime'] = twitterTime(user['created_at'])    
    return twit