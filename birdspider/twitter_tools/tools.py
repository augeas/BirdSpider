
from datetime import datetime
import re

"""
Fields of interest from twitter users and tweets returned by Twython.
Extra fields provided by the various render methods are appended.
"""
twitterUserFields = [u'id', u'id_str', u'verified', u'profile_image_url_https', u'followers_count', u'listed_count',
u'utc_offset',u'statuses_count', u'description', u'friends_count', u'location', u'profile_image_url', u'geo_enabled',
u'screen_name', u'lang',u'favourites_count',u'name', u'url', u'created_at', u'time_zone', u'protected'] + [u'isotime', u'last_scraped']

tweetFields = [u'text', u'in_reply_to_status_id', u'id', u'favorite_count', u'source',u'retweeted',
    u'in_reply_to_screen_name', u'id_str', u'retweet_count', u'in_reply_to_user_id', u'favorited',
    u'in_reply_to_user_id_str', u'possibly_sensitive', u'lang',u'created_at', u'in_reply_to_status_id_str',
    u'quoted_status_id'] + [u'isotime', u'last_scraped']

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

def renderTweet(tweet):
    """Return a serializable dictionary of relevant fields for a tweet."""
    rendered = {field:tweet[field] for field in tweetFields if tweet.get(field,False)}
    if tweet.get('created_at',False):
        rendered['isotime'] = twitterTime(tweet['created_at'])
    if tweet.get('coordinates',False):
        lng, lat = tweet['coordinates']['coordinates']
        rendered['longitude'] = lng
        rendered['latitude'] = lat
    if tweet.get('user',False):
        rendered['user'] = renderTwitterUser(tweet['user'])
    return rendered

entity_types = ['user_mentions', 'hashtags', 'urls']

def pushEntities(tweet, entity_store):
    tweet_id = tweet['id_str']
    entities = tweet.get('entities',False)
    if entities:    
        for key in entity_types:
            for item in entities[key]:
                entity_store[key].append((tweet_id, item))
    extended = tweet.get('extended_entities', False)
    if extended:
        for item in extended['media']:
            entity_store['media'].append((tweet_id, item))

def entityStore():
    ents = {key:[] for key in entity_types}
    ents['media'] = []

def replies(tweets):
    for t in tweets:
        reply_id = t.get('in_reply_to_status_id',False)
        if reply_id:
            yield (t['id'],reply_id)

def cleanMentions(entities):
    for m in entities['user_mentions']:
        m[1].pop('indices')

def decomposeTweets(tweets):
    """Decompose a list of tweets returned by Twython into lists of rendered tweets, retweets, mentions, hastags, URLs and replies."""
    allTweets = []
    allRetweets = []
    allQuoteTweets = []
    
    allEntities = {key:entityStore() for key in ['tweet', 'retweet', 'quotetweet']}
    
    for tweet in tweets:
        
        retweeted = tweet.get('retweeted',False)
        quoted_status = tweet.get('quoted_status',False)
        
        if retweeted and not quoted_status:
            raw = tweet['retweeted_status']
            renderedTweet = renderTweet(raw)
            renderedRetweet = renderTweet(tweet)
            allRetweets.append((renderedTweet,renderedRetweet))
            pushEntities(raw, allEntities['retweet'])
        
        if quoted_status and not retweeted:
            raw = quoted_status
            renderedTweet = renderTweet(raw)
            renderedQuoteTweet = r
            allQuoteTweets.append((renderedTweet,renderedQuoteTweet))
            pushEntities(raw, allEntities['quotetweet'])
        
        if not retweeted and not quoted_status:
            renderedTweet = renderTweet(tweet)
            allTweets.append((renderedTweet,))
            pushEntities(tweet, allEntities['tweet'])

    allReplies = {}
    allReplies['tweet'] = list(replies(allTweets))
    allReplies['retweet'] = list(replies(allRetweets))
    allReplies['quotetweet'] = list(replies(allQuoteTweets))

    cleanMentions(allEntities['tweet'])
    cleanMentions(allEntities['retweet'])
    cleanMentions(allEntities['quotetweet'])
    
    return {'tweet':allTweets, 'retweet':allRetweets, 'quotetweet':allQuoteTweets,'entities':allEntities, 'replies':allReplies}
            