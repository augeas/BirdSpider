
from datetime import datetime
import re

"""
Fields of interest from twitter users and tweets returned by Twython.
Extra fields provided by the various render methods are appended.
"""
twitterUserFields = ['id', 'id_str', 'verified', 'profile_image_url_https', 'followers_count', 'listed_count',
'utc_offset','statuses_count', 'description', 'friends_count', 'location', 'profile_image_url', 'geo_enabled',
'screen_name', 'lang','favourites_count','name', 'url', 'created_at', 'time_zone', 'protected'] + ['isotime', 'last_scraped']

tweetFields = ['text', 'in_reply_to_status_id', 'id', 'favorite_count', 'source','retweeted',
    'in_reply_to_screen_name', 'id_str', 'retweet_count', 'in_reply_to_user_id', 'favorited',
    'in_reply_to_user_id_str', 'possibly_sensitive', 'lang','created_at', 'in_reply_to_status_id_str',
    'quoted_status_id'] + ['isotime', 'last_scraped']

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

def renderTweet(tweet, get_user=False):
    """Return a serializable dictionary of relevant fields for a tweet."""
    rendered = {field:tweet[field] for field in tweetFields if tweet.get(field,False)}
    if tweet.get('created_at',False):
        rendered['isotime'] = twitterTime(tweet['created_at'])
    if tweet.get('coordinates',False):
        lng, lat = tweet['coordinates']['coordinates']
        rendered['longitude'] = lng
        rendered['latitude'] = lat
    if get_user:
        if tweet.get('user',False):
            rendered_user = renderTwitterUser(tweet['user'])
        else:
            rendered_user = False
        return (rendered,rendered_user)
    else:
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
    return ents

def replies(tweets):
    for t in tweets:
        reply_id = t[0].get('in_reply_to_status_id',False)
        if reply_id:
            yield (t[0]['id'],reply_id)

def cleanMentions(entities):
    for m in entities['user_mentions']:
        m[1].pop('indices')

def decomposeTweets(tweets):
    """Decompose a list of tweets returned by Twython into lists of rendered tweets, retweets, mentions, hastags, URLs and replies."""
    allTweets = []
    allRetweets = []
    allQuoteTweets = []
    allUsers = {}
    
    allEntities = {key:entityStore() for key in ['tweet', 'retweet', 'quotetweet']}
    
    for tweet in tweets:
        
        retweeted = tweet.get('retweeted',False)
        quoted_status = tweet.get('quoted_status',False)
        
        if retweeted and not quoted_status:
            raw = tweet['retweeted_status']
            renderedTweet, rendered_user = renderTweet(raw, get_user=True)
            renderedRetweet = renderTweet(tweet)
            allRetweets.append((renderedTweet,renderedRetweet))
            pushEntities(raw, allEntities['retweet'])
            allUsers[renderedTweet['id_str']] = rendered_user
        
        if quoted_status and not retweeted:
            raw = quoted_status
            renderedTweet, rendered_user = renderTweet(raw, get_user=True)
            renderedQuoteTweet = renderTweet(tweet)
            allQuoteTweets.append((renderedTweet,renderedQuoteTweet))
            pushEntities(raw, allEntities['quotetweet'])
            allUsers[renderedTweet['id_str']] = rendered_user
        
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
    
    return {'tweet':allTweets, 'retweet':allRetweets, 'quotetweet':allQuoteTweets,'entities':allEntities, 'replies':allReplies,
    'users':allUsers}