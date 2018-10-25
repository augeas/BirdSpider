# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt

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
    return datetime.strptime(re.sub('[+\-][0-9]{4}\s', '', t), '%a %b %d %X %Y').isoformat()


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
        for item in entities['user_mentions']:
            entity_store['user_mentions'].append(
                (tweet_id,renderTwitterUser(item)))
        for key in entity_types[1:]:
            for item in entities[key]:
                item.pop('indices')
                entity_store[key].append((tweet_id, item))
    extended = tweet.get('extended_entities', False)
    if extended:
        for item in extended['media']:
            for key in ['indices', 'sizes', 'video_info', 'additional_media_info']:
                if item.get(key,False):
                    item.pop(key)
            entity_store['media'].append((tweet_id, item))


def entityStore():
    ents = {key:[] for key in entity_types}
    ents['media'] = []
    return ents


def replies(tweets):
    for t in tweets:
        reply_id = t[0].get('in_reply_to_status_id', False)
        if reply_id:
            yield (t[0]['id'], reply_id)


def cleanMentions(entities):
    for m in entities['user_mentions']:
        m[1].pop('indices')


def decomposeTweets(tweets):
    """Decompose a list of tweets returned by Twython into lists of rendered tweets, retweets, mentions, hastags, URLs and replies. Be sure to set trim_user=False and exclude_replies=False in get_user_timeline."""
    all_tweets = []
    all_retweets = []
    all_quote_tweets = []
    all_users = {}
    
    all_entities = {key:entityStore() for key in ['tweet', 'retweet', 'quotetweet']}
    
    for tweet in tweets:
        
        retweeted = tweet.get('retweeted_status',False)
        quoted_status = tweet.get('quoted_status',False)
        
        if retweeted and not quoted_status:
            raw = retweeted
            rendered_tweet, rendered_user = renderTweet(raw, get_user=True)
            rendered_retweet = renderTweet(tweet)
            all_retweets.append((rendered_tweet, rendered_retweet))
            pushEntities(raw, all_entities['retweet'])
            all_users[rendered_tweet['id_str']] = rendered_user
        
        if quoted_status and not retweeted:
            raw = quoted_status
            rendered_tweet, rendered_user = renderTweet(raw, get_user=True)
            rendered_quote_tweet = renderTweet(tweet)
            all_quote_tweets.append((rendered_tweet, rendered_quote_tweet))
            pushEntities(raw, all_entities['quotetweet'])
            all_users[rendered_tweet['id_str']] = rendered_user
        
        if not retweeted and not quoted_status:
            rendered_tweet = renderTweet(tweet)
            all_tweets.append((rendered_tweet,))
            pushEntities(tweet, all_entities['tweet'])

    all_replies = {}
    all_replies['tweet'] = list(replies(all_tweets))
    all_replies['retweet'] = list(replies(all_retweets))
    all_replies['quotetweet'] = list(replies(all_quote_tweets))
    
    return {'tweet': all_tweets, 'retweet': all_retweets, 'quotetweet': all_quote_tweets, 'entities': all_entities,
            'replies': all_replies, 'users': all_users}