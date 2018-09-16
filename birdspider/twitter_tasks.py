
# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt
"""Celery tasks relating to Twitter."""

__author__ = 'Giles Richard Greenway'

from datetime import datetime

from celery import chain, group
from celery.utils.log import get_task_logger

from app import app
from db_settings import cache
from solr_tools import tweets2Solr
from twitter_settings import *
from twitter_tools.neo import  connections2Neo, tweetDump2Neo, users2Neo, setUserDefunct
from twitter_tools.rated_twitter import RatedTwitter
from twitter_tools.tools import renderTwitterUser, decomposeTweets
from crawl.crawl_cypher import nextNearest, whoNext

logger = get_task_logger(__name__)


@app.task
def twitterCall(method_name, **kwargs):
    """Attempt a given Twitter API call, retry if rate-limited. Returns the result of the call.
    
        Positional arguments:
        methodName -- the Twitter API method to call
        args -- a dicionary of keyword arguments
        
    """
    api = RatedTwitter()
    limit = api.can_we_do_that(method_name)
    if limit:
        loggier.info('*** TWITTER RATE-LIMITED: %s ***' % method_name)
        raise twitterCall.retry(exc=Exception('Twitter rate-limited',method_name), countdown = limit)
    else:
        okay, result = api.method_call(method_name, **kwargs)
        if okay:
            loggier.info('*** TWITTER CALL: %s ***' % method_name)
            return result
        else:
            assert False

@app.task
def pushRenderedTwits2Neo(twits):
    users2Neo(twits)

@app.task
def pushTwitterUsers(twits):
    """Store Twitter users returned by a Twitter API call in Neo4J.
    
    Positional arguments:
    twits -- a list of Twitter users as returned by Twython
    """
    rightNow = datetime.now().isoformat()
    for twit in twits:
        twit['last_scraped'] = rightNow
        
    renderedTwits = [ renderTwitterUser(twit) for twit in twits ]
    pushRenderedTwits2Neo.delay(renderedTwits)

@app.task
def getTwitterUsers(users,credentials=False):
    """Look-up a set of Twitter users by screen_name and store them in Neo4J.

    Positional arguments:
    users -- a list of screen_names
    
    """
    userList = ','.join(users)
    chain(twitterCall.s('lookup_user',**{'screen_name':userList}), pushTwitterUsers.s())()


@app.task
def pushRenderedTweets2Neo(user, tweetDump):
    tweetDump2Neo(user, tweetDump)
    
@app.task
def pushRenderedTweets2Solr(tweets):
    tweets2Solr(tweets)

@app.task
def pushTweets(tweets, user, cacheKey=False):
    """ Dump a set of tweets from a given user's timeline to Neo4J/Solr.

    Positional arguments:
    tweets -- a list of tweets as returned by Twython.
    user -- screen_name of the user
    
    Keyword arguments:
    cacheKey -- a Redis key that identifies an on-going task to grab a user's timeline
    
    """
    
    tweetDump = decomposeTweets(tweets) # Extract mentions, URLs, replies hashtags etc...

    pushRenderedTweets2Neo.delay(user, tweetDump)
    for label in ['tweet', 'retweet', 'quotetweet']:
        pushRenderedTweets2Solr.delay([t[0] for t in tweetDump[label]])

    if cacheKey: # These are the last Tweets, tell the scaper we're done.
        cache.set(cacheKey,'done')
        logger.info('*** %s: DONE WITH TWEETS ***' % user) 

@app.task
def getTweets(user, maxTweets=3000, count=0, tweetId=0, cacheKey=False, credentials=False):
    """Get tweets from the timeline of the given user, push them to Neo4J.
    
    Positional arguments:
    user -- The screen_name of the user

    Keyword arguments:
    maxTweets -- The maximum number of tweets to retrieve
    cacheKey -- a Redis key that identifies an on-going task to grab a user's timeline
    count -- The number of tweets already retrieved, set when the task calls itself
    tweetId -- The maximum tweet ID to retrieve, set when the task calls itself
    
    """
    api = RatedTwitter()
    limit = api.get_user_timeline_wait()
    if limit:
        logger.info('*** TWITTER RATE-LIMITED: statuses.user_timeline: %s:%d  ***' % (user, str(count)))
        raise getTweets.retry(countdown=limit)
    else:
        args = {'screen_name': user, 'exclude_replies': False, 'include_rts': True, 'trim_user': False, 'count': 200}
        if tweetId:
            args['max_id'] = tweetId

        okay, result = api.get_user_timeline(**args)

        if okay:
            print('*** TWITTER USER_TIMELINE: '+user+':'+str(tweetId)+' ***')
            if result:
                newCount = count + len(result)
                if maxTweets:
                    if newCount > maxTweets: # No need for the task to call itself again.
                        pushTweets.delay(result, user, cacheKey=cacheKey) # Give pushTweets the cache-key to end the job.
                        return
                    else:
                        pushTweets.delay(result, user)

                newTweetId = min([t['id'] for t in result]) - 1
                # Not done yet, the task calls itself with an updated count and tweetId.
                getTweets.delay(user, maxTweets=maxTweets, count=newCount, tweetId=newTweetId, cacheKey=cacheKey, credentials=credentials)
            else:
                pushTweets.delay([], user, cacheKey=cacheKey) # Nothing more found, so tell pushTweets the job is done.
        else:
            if result == '404':
                setUserDefunct(user)
            cache.set('scrape_tweets', 'done')
            if result == 'limited':
                raise getTweets.retry(countdown = api.get_user_timeline_wait())

@app.task
def pushRenderedConnections2Neo(user, renderedTwits, friends=True):
    connections2Neo(user,renderedTwits,friends=friends)
                        
@app.task
def pushTwitterConnections(twits, user, friends=True, cacheKey=False):
    """Push the Twitter connections of a given user to Neo4J.
    
    Positional arguments:
    twits -- a list of Twitter users as returned by Twython
    user -- The screen_name of the user

    Keyword arguments:
    friends -- "twits" are the user's friends if True, (default) else they're followers 
    cacheKey -- a Redis key that identifies an on-going task to grab a user's friends or followers
    
    """

    if friends:
        job = ' FRIENDS'
    else:
        job = ' FOLLOWERS'
    
    if twits:
        rendered_twits = [renderTwitterUser(twit) for twit in twits]
        pushRenderedConnections2Neo.delay(user, rendered_twits, friends=friends)
# These are the last Tweets, tell the scaper we're done.
    if cacheKey: # These are the last connections, tell the scaper we're done.
        cache.set(cacheKey, 'done')
        logger.info('*** %s: DONE WITH %s ***' % (user, job))

@app.task
def getTwitterConnections(user, friends=True, cursor=-1, credentials=False, cacheKey=False):
    """Get the connections of the given user, push them to Neo4J.

    Positional arguments:
    user -- The screen_name of the user

    Keyword arguments:
    friends -- "twits" are the user's friends if True, (default) else they're followers 
    cacheKey -- a Redis key that identifies an on-going task to grab a user's friends or followers
    cursor -- Id of the next block of connections to retrieve, set when the task calls itself
    """
    api = RatedTwitter()
    if friends:
        method = api.get_friends_list
        limit = api.get_friends_list_wait()
        method_name = 'get_friends_list'
    else:
        method = api.get_followers_list
        limit = api.get_followers_list_wait()
        method_name = 'get_followers_list'
    
    if limit:
        logger.info('*** TWITTER RATE-LIMITED: %s:%s ***' % (method_name, str(cursor)))
        raise getTwitterConnections.retry(countdown=limit)
    else:
        okay,result = method(screen_name=user, cursor=cursor, count=200) # We can get a maximum of 200 connections at once.       
        if okay:
            logger.info('*** TWITTER CURSOR: %s:%s:%s ***' % (method_name, user, str(cursor)))
            twits = result['users']
            next_cursor = result.get('next_cursor', False)
            if next_cursor: # Unless the next cursor is 0, we're not done yet.
                getTwitterConnections.delay(user, friends=friends, cursor=next_cursor, cacheKey=cacheKey, credentials=credentials)
                pushTwitterConnections.delay(twits, user, friends=friends)
            else:
                pushTwitterConnections.delay(twits, user, friends=friends, cacheKey=cacheKey) # All done, send the cacheKey.
                    
        else:
            if result == 'limited':
                raise getTwitterConnections.retry(exc=Exception('Twitter rate-limited', method_name), countdown=API_TIMEOUT)
            if result == '404':
                setUserDefunct(user)
                if friends:
                    cache.set('scrape_friends', 'done')
                else:
                    cache.set('scrape_followers', 'done')

@app.task
def seedUser(user, scrape=False):
    """Retrieve the given Twitter user's account, and their timelines, friends and followers. Optionally, start scraping around them."""
    print('*** SEEDING: '+user+' ***')
    if scrape:
        chain(getTwitterUsers.s([user]), getTwitterConnections.si(user), getTwitterConnections.si(user, friends=False),
              getTweets.si(user, maxTweets=1000), startUserScrape.si(user))()
    else:
        chain(getTwitterUsers.s([user]), getTwitterConnections.si(user), getTwitterConnections.si(user, friends=False),
              getTweets.si(user, maxTweets=1000))()

@app.task
def startScrape(latest=False):
    """Start the default scrape, retrieving the users that need timelines, friends or followers updated,
    in the order that they were first added. """
    logger.info('*** STARTED SCRAPING: DEFAULT: ***') 
    cache.set('default_scrape', 'true')
    cache.set('scrape_mode', 'default')
    
    for key in ['scrape_friends', 'scrape_followers', 'scrape_tweets']:
        cache.set(key, '')
    
    doDefaultScrape.delay(latest=latest)
          
@app.task
def doDefaultScrape(latest=False):
    """Retrieve the tweets, friends or followers of trhe next users in the default scrape."""
    keep_going = cache.get('default_scrape').decode('utf-8')
    if (not keep_going) or keep_going != 'true':
        logger.info('*** STOPPED DEFAULT SCRAPE ***') 
        return False
    
    logger.info('*** SCRAPING... ***')

    this_friend = cache.get('scrape_friends').decode('utf-8')
    if (not this_friend) or this_friend == 'done':
        cache.set('scrape_friends','running')
        getTwitterConnections.delay(whoNext('friends', latest=latest), cacheKey='scrape_friends')
    else:
        logger.info('*** FRIENDS BUSY ***')

    this_follower = cache.get('scrape_followers').decode('utf-8')
    if (not this_follower) or this_follower == 'done':
        cache.set('scrape_followers','running')
        getTwitterConnections.delay(whoNext('friends', latest=latest), friends=False, cacheKey='scrape_followers')
    else:
        logger.info('*** FOLLOWERS BUSY ***')

    this_tweet = cache.get('scrape_tweets').decode('utf-8')
    if (not this_tweet) or this_tweet == 'done':
        cache.set('scrape_tweets', 'running')
        getTweets.delay(whoNext('tweets', latest=latest), maxTweets=1000, cacheKey='scrape_tweets')
    else:
        logger.info('*** TWEETS BUSY ***')
                    
    doDefaultScrape.apply_async(kwargs={'latest':latest},countdown=30)

@app.task
def startUserScrape(user):
    """Start scraping around the given user."""
    logger.info('*** STARTED SCRAPING: USER: %s ***' % (user,))
    cache.set('user_scrape', 'true')
    cache.set('scrape_mode', 'user')
    cache.set('scrape_user', user)

    for key in ['scrape_friends','scrape_followers', 'scrape_tweets']:
        cache.set(key, '')
        
    for job in ['friends', 'followers', 'tweets']:
        cache_key = '_'.join(['nextnearest', job, user])
        cache.set(cache_key, False)
        
    doUserScrape.delay()
    
@app.task
def doUserScrape():
    """Retrieve the next timelines, friends and followers for the next accounts in the user scrape. """
    keep_going = cache.get('user_scrape').decode('utf-8')
    if (not keep_going) or keep_going != 'true':
        logger.info('*** STOPPED USER SCRAPE ***') 
        return False
    
    user = cache.get('scrape_user').decode('utf-8')
    logger.info('*** SCRAPING USER: %s... ***' % (user,))

    this_friend = cache.get('scrape_friends').decode('utf-8')
    if (not this_friend) or this_friend == 'done':
        next_friends = nextNearest(user, 'friends')
        if next_friends:
            cache.set('scrape_friends', 'running')
            getTwitterConnections.delay(next_friends, cacheKey='scrape_friends')
    else:
        logger.info('*** FRIENDS BUSY ***')

    this_follower = cache.get('scrape_followers').decode('utf-8')
    if (not this_follower) or this_follower == 'done':
        next_followers = nextNearest(user,'followers')
        if next_followers:
            cache.set('scrape_followers','running')
            getTwitterConnections.delay(next_followers, friends=False, cacheKey='scrape_followers')
    else:
        logger.info('*** FOLLOWERS BUSY ***')

    this_tweet = cache.get('scrape_tweets').decode('utf-8')
    if (not this_tweet) or this_tweet == 'done':
        next_tweets = nextNearest(user, 'tweets')
        if next_tweets:
            cache.set('scrape_tweets', 'running')
            getTweets.delay(next_tweets, maxTweets=1000, cacheKey='scrape_tweets')
    else:
        logger.info('*** TWEETS BUSY ***')

    if 'running' in [cache.get(k).decode('utf-8') for k in ['scrape_friends', 'scrape_followers', 'scrape_tweets']]:
        doUserScrape.apply_async(countdown=30)
    else:
        cache.set('user_scrape', '')
        cache.set('scrape_mode', '')
        logger.info('*** FINISHED SCRAPING USER: %s ***' % (user,))

