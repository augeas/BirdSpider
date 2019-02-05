
# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt
"""Celery tasks relating to Twitter."""

__author__ = 'Giles Richard Greenway'

from datetime import datetime

from celery import chain, group
from celery.task.control import revoke
from celery.utils.log import get_task_logger

from app import app
from db_settings import get_neo_driver, cache
from solr_tools import tweets2Solr
from twitter_settings import *
from twitter_tools.neo import connections2Neo, tweetDump2Neo, users2Neo, setUserDefunct
from twitter_tools.rated_twitter import RatedTwitter
from twitter_tools.streaming_twitter import StreamingTwitter
from twitter_tools.tools import renderTwitterUser, decomposeTweets
from crawl.crawl_cypher import nextNearest, whoNext, start_user_crawl, update_crawl

logger = get_task_logger(__name__)


@app.task(name='twitter_tasks.twitterCall', bind=True)
def twitterCall(self, method_name, credentials=False, **kwargs):
    """Attempt a given Twitter API call, retry if rate-limited. Returns the result of the call.
    
        Positional arguments:
        methodName -- the Twitter API method to call
        args -- a dicionary of keyword arguments
        
    """
    api = RatedTwitter(credentials=credentials)
    limit = api.can_we_do_that(method_name)
    if limit:
        logger.info('*** TWITTER RATE-LIMITED: %s ***' % method_name)
        raise twitterCall.retry(exc=Exception('Twitter rate-limited', method_name), countdown=limit)
    else:
        okay, result = api.method_call(method_name, **kwargs)
        if okay:
            logger.info('*** TWITTER CALL: %s  suceeded***' % method_name)
            return result
        else:
            assert False


@app.task(name='twitter_tasks.pushRenderedTwits2Neo', bind=True)
def pushRenderedTwits2Neo(self, twits):
    db = get_neo_driver()
    users2Neo(db, twits)
    db.close()


@app.task(name='twitter_tasks.pushTwitterUsers', bind=True)
def pushTwitterUsers(self, twits):
    """Store Twitter users returned by a Twitter API call in Neo4J.
    
    Positional arguments:
    twits -- a list of Twitter users as returned by Twython
    """
    logger.info('***Push twitter users to neo ***')
    rightNow = datetime.now().isoformat()
    for twit in twits:
        twit['last_scraped'] = rightNow
        logger.info('***Push twitter user: ' + twit['screen_name'] + ' ***')
    renderedTwits = [renderTwitterUser(twit) for twit in twits]
    pushRenderedTwits2Neo.delay(renderedTwits)


@app.task(name='twitter_tasks.getTwitterUsers', bind=True)
def getTwitterUsers(self, users, credentials=False):
    """Look-up a set of Twitter users by screen_name and store them in Neo4J.

    Positional arguments:
    users -- a list of screen_names
    
    """
    userList = ','.join(users)
    logger.info('***Getting twitter users: ' + userList + ' ***')
    chain(twitterCall.s('lookup_user', credentials, **{'screen_name': userList}), pushTwitterUsers.s())()


@app.task(name='twitter_tasks.start_stream', bind=True)
def start_stream(self, track=None, follow=False, credentials=False):

    logger.info('***Starting twitter filter stream***')
    # TODO one of filter_terms or follow is required, return error if neither present
    # start the stream
    stream_task = stream_filter.delay(credentials=credentials, track=track)
    cache.set("stream_id_" + self.request.id, stream_task.id)


@app.task(name='twitter_tasks.stop_stream', bind=True)
def stop_stream(self, task_id):

    # stop the stream running in the stream started by the given task id
    stream_id = cache.get("stream_id_" + task_id)
    logger.info('***Stopping twitter filter streamer in task id: '  + stream_id + ' ***')
    cache.set('stream_' + stream_id + '_connected', False)
    # clean up the cache


@app.task(name='twitter_tasks.stream_filter', bind=True)
def stream_filter(self, credentials=False, retry_count=None, track=None):

    logger.info('***Creating twitter filter streamer in task id: ' + self.request.id + ' ***')

    streamer = StreamingTwitter(credentials=credentials, retry_count=retry_count, stream_id=self.request.id)
    streamer.statuses.filter(track=track)


@app.task(name='twitter_tasks.push_stream_results', bind=True)
def push_stream_results(self, results):

    logger.info('***Push twitter filter stream results***')
    users = []
    for tweet in results:
        users.append(tweet['user'])
    pushTwitterUsers.delay(users)
    for tweet in results:
        logger.info('***Push tweet id %s***' % (tweet['id']))
        pushTweets.delay([tweet], tweet['user']['screen_name'])


@app.task(name='twitter_tasks.search', bind=True)
def search(self, query_terms, result_type='mixed', page_size=100, lang='en', tweet_id=0, maxTweets=False, count=0, credentials=False):

    logger.info('***Starting TWITTER search ***')
    api = RatedTwitter(credentials=credentials)
    limit = api.search_wait()
    if limit:
        logger.info('*** TWITTER RATE-LIMITED: search starts with: %s:%d  ***' % (query_terms[0], str(count)))
        raise search.retry(countdown=limit)
    else:
        query = {'q': query_terms,
                 'result_type': result_type,
                 'count': page_size,
                 'lang': lang,
                 }
        if tweet_id:
            query['max_id'] = tweet_id

        okay, result = api.search(**query)

        if okay:
            logger.info('*** TWITTER search starts with: %s:%s ***' % (query_terms[0], str(tweet_id)))
            if result:
                push_search_results.delay(result)
                newCount = count + len(result['statuses'])
                if maxTweets:
                    if newCount > maxTweets: # No need for the task to call itself again.
                        return

                try:
                    # Parse the data returned to get max_id to be passed in consequent call.
                    next_results_url_params = result['search_metadata']['next_results']
                    next_max_id = next_results_url_params.split('max_id=')[1].split('&')[0]

                    # Not done yet, the task calls itself with an updated count and tweetId.
                    search.delay(query_terms, maxTweets=maxTweets, count=newCount, tweet_id=next_max_id,
                                 result_type=result_type, page_size=page_size, lang=lang, credentials=credentials)
                except:  #do we have anything we want in the except clause if there is not a next batch of tweets?
                    return
        else:
            if result == 'limited':
                raise search.retry(countdown=api.search_wait())


@app.task(name='twitter_tasks.push_search_results', bind=True)
def push_search_results(self, search_results, cacheKey=False):

    statuses = search_results['statuses']

    for tweet in statuses:
        pushTwitterUsers.delay([tweet['user']])
        pushTweets.delay([tweet], tweet['user']['screen_name'])


@app.task(name='twitter_tasks.pushRenderedTweets2Neo', bind=True)
def pushRenderedTweets2Neo(self, user, tweetDump):
    db = get_neo_driver()
    tweetDump2Neo(db, user, tweetDump)
    db.close()


@app.task(name='twitter_tasks.pushRenderedTweets2Solr', bind=True)
def pushRenderedTweets2Solr(self, tweets):
    tweets2Solr(tweets)


@app.task(name='twitter_tasks.pushTweets', bind=True)
def pushTweets(self, tweets, user, cacheKey=False):
    """ Dump a set of tweets from a given user's timeline to Neo4J/Solr.

    Positional arguments:
    tweets -- a list of tweets as returned by Twython.
    user -- screen_name of the user
    
    Keyword arguments:
    cacheKey -- a Redis key that identifies an on-going task to grab a user's timeline
    
    """
    logger.info('Executing pushTweets task id {0.id}, task parent id {0.parent_id}, root id {0.root_id}'.format(self.request))

    tweetDump = decomposeTweets(tweets)  # Extract mentions, URLs, replies hashtags etc...

    pushRenderedTweets2Neo.delay(user, tweetDump)
        
    for label in ['tweet', 'retweet', 'quotetweet']:
        pushRenderedTweets2Solr.delay([t[0] for t in tweetDump[label]])

    if cacheKey: # These are the last Tweets, tell the scraper we're done.
        cache.set(cacheKey, 'done')
        logger.info('*** %s: DONE WITH TWEETS ***' % user) 


@app.task(name='twitter_tasks.getTweets', bind=True)
def getTweets(self, user, maxTweets=3000,  count=0, tweetId=0, cacheKey=False, credentials=False):
    logger.info('Executing getTweets task id {0.id}, args: {0.args!r} kwargs: {0.kwargs!r}'.format(self.request))
    logger.info('task parent id {0.parent_id}, root id {0.root_id}'.format(self.request))
    """Get tweets from the timeline of the given user, push them to Neo4J.
    
    Positional arguments:
    user -- The screen_name of the user

    Keyword arguments:
    maxTweets -- The maximum number of tweets to retrieve
    cacheKey -- a Redis key that identifies an on-going task to grab a user's timeline
    count -- The number of tweets already retrieved, set when the task calls itself
    tweetId -- The maximum tweet ID to retrieve, set when the task calls itself
    
    """
    api = RatedTwitter(credentials=credentials)
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
            logger.info('*** TWITTER USER_TIMELINE: %s:%s ***' % (user, str(tweetId)))
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
                pushTweets.delay([], user, cacheKey=cacheKey)  # Nothing more found, so tell pushTweets the job is done.
        else:
            if result == '404':
                db = get_neo_driver()
                setUserDefunct(db, user)
                db.close()
            cache.set('scrape_tweets_' + self.request.root_id, 'done')
            if result == 'limited':
                raise getTweets.retry(countdown=api.get_user_timeline_wait())


@app.task(name='twitter_tasks.pushRenderedConnections2Neo', bind=True)
def pushRenderedConnections2Neo(self, user, renderedTwits, friends=True):
    db = get_neo_driver()
    connections2Neo(db, user,renderedTwits,friends=friends)
    db.close()


@app.task(name='twitter_tasks.pushTwitterConnections', bind=True)
def pushTwitterConnections(self, twits, user, friends=True, cacheKey=False):
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

    if cacheKey:  # These are the last connections, tell the scraper we're done.
        cache.set(cacheKey, 'done')
        logger.info('*** %s: DONE WITH %s ***' % (user, job))


@app.task(name='twitter_tasks.getTwitterConnections', bind=True)
def getTwitterConnections(self, user, friends=True, cursor=-1, credentials=False, cacheKey=False):
    """Get the connections of the given user, push them to Neo4J.

    Positional arguments:
    user -- The screen_name of the user

    Keyword arguments:
    friends -- "twits" are the user's friends if True, (default) else they're followers 
    cacheKey -- a Redis key that identifies an on-going task to grab a user's friends or followers
    cursor -- Id of the next block of connections to retrieve, set when the task calls itself
    """
    api = RatedTwitter(credentials=credentials)
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
        okay, result = method(screen_name=user, cursor=cursor, count=200)  # We can get a maximum of 200 connections at once.
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
                db = get_neo_driver()
                setUserDefunct(db, user)
                db.close()
                if friends:
                    cache.set('scrape_friends_' + self.request.root_id, 'done')
                else:
                    cache.set('scrape_followers_' + self.request.root_id, 'done')


@app.task(name='twitter_tasks.seedUser', bind=True)
def seedUser(self, user, scrape=False, credentials=False):
    """Retrieve the given Twitter user's account, and their timelines, friends and followers. Optionally, start scraping around them."""
    logger.info('*** SEEDING: %s ***' % (user,))
    logger.info('Executing getTweets task id {0.id}, args: {0.args!r} kwargs: {0.kwargs!r}'.format(self.request))
    logger.info('task parent id {0.parent_id}, root id {0.root_id}'.format(self.request))

    if scrape:
        chain(getTwitterUsers.s([user], credentials=credentials),
              getTwitterConnections.si(user, credentials=credentials), getTwitterConnections.si(user, credentials=credentials, friends=False),
              getTweets.si(user, maxTweets=1000, credentials=credentials), startUserScrape.si(user, credentials=credentials))()
    else:
        chain(getTwitterUsers.s([user], credentials=credentials), getTwitterConnections.si(user, credentials=credentials),
              getTwitterConnections.si(user, credentials=credentials, friends=False),
              getTweets.si(user, maxTweets=1000, credentials=credentials))()


@app.task(name='twitter_tasks.startScrape', bind=True)
def startScrape(self, latest=False, credentials=False):
    """Start the default scrape, retrieving the users that need timelines, friends or followers updated,
    in the order that they were first added. """
    logger.info('*** STARTED SCRAPING: DEFAULT: ***') 
    cache.set('default_scrape_' + self.request.root_id, 'true')
    cache.set('scrape_mode_' + self.request.root_id, 'default')
    
    for key in ['scrape_friends', 'scrape_followers', 'scrape_tweets']:
        cache.set(key + '_' + self.request.root_id, '')
    
    doDefaultScrape.delay(latest=latest, credentials=credentials)


@app.task(name='twitter_tasks.doDefaultScrape', bind=True)
def doDefaultScrape(self, latest=False, credentials=False):
    """Retrieve the tweets, friends or followers of trhe next users in the default scrape."""
    keep_going = cache.get('default_scrape_' + self.request.root_id)
    if (not keep_going) or keep_going.decode('utf-8') != 'true':
        logger.info('*** STOPPED DEFAULT SCRAPE ***') 
        return False
    
    logger.info('*** SCRAPING... ***')

    this_friend = cache.get('scrape_friends_' + self.request.root_id)
    if (not this_friend) or this_friend.decode('utf-8') == 'done':
        cache.set('scrape_friends_' + self.request.root_id, 'running')
        getTwitterConnections.delay(whoNext('friends', latest=latest),  credentials=credentials, cacheKey='scrape_friends_' + self.request.root_id)
    else:
        logger.info('*** FRIENDS BUSY ***')

    this_follower = cache.get('scrape_followers_' + self.request.root_id)
    if (not this_follower) or this_follower.decode('utf-8') == 'done':
        cache.set('scrape_followers_' + self.request.root_id, 'running')
        getTwitterConnections.delay(whoNext('friends', latest=latest), credentials=credentials, friends=False, cacheKey='scrape_followers_' + self.request.root_id)
    else:
        logger.info('*** FOLLOWERS BUSY ***')

    this_tweet = cache.get('scrape_tweets_' + self.request.root_id)
    if (not this_tweet) or this_tweet.decode('utf-8') == 'done':
        cache.set('scrape_tweets_' + self.request.root_id, 'running')
        getTweets.delay(whoNext('tweets', latest=latest), maxTweets=1000, credentials=credentials, cacheKey='scrape_tweets_' + self.request.root_id)
    else:
        logger.info('*** TWEETS BUSY ***')
                    
    doDefaultScrape.apply_async(kwargs={'latest': latest}, credentials=credentials, countdown=30)


@app.task(name='twitter_tasks.startUserScrape', bind=True)
def startUserScrape(self, user, credentials=False):
    """Start scraping around the given user."""
    logger.info('*** STARTED SCRAPING: USER: %s ***' % (user,))
    cache.set('user_scrape_' + self.request.root_id, 'true')
    cache.set('scrape_mode_' + self.request.root_id, 'user')
    cache.set('scrape_user_' + self.request.root_id, user)

    # add crawl node for this user as centre of scrape
    db = get_neo_driver()
    start_user_crawl(db, user, crawl_task=self.request.root_id, status='initiated')
    db.close()

    for key in ['scrape_friends','scrape_followers', 'scrape_tweets']:
        cache.set(key + self.request.root_id, '')
        
    for job in ['friends', 'followers', 'tweets']:
        cache_key = '_'.join(['nextnearest', job, user, self.request.root_id])
        cache.set(cache_key, False)
        
    doUserScrape.delay(credentials=credentials)


@app.task(name='twitter_tasks.doUserScrape', bind=True)
def doUserScrape(self, credentials=False):
    """Retrieve the next timelines, friends and followers for the next accounts in the user scrape. """
    keep_going = cache.get('user_scrape_' + self.request.root_id)
    if (not keep_going) or keep_going.decode('utf-8') != 'true':
        logger.info('*** STOPPED USER SCRAPE ***')
        # mark crawl as stopped on crawl node
        db = get_neo_driver()
        update_crawl(db, crawl_task=self.request.root_id, status='done')
        db.close()
        return False

    user = cache.get('scrape_user_' + self.request.root_id).decode('utf-8')
    logger.info('*** SCRAPING USER: %s... ***' % (user,))

    this_friend = cache.get('scrape_friends_' + self.request.root_id).decode('utf-8')
    if (not this_friend) or this_friend == 'done':
        db = get_neo_driver()
        next_friends = nextNearest(db, user, 'friends', self.request.root_id)
        db.close()
        if next_friends:
            cache.set('scrape_friends_' + self.request.root_id, 'running')
            getTwitterConnections.delay(next_friends, cacheKey='scrape_friends_' + self.request.root_id)
    else:
        logger.info('*** FRIENDS BUSY ***')

    this_follower = cache.get('scrape_followers_' + self.request.root_id).decode('utf-8')
    if (not this_follower) or this_follower == 'done':
        db = get_neo_driver()
        next_followers = nextNearest(db, user, 'followers', self.request.root_id)
        db.close()
        if next_followers:
            cache.set('scrape_followers_' + self.request.root_id, 'running')
            getTwitterConnections.delay(next_followers, friends=False, cacheKey='scrape_followers_' + self.request.root_id)
    else:
        logger.info('*** FOLLOWERS BUSY ***')

    this_tweet = cache.get('scrape_tweets').decode('utf-8')
    if (not this_tweet) or this_tweet == 'done':
        db = get_neo_driver()
        next_tweets = nextNearest(db, user, 'tweets', self.request.root_id)
        db.close()
        if next_tweets:
            cache.set('scrape_tweets_' + self.request.root_id, 'running')
            getTweets.delay(next_tweets, maxTweets=1000, credentials=credentials, cacheKey='scrape_tweets_' + self.request.root_id)
    else:
        logger.info('*** TWEETS BUSY ***')

    if 'running' in [cache.get(k).decode('utf-8') for k in
                     ['scrape_friends_' + self.request.root_id, 'scrape_followers_' + self.request.root_id,
                      'scrape_tweets_' + self.request.root_id]]:
        doUserScrape.apply_async(countdown=30, credentials=credentials)
    else:
        cache.set('user_scrape_' + self.request.root_id, 'false')
        cache.set('scrape_mode_' + self.request.root_id, '')
        logger.info('*** FINISHED SCRAPING USER: %s ***' % (user,))



# currently does simplistic cache value based halt. The effect is that the next time round on the loop, the
# executing scrape will test for keep going and see 'false' instead of true and stop gracefully at that point.
@app.task(name='twitter_tasks.stop_scrape', bind=True)
def stop_scrape(self, task_id):
    '''Stop an excuting scrape on the next loop.'''
    scrape_mode = cache.get('scrape_mode_' + task_id)
    if scrape_mode:
        scrape_mode = scrape_mode.decode('utf-8')
        cache.set(scrape_mode + '_scrape_' + task_id, 'false')