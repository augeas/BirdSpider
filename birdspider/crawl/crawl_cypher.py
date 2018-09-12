
# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt

__author__ = 'Giles Richard Greenway'


import json
import redis
from datetime import datetime, timedelta
from db_settings import neoDb, cache


#TODO: make sure this query really does the following
# how this function works
# a user node has the friend and follower counts as attributes (in json returned when you get the user,
#  so therefore in neo node added)
# if the count of fr and fol in the attributes of a node are more than half of actual num of relations in the graph
# then that is the node you pick to scrape next
# rephrase or rewrite?
# actual number of relationships is less than the count attribute
def nextFriends(latest=False, max_friends=2000, max_followers=2000, limit=20):
    """ Return a list of non-supernode users who have fewer friend relationships than Twitter thinks they should."""
    desc = 'DESC' if latest else ''

    query = """MATCH (a:twitter_user)-[:FOLLOWS]-(b:twitter_user) WITH a, COUNT(*) as c
        WHERE c < a.friends_count/2 AND a.friends_count < {} AND a.followers_count < {} AND NOT EXISTS (a.protected)
        AND NOT EXISTS (a.defunct)
        RETURN a.screen_name
        ORDER BY a.last_scraped {} LIMIT {}""".format(max_friends, max_followers, desc, limit)

    with neoDb.session() as session:
        with session.begin_transaction() as tx:
            result = tx.run(query)
            next_friends = [record.values[0] for record in result]

    return next_friends


def nextFollowers(latest=False, max_friends=2000, max_followers=2000, limit=20):
    """ Return a list of non-supernode users who have fewer follower relationships than Twitter thinks they should."""
    desc = ' DESC' if latest else ''
    query = """MATCH (b:twitter_user)-[:FOLLOWS]-(a:twitter_user) WITH a, COUNT(*) as c
        WHERE c < a.followers_count/2 AND a.followers_count < {} AND a.friends_count < {} AND NOT EXISTS (a.protected)
        AND NOT EXISTS (a.defunct)
        RETURN a.screen_name
        ORDER BY a.last_scraped {} LIMIT {}""".format(max_followers, max_friends, desc, limit)

    with neoDb.session() as session:
        with session.begin_transaction() as tx:
            result = tx.run(query)
            next_followers = [record.values[0] for record in result]

    return next_followers


def nextTweets(latest=False, max_friends=2000, max_followers=2000, limit=20, max_tweets=3000):
    """ Return a list of non-supernode users who have fewer tweets than Twitter thinks they should."""
    desc = ' DESC' if latest else ''
    query = """MATCH (a:twitter_user) WHERE NOT (a)-[:TWEETED]->(:tweet) WITH a, COUNT(*) as c
        WHERE c < a.statuses_count AND c < {} AND a.followers_count < {} AND a.friends_count < {}
        AND NOT EXISTS (a.protected) AND NOT EXISTS (a.defunct)
        RETURN a.screen_name
        ORDER BY a.last_scraped {} LIMIT {}""".format(max_tweets, max_followers, max_friends, desc, limit)

    with neoDb.session() as session:
        with session.begin_transaction() as tx:
            result = tx.run(query)
            next_tweets = [record.values[0] for record in result]

    return next_tweets


def whoNext(job, latest=False):
    """Find the next user to retrieve friends, followers or tweets, closest to the initial seed of the network."""
    if job == 'friends':
        victim_getter = nextFriends
        
    if job == 'followers':
        victim_getter = nextFollowers

    if job == 'tweets':
        victim_getter = nextTweets
          
    victim_list = False
    while not victim_list:
        try:
            victim_list = victim_getter(latest=latest)
        except:
            pass
        
    return victim_list[0]


def nextNearest(user, job, max_friends=2000, max_followers=2000, limit=20, max_tweets=2000, test=False):
    """Find the next user to retrieve friends, followers or tweets, closest to a given user."""
    cacheKey = '_'.join(['nextnearest', job, user])
    nextUserDump = cache.get(cacheKey).decode('utf-8')
    next_users = False
    if nextUserDump:
        try:
            next_users = json.loads(nextUserDump)
        except:
            next_users = []
    if next_users:
        print('*** NEXT '+job+': '+', '.join(next_users)+' ***')
        next_user = next_users.pop(0)
        cache.set(cacheKey, json.dumps(next_users))
        return next_user
    
    queryStr = ('MATCH (a:twitter_user{{screen_name:"'+user+'"}})-[:FOLLOWS]-(d:twitter_user)').format()+'\n'
    queryStr += ' MATCH (b:twitter_user)-[:FOLLOWS]-(d) WITH DISTINCT b '
    if job == 'friends':
        queryStr += 'MATCH (b)-[:FOLLOWS]->(c:twitter_user) '
    if job == 'followers':
        queryStr += 'MATCH (b)<-[:FOLLOWS]-(c:twitter_user) '
    if job == 'tweets':
        queryStr += 'MATCH (b)-[:TWEETED]->(c:tweet) '
    queryStr += 'WITH b, COUNT(c) AS n\n'     
    queryStr += 'WHERE b.friends_count < {} AND b.followers_count < {} ' \
                'AND NOT EXISTS (b.protected) AND NOT EXISTS (b.defunct) '.format(max_friends, max_followers)
    if job == 'friends':
        queryStr += 'AND n < b.friends_count/2\n'
    if job == 'followers':
        queryStr += 'AND n < b.followers_count/2\n'
    if job == 'tweets':
        queryStr += 'AND b.statuses_count > 0 AND n < b.statuses_count/2 AND n<{} '.format(max_tweets)
    queryStr += 'RETURN b.screen_name ORDER BY b.{}_last_scraped LIMIT {}'.format(job, limit)

    print('*** Looking for '+job+' ***')
 
    if test:
        return queryStr
    
    query = queryStr
    try:
        with neoDb.session() as session:
            with session.begin_transaction() as tx:
                result = tx.run(query)
                next_users = [record.values[0] for record in result]
    except:
        next_users = []
    
    if next_users:
        print('*** NEXT '+job+': '+', '.join(next_users)+' ***')
        next_user = next_users.pop(0)
        cache.set(cacheKey, json.dumps(next_users))
        return next_user
    else:
        print('No more '+job+' for '+user)
    
    return False
