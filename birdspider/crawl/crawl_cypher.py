
# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt

__author__ = 'Giles Richard Greenway'


import json
import logging
import redis
from datetime import datetime, timedelta


def start_user_crawl(db, user, crawl_task, status='initiated'):
    """ Crawl centred on a user """
    started = datetime.now()
    right_now = started.isoformat()

    # push new node to neo4j
    crawl_data = {'timestamp': right_now, 'crawl_task': crawl_task, 'status': status}
    create_query = '''UNWIND {data} AS d
        CREATE (a:crawl {timestamp: d.timestamp, crawl_task: d.crawl_task, status: d.status})
        RETURN id(a)'''

    with db.session() as session:
        with session.begin_transaction() as tx:
            crawl_id = tx.run(create_query, data=crawl_data).single().value()

    # relationship: crawl--centred_on-->user
    match = "MATCH (c:crawl), (t:twitter_user {{screen_name: '{}'}}) WHERE ID(c) = {}".format(user, crawl_id)

    merge = "MERGE (c)-[:CENTRED_ON]->(t)"

    relation_query = '\n'.join(['UNWIND {data} AS d', match, merge])
    with db.session() as session:
        with session.begin_transaction() as tx:
            tx.run(relation_query, data=user)

    return crawl_id


def update_crawl(db, crawl_task, status):
    updated = datetime.now()
    right_now = updated.isoformat()
    crawl_data = {'timestamp': right_now, 'status': status}
    query = '''UNWIND {data} AS d
    MATCH (c:crawl {{crawl_task: '{}'}})
    SET c.status = d.status, c.timestamp = d.right_now'''.format(crawl_task)

    with db.session() as session:
        with session.begin_transaction() as tx:
            tx.run(query, data=crawl_data)



#TODO: make sure this query really does the following
# how this function works
# a user node has the friend and follower counts as attributes (in json returned when you get the user,
#  so therefore in neo node added)
# if the count of fr and fol in the attributes of a node are more than half of actual num of relations in the graph
# then that is the node you pick to scrape next
# rephrase or rewrite?
# actual number of relationships is less than the count attribute
def nextFriends(db, latest=False, max_friends=2000, max_followers=2000, limit=20):
    """ Return a list of non-supernode users who have fewer friend relationships than Twitter thinks they should."""
    desc = 'DESC' if latest else ''

    query = """MATCH (a:twitter_user)-[:FOLLOWS]-(b:twitter_user) WITH a, COUNT(*) as c
        WHERE c < a.friends_count/2 AND a.friends_count < {} AND a.followers_count < {} AND NOT EXISTS (a.protected)
        AND NOT EXISTS (a.defunct)
        RETURN a.screen_name
        ORDER BY a.last_scraped {} LIMIT {}""".format(max_friends, max_followers, desc, limit)

    with db.session() as session:
        with session.begin_transaction() as tx:
            result = tx.run(query)
            next_friends = [record.values()[0] for record in result]

    return next_friends


def nextFollowers(db, latest=False, max_friends=2000, max_followers=2000, limit=20):
    """ Return a list of non-supernode users who have fewer follower relationships than Twitter thinks they should."""
    desc = ' DESC' if latest else ''
    query = """MATCH (b:twitter_user)-[:FOLLOWS]-(a:twitter_user) WITH a, COUNT(*) as c
        WHERE c < a.followers_count/2 AND a.followers_count < {} AND a.friends_count < {} AND NOT EXISTS (a.protected)
        AND NOT EXISTS (a.defunct)
        RETURN a.screen_name
        ORDER BY a.last_scraped {} LIMIT {}""".format(max_followers, max_friends, desc, limit)

    with db.session() as session:
        with session.begin_transaction() as tx:
            result = tx.run(query)
            next_followers = [record.values()[0] for record in result]

    return next_followers


def nextTweets(db, latest=False, max_friends=2000, max_followers=2000, limit=20, max_tweets=3000):
    """ Return a list of non-supernode users who have fewer tweets than Twitter thinks they should."""
    desc = ' DESC' if latest else ''
    query = """MATCH (a:twitter_user) WHERE NOT (a)-[:TWEETED]->(:tweet) WITH a, COUNT(*) as c
        WHERE c < a.statuses_count AND c < {} AND a.followers_count < {} AND a.friends_count < {}
        AND NOT EXISTS (a.protected) AND NOT EXISTS (a.defunct)
        RETURN a.screen_name
        ORDER BY a.last_scraped {} LIMIT {}""".format(max_tweets, max_followers, max_friends, desc, limit)

    with db.session() as session:
        with session.begin_transaction() as tx:
            result = tx.run(query)
            next_tweets = [record.values()[0] for record in result]

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


def nextNearest(db, user, job, root_task, max_friends=2000, max_followers=2000, limit=20, max_tweets=2000, test=False):
    """Find the next user to retrieve friends, followers or tweets, closest to a given user."""
    cacheKey = '_'.join(['nextnearest', job, user, root_task])
    nextUserDump = cache.get(cacheKey).decode('utf-8')
    next_users = False
    if nextUserDump:
        try:
            next_users = json.loads(nextUserDump)
        except:
            next_users = []
    if next_users:
        logging.info('*** NEXT '+job+': '+', '.join(next_users)+' from '+user+' ***')
        next_user = next_users.pop(0)
        cache.set(cacheKey, json.dumps(next_users))
        return next_user

    query_str = "MATCH (a:twitter_user {{screen_name: '{}'}})-[:FOLLOWS]-(d:twitter_user)".format(user)
    query_str += ' MATCH (b:twitter_user)-[:FOLLOWS]-(d) WITH DISTINCT b '
    if job == 'friends':
        query_str += 'MATCH (b)-[:FOLLOWS]->(c:twitter_user) '
    if job == 'followers':
        query_str += 'MATCH (b)<-[:FOLLOWS]-(c:twitter_user) '
    if job == 'tweets':
        query_str += 'MATCH (b)-[:TWEETED]->(c:tweet) '
    query_str += 'WITH b, COUNT(c) AS n '
    query_str += 'WHERE b.friends_count < {} AND b.followers_count < {} ' \
                 'AND NOT EXISTS (b.protected) AND NOT EXISTS (b.defunct) '.format(max_friends, max_followers)
    if job == 'friends':
        query_str += 'AND n < b.friends_count/2 '
    if job == 'followers':
        query_str += 'AND n < b.followers_count/2 '
    if job == 'tweets':
        query_str += 'AND b.statuses_count > 0 AND n < b.statuses_count/2 AND n<{} '.format(max_tweets)
    query_str += 'RETURN b.screen_name ORDER BY b.{}_last_scraped LIMIT {}'.format(job, limit)

    logging.info('*** Looking for '+job+' for '+user+' ***')

    if test:
        return query_str

    query = query_str
    try:
        with db.session() as session:
            with session.begin_transaction() as tx:
                result = tx.run(query)
                next_users = [record.values()[0] for record in result]
    except:
        next_users = []

    if next_users:
        logging.info('*** NEXT '+job+': '+', '.join(next_users)+' from '+user+' ***')
        next_user = next_users.pop(0)
        cache.set(cacheKey, json.dumps(next_users))
        return next_user
    else:
        logging.info('No more '+job+' for '+user)

    return False
