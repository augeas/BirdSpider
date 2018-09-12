
# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt

__author__ = 'Giles Richard Greenway'

from datetime import datetime, timedelta
import json

import redis

from db_settings import neoDb, cache

#TODO: make sure this query really does the following
# how this function works
# a user node has the frind anf follower counts as attributes (in json returned when you get the user,
#  so therefore in neo node added)
# if the count of fr and fol in the attributes of a node are more than half of actual num of relations in the graph
# then that is the node you pick to scrape next
# rephrase or rewrite?
# actual number of relationships is less than the count attribute
def nextFriends(latest=False):
    """ Return a list of non-supernode users who have fewer friend relationships than Twitter thinks they should."""
    desc = 'DESC' if latest else ''
    #TODO: make max freinds / folowers to be a god node configuarable (currently hc to 1000) make lLIMIT 2-0 configurabe
    #TODO: possibly this should not be C < a.freinds_count/2 but rather just  c < a.freinds_count, next to scrape should be oldest one with deficiency of freinds
    #
    query = """MATCH (a:twitter_user)-[:FOLLOWS]-(b:twitter_user) WITH a, COUNT(*) as c
        WHERE c < a.friends_count/2 AND a.friends_count < 1000 AND a.followers_count < 1000 AND NOT has (a.protected)
        AND NOT HAS (a.defunct)
        RETURN a.screen_name
        ORDER BY a.last_scraped {} LIMIT 20""".format(desc)
    #TODO run.tx with this query using new neo4j bindings, not using old py2neo stuff
    return [i.values[0] for i in query.execute().data]

def nextFollowers(latest=False):
    """ Return a list of non-supernode users who have fewer follower relationships than Twitter thinks they should."""
    desc = ' DESC' if latest else ''
    query = neo4j.CypherQuery(neoDb,'MATCH (b:twitter_user)-[:FOLLOWS]-(a:twitter_user) WITH a, COUNT(*) as c\n'
    +'WHERE c < a.followers_count/2 AND a.followers_count < 1000 AND a.friends_count < 1000 AND NOT has (a.protected) AND NOT HAS (a.defunct)\n'
    +'RETURN a.screen_name\n'
    +'ORDER BY a.last_scraped'+desc+'\n'
    +'LIMIT 20')
    return [ i.values[0] for i in query.execute().data ]

def nextTweets(latest=False):
    """ Return a list of non-supernode users who have fewer tweets than Twitter thinks they should."""
    desc = ' DESC' if latest else ''
    query = neo4j.CypherQuery(neoDb,'MATCH (a:twitter_user) WHERE NOT (a)-[:TWEETED]->(:tweet) WITH a, COUNT(*) as c\n'
    +'WHERE c < a.statuses_count AND c < 3000 AND a.followers_count < 1000 AND a.friends_count < 1000 AND NOT has (a.protected) AND NOT HAS (a.defunct)\n'                          
    +'RETURN a.screen_name\n'
    +'ORDER BY a.last_scraped'+desc+'\n'
    +'LIMIT 20')
    return [ i.values[0] for i in query.execute().data ]

def whoNext(job,latest=False):
    """Find the next user to retrieve friends, followers or tweets, closest to the initial seed of the network."""
    if job == 'friends':
        victimGetter = nextFriends
        
    if job == 'followers':
        victimGetter = nextFollowers

    if job == 'tweets':
        victimGetter = nextTweets
          
    victimList = False
    while not victimList:
        try:
            victimList = victimGetter(latest=latest)
        except:
            pass
        
    return victimList[0]

def nextNearest(user,job,test=False):
    """Find the next user to retrieve friends, followers or tweets, closest to a given user."""
    cacheKey = '_'.join(['nextnearest',job,user])
    nextUserDump = cache.get(cacheKey)
    nextUsers = False
    if nextUserDump:
        try:
            nextUsers = json.loads(nextUserDump)
        except:
            nextUsers = []
    if nextUsers:
        print('*** NEXT '+job+': '+', '.join(nextUsers)+' ***')
        nextUser = nextUsers.pop(0)
        cache.set(cacheKey,json.dumps(nextUsers))
        return nextUser
    
    queryStr = ('MATCH (a:twitter_user{{screen_name:"'+user+'"}})-[:FOLLOWS]-(d:twitter_user)').format()+'\n'
    queryStr += ' MATCH (b:twitter_user)-[:FOLLOWS]-(d) WITH DISTINCT b '
    if job == 'friends':
        queryStr += 'MATCH (b)-[:FOLLOWS]->(c:twitter_user) '
    if job == 'followers':
        queryStr += 'MATCH (b)<-[:FOLLOWS]-(c:twitter_user) '
    if job == 'tweets':
        queryStr += 'MATCH (b)-[:TWEETED]->(c:tweet) '
    queryStr += 'WITH b, COUNT(c) AS n\n'     
    queryStr += 'WHERE b.friends_count < 1000 AND b.followers_count < 1000 AND NOT has (b.protected) AND NOT HAS (b.defunct) '
    if job == 'friends':
        queryStr += 'AND n < b.friends_count/2\n'
    if job == 'followers':
        queryStr += 'AND n < b.followers_count/2\n'
    if job == 'tweets':
        queryStr += 'AND b.statuses_count > 0 AND n < b.statuses_count/2 AND n<1000 '
    queryStr += 'RETURN b.screen_name ORDER BY b.'+job+'_last_scraped LIMIT 20'

    print('*** Looking for '+job+' ***')
 
    if test:
        return queryStr
    
    query = neo4j.CypherQuery(neoDb,queryStr)
    try:
        nextUsers = [ i.values[0] for i in query.execute().data ]
    except:
        nextUsers = []
    
    if nextUsers:
        print('*** NEXT '+job+': '+', '.join(nextUsers)+' ***')
        nextUser = nextUsers.pop(0)
        cache.set(cacheKey,json.dumps(nextUsers))
        return nextUser
    else:
        print('No more '+job+' for '+user)
    
    return False
