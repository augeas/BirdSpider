
from datetime import datetime
import re

from db_settings import neoDb

noSlash = re.compile(r'\\')

def cypherVal(val):
    """Escape quotes and slashes for use in Cypher queries."""    
    if isinstance(val, (int, long, bool)):
        return unicode(val).lower()
    else:
        # Escape all the backslashes.
        escval = re.sub(noSlash,r'\\\\',val)
        escval = unicode(re.sub("'","\\'",escval))
        escval = unicode(re.sub('"','\\"',escval))
        escval = unicode(re.sub("\n","\\\\n",escval))
        return u"'"+escval+u"'"

def nodeRef(name, label, item):
    properties = u' { ' + u', '.join([u': '.join([prop, cypherVal(val)]) for prop, val in item.items()]) + u' }'
    return u'(' + unicode('t'+name) + u': ' + unicode(label) + properties + u')'

def mergeNode(*nodes, **kw):
    if kw.get('match', False):
        action = u'MATCH '
    else:
        action = u'MERGE '
    return action + u','.join(nodes)

def mergeRel(src, rel, dest):
    return 'MERGE ({})-[:{}]->({})'.format('t'+src, rel, 't'+dest)

def users2Neo(renderedTwits):
    """Store  a list of rendered Twitter users in Neo4J. No relationships are formed."""
    started = datetime.now()
    rightNow = started.isoformat()
    
    for twit in renderedTwits:
        twit['last_scraped'] = rightNow
            
    data = [{'screen_name':twit['screen_name'], 'props':twit} for twit in renderedTwits]
    
    query = '''UNWIND {data} AS d
        MERGE (x:twitter_user {screen_name: d.screen_name})
        SET x += d.props'''
        
    with neoDb.session() as session:
        with session.begin_transaction() as tx:
            tx.run(query, data=data)
        
def connections2Neo(user, renderedTwits, friends=True):
    """Add friend/follower relationships between an existing user node with screen_name <user> and
    the rendered Twitter users."""
    started = datetime.now()
    rightNow = started.isoformat()
        
    users2Neo(renderedTwits)
    
    match = ("MATCH (t:twitter_user {{screen_name: '{}'}}),"
        +" (f:twitter_user {{screen_name: d.screen_name}})").format(user)

    if friends:
        merge = u"MERGE (t)-[:FOLLOWS]->(f)"
        update = "SET {}.friends_last_scraped = '{}'".format('t'+user,rightNow)
    else:
        merge = u"MERGE (f)-[:FOLLOWS]->(t)"
        update = "SET {}.followers_last_scraped = '{}'".format('t'+user,rightNow)
            
    query = u'\n'.join([u'UNWIND {data} AS d', match, merge])
    
    data = [{'screen_name':twit['screen_name']} for twit in renderedTwits]

    userNode = nodeRef(user, u'twitter_user', {u'screen_name':user})
    update_query = u'\n'.join([mergeNode(userNode, match=True), update])

    with neoDb.session() as session:
        with session.begin_transaction() as tx:
            tx.run(update_query)
            tx.run(query, data=data)

def tweets2Neo(renderedTweets,label='tweet'):
    started = datetime.now()
    rightNow = started.isoformat()    

    tweets = (t[-1] for t in renderedTweets)

    data = ({'id':tweet['id'], 'props':tweet} for tweet in renderedTweets])

    query = '''UNWIND {data} AS d
        MERGE (x:{} {id: d.id})
        SET x += d.props'''.format(label)
        
    with neoDb.session() as session:
        with session.begin_transaction() as tx:
            tx.run(query, data=data)

def tweetActions(user,renderedTweets,label='tweet')
    actions = {'tweet':'TWEETED', 'retweet':'RETWEETED', 'quotetweet':'QUOTED'}

    match = (u"MATCH (u:twitter_user {{screen_name: '{}'}})," +
        u" (t:{} {{id_str: t.id_str}})").format(user, label)

    merge = u"MERGE (u)-[:{}]->(t)".format(actions['label'])

    query = u'\n'.join([u'UNWIND {data} AS d', match, merge])

    tweets = (t[-1] for t in renderedTweets)
    data = [{'id_str':tweet['id_str']} for tweet in tweets]
    
    with neoDb.session() as session:
        with session.begin_transaction() as tx:
            tx.run(query, data=data)

def tweetLinks(links,src_label,dest_label,relation):
    
    match = ("MATCH (s:{} {{id_str: d.src_id_str}}),"
        +" (d:{} {{id_str: d.dest_id_str}})").format(src_label,dest_label)

    merge = u"MERGE (s)-[:{}]->(d)".format(relation)

    query = u'\n'.join([u'UNWIND {data} AS d', match, merge])

    data = [{'src_id_str':src['id_str'], 'dest_id_str':dest['id_str']} for src,dest in links]
    
    with neoDb.session() as session:
        with session.begin_transaction() as tx:
            tx.run(query, data=data)    

def tweetDump2Neo(user,tweetDump):
    """Store a rendered set of tweets by a given user in Neo4J.
       
    Positional arguments:
    user -- screen_name of the author of the tweets
    tweetDump -- tweets, retweets, mentions, hastags, URLs and replies from "decomposeTweets"

    """
    
    for label in ['tweet', 'retweet', 'quotetweet']:
        tweets2Neo(tweetDump[label],label=label)
        tweetActions(user,tweetDump[label],label=label)
    
    
