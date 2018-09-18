
from datetime import datetime
import logging
import re
import time

noSlash = re.compile(r'\\')


def cypherVal(val):
    """Escape quotes and slashes for use in Cypher queries."""    
    if isinstance(val, (int, bool)):
        return str(val).lower()
    else:
        # Escape all the backslashes.
        escval = re.sub(noSlash,r'\\\\',val)
        escval = str(re.sub("'","\\'",escval))
        escval = str(re.sub('"','\\"',escval))
        escval = str(re.sub("\n","\\\\n",escval))
        return "'"+escval+"'"


def nodeRef(name, label, item):
    properties = ' { ' + ', '.join([': '.join([prop, cypherVal(val)]) for prop, val in list(item.items())]) + ' }'
    return '(' + str('t'+name) + ': ' + str(label) + properties + ')'


def mergeNode(*nodes, **kw):
    if kw.get('match', False):
        action = 'MATCH '
    else:
        action = 'MERGE '
    return action + ','.join(nodes)


def mergeRel(src, rel, dest):
    return 'MERGE ({})-[:{}]->({})'.format('t'+src, rel, 't'+dest)


def unwind_query(*clauses):
    return '\n'.join(['UNWIND {data} AS d'] + list(clauses))


def neo_tx(db, query, data=None):
    with db.session() as session:
        success = False
        tries = 0
        while not success and tries < 50:
            try:
                with session.begin_transaction() as tx:
                    if data == None:
                        tx.run(query)
                    else:
                        tx.run(query, data=data)
                success = True
            except:
                logging.warn('*** Neo tx failed, attempt %d ***' % (tries+1,))
                tries += 1
                time.sleep(2)
                
    assert success
                
     
def unwind_tx(db, data, *clauses):
    query = unwind_query(*clauses)
    neo_tx(db, query, data)
     
     
def users2Neo(db, renderedTwits):
    """Store  a list of rendered Twitter users in Neo4J. No relationships are formed."""
    started = datetime.now()
    rightNow = started.isoformat()
    
    for twit in renderedTwits:
        twit['last_scraped'] = rightNow
            
    data = [{'screen_name': twit.get('screen_name',False), 'props':twit}
        for twit in renderedTwits if twit.get('screen_name',False)]
    
    unwind_tx(db, data, 'MERGE (x:twitter_user {screen_name: d.screen_name})',
        'SET x += d.props')

    how_long = (datetime.now() - started).seconds
    logging.info(
        '*** PUSHED %d USERS TO NEO IN %ds ***' %
        (len(renderedTwits),how_long))    


def connections2Neo(db, user, renderedTwits, friends=True):
    """Add friend/follower relationships between an existing user node with screen_name <user> and
    the rendered Twitter users."""
    started = datetime.now()
    rightNow = started.isoformat()
        
    users2Neo(db, renderedTwits)
    
    match = ("MATCH (t:twitter_user {{screen_name: '{}'}}),"
        +" (f:twitter_user {{screen_name: d.screen_name}})").format(user)

    if friends:
        merge = "MERGE (t)-[:FOLLOWS]->(f)"
        update = "SET {}.friends_last_scraped = '{}'".format('t'+user,rightNow)
    else:
        merge = "MERGE (t)<-[:FOLLOWS]-(f)"
        update = "SET {}.followers_last_scraped = '{}'".format('t'+user,rightNow)
            
    query = '\n'.join(['UNWIND {data} AS d', match, merge])
    
    data = [{'screen_name':twit.get('screen_name',False)}
        for twit in renderedTwits if twit.get('screen_name',False)]

    userNode = nodeRef(user, 'twitter_user', {'screen_name':user})
    update_query = '\n'.join([mergeNode(userNode, match=True), update])

    neo_tx(db, update_query)
    neo_tx(db, query, data=data)

    how_long = (datetime.now() - started).seconds
    logging.info(
        '*** PUSHED %d CONNECTIONS TO NEO IN %ds ***' %
        (len(renderedTwits),how_long))

    
def tweets2Neo(db, renderedTweets, label='tweet'):
    started = datetime.now()
    tweets = (t[-1] for t in renderedTweets)

    data = [{'id':tweet['id'], 'props':tweet} for tweet in tweets]

    merge = "MERGE (x:{} {{id: d.id}})".format(label)
    update = "SET x += d.props"

    unwind_tx(db, data, merge, update)

    how_long = (datetime.now() - started).seconds
    logging.info(
        '*** PUSHED %d TWEETS TO NEO IN %ds ***' %
        (len(renderedTweets),how_long))

def tweetActions(db, user, renderedTweets, label='tweet'):
    started = datetime.now()
    
    actions = {'tweet':'TWEETED', 'retweet':'RETWEETED', 'quotetweet':'QUOTED'}

    match = ("MATCH (u:twitter_user {{screen_name: '{}'}})," +
        " (t:{} {{id_str: d.id_str}})").format(user, label)

    merge = "MERGE (u)-[:{}]->(t)".format(actions[label])

    tweets = (t[-1] for t in renderedTweets)
    data = [{'id_str':tweet['id_str']} for tweet in tweets]

    unwind_tx(db, data, match, merge)
    
    how_long = (datetime.now() - started).seconds
    logging.info(
        '*** PUSHED %d TWEET ACTIONS TO NEO IN %ds ***' %
        (len(renderedTweets),how_long))

def multi_user_tweet_actions(db, tweet_user_dump):
    started = datetime.now()
    match = ("MATCH (u:twitter_user {screen_name: d.name}), " +
        "(t:tweet {id_str: d.id})")
    
    merge = "MERGE (u)-[:TWEETED]->(t)"
        
    data = [{'name':user['screen_name'], 'id':id_str} for id_str,user in
        tweet_user_dump.items()]
    
    unwind_tx(db, data, match, merge)

    how_long = (datetime.now() - started).seconds
    logging.info(
        '*** PUSHED %d TWEET ACTIONS TO NEO IN %ds ***' %
        (len(data),how_long))
    
   
def tweetLinks(db, links,src_label,dest_label,relation):
    started = datetime.now()
    
    match = ("MATCH (src:{} {{id_str: d.src_id_str}}),"
        +" (dest:{} {{id_str: d.dest_id_str}})").format(src_label,dest_label)

    merge = "MERGE (src)-[:{}]->(dest)".format(relation)

    data = [{'src_id_str':src['id_str'], 'dest_id_str':dest['id_str']} for dest,src in links]
    
    unwind_tx(db, data, match, merge)
       
    how_long = (datetime.now() - started).seconds
    logging.info(
        '*** PUSHED %d TWEET LINKS TO NEO IN %ds ***' %
        (len(links),how_long))

entity_node_lables = {'hashtags': 'hashtag', 'urls':'url', 'media': 'media'}
entity_ids = {'hashtags': 'text', 'urls': 'expanded_url', 'media': 'id_str'}


def entities2neo(db, entities,entity_type):
    started = datetime.now()
    
    merge = "MERGE (x:{} {{id: d.id}})".format(entity_node_lables[entity_type])
    
    update = "SET x += d.props"
    
    id_field = entity_ids[entity_type]
    data = [{'id': e[id_field], 'props': e} for e in entities]

    unwind_tx(db, data, merge, update)

    how_long = (datetime.now() - started).seconds
    logging.info(
        '*** PUSHED %d TWEET ENTITIES TO NEO IN %ds ***' %
        (len(entities),how_long))


def entity_links(db, entities, relation, src_label, dest_label, src_prop, dest_prop):
    started = datetime.now()
    
    match = ("MATCH (src:{} {{{}:d.src}}), (dest:{} {{{}:d.dest}})").format(
    src_label,src_prop,dest_label,dest_prop)
    
    merge = "MERGE (src)-[:{}]->(dest)".format(relation)
    
    data = [{'src':src, 'dest':dest[dest_prop]} for (src,dest) in entities]

    unwind_tx(db, data, match, merge)
    
    how_long = (datetime.now() - started).seconds
    logging.info(
        '*** PUSHED %d ENTITY LINKS TO NEO IN %ds ***' %
        (len(entities),how_long))   

def tweetDump2Neo(db, user, tweetDump):
    """Store a rendered set of tweets by a given user in Neo4J.
       
    Positional arguments:
    user -- screen_name of the author of the tweets
    tweetDump -- tweets, retweets, mentions, hastags, URLs and replies from "decomposeTweets"

    """
    
    # user->[tweeted/RTed/quoted]->(tweet/RT/quoteTweet)
    for label in ['tweet', 'retweet', 'quotetweet']:
        if tweetDump[label]:
            tweets2Neo(db, tweetDump[label], label=label)
            tweetActions(db, user, tweetDump[label], label=label)
    
    # push original tweets from RTs/quotes
    for label in ['retweet', 'quotetweet']:
        tweets = [(tw[0],) for tw in tweetDump[label]]
        if tweets:
            tweets2Neo(db, tweets,label='tweet')
    
    # (RT/quote)-[RETWEET_OF/QUOTE_OF]->(tweet)
    if tweetDump['retweet']:
        tweetLinks(db, tweetDump['retweet'],'retweet','tweet','RETWEET_OF')
    if tweetDump['quotetweet']:
        tweetLinks(db, tweetDump['quotetweet'],'quotetweet','tweet','QUOTE_OF')

    # push users of original tweets.
    if tweetDump['users']:
        users2Neo(db, tweetDump['users'].values())
        multi_user_tweet_actions(db, tweetDump['users'])
    
    # mentions
    for label in ['tweet', 'retweet', 'quotetweet']:
        mentions = [m[1] for m in tweetDump['entities'][label]['user_mentions']]
        if mentions:
            users2Neo(db, mentions)
            entities = tweetDump['entities'][label]['user_mentions']
            entity_links(db, entities, 'MENTIONS', label, 'twitter_user', 'id_str', 'screen_name')

    # hashtags, urls and media
    for label in ['tweet', 'retweet', 'quotetweet']:
        for entity_type in ['hashtags', 'urls', 'media']:
            entities = [e[1] for e in tweetDump['entities'][label][entity_type]]
            if entities:
                entities2neo(db, entities,entity_type)

        if tweetDump['entities'][label]['hashtags']:
            entity_links(db, tweetDump['entities'][label]['hashtags'],
                'TAGGED', label, 'hashtag', 'id_str', 'text')
        
        if tweetDump['entities'][label]['urls']:
            entity_links(db, tweetDump['entities'][label]['urls'],
                'LINKS_TO', label, 'url', 'id_str', 'expanded_url')
        
        if tweetDump['entities'][label]['media']:
            entity_links(db, tweetDump['entities'][label]['media'],
                'EMBEDS', label, 'media', 'id_str', 'id_str')
        
        
def setUserDefunct(user):
    try:
        userNode = next(neoDb.find('twitter_user', property_key='screen_name', property_value=user))
    except:
        return
    userNode.update_properties({'defunct': 'true'})