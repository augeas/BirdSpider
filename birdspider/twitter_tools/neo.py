
from datetime import datetime
import re

from db_settings import neoDb

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


def users2Neo(renderedTwits):
    """Store  a list of rendered Twitter users in Neo4J. No relationships are formed."""
    started = datetime.now()
    rightNow = started.isoformat()
    
    for twit in renderedTwits:
        twit['last_scraped'] = rightNow
            
    data = [{'screen_name': twit['screen_name'], 'props':twit} for twit in renderedTwits]
    
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
        merge = "MERGE (t)-[:FOLLOWS]->(f)"
        update = "SET {}.friends_last_scraped = '{}'".format('t'+user,rightNow)
    else:
        merge = "MERGE (t)<-[:FOLLOWS]-(f)"
        update = "SET {}.followers_last_scraped = '{}'".format('t'+user,rightNow)
            
    query = '\n'.join(['UNWIND {data} AS d', match, merge])
    
    data = [{'screen_name':twit['screen_name']} for twit in renderedTwits]

    userNode = nodeRef(user, 'twitter_user', {'screen_name':user})
    update_query = '\n'.join([mergeNode(userNode, match=True), update])

    with neoDb.session() as session:
        with session.begin_transaction() as tx:
            tx.run(update_query)
            tx.run(query, data=data)


def tweets2Neo(renderedTweets, label='tweet'):
    #started = datetime.now()
    #rightNow = started.isoformat()

    tweets = (t[-1] for t in renderedTweets)

    data = [{'id':tweet['id'], 'props':tweet} for tweet in tweets]

    merge = "MERGE (x:{} {{id: d.id}})".format(label)
    update = "SET x += d.props"

    query = '\n'.join(['UNWIND {data} AS d', merge, update])
        
    with neoDb.session() as session:
        with session.begin_transaction() as tx:
            tx.run(query, data=data)

def tweetActions(user, renderedTweets, label='tweet'):
    actions = {'tweet':'TWEETED', 'retweet':'RETWEETED', 'quotetweet':'QUOTED'}

    match = ("MATCH (u:twitter_user {{screen_name: '{}'}})," +
        " (t:{} {{id_str: d.id_str}})").format(user, label)

    merge = "MERGE (u)-[:{}]->(t)".format(actions[label])

    query = '\n'.join(['UNWIND {data} AS d', match, merge])

    tweets = (t[-1] for t in renderedTweets)
    data = [{'id_str':tweet['id_str']} for tweet in tweets]
    
    with neoDb.session() as session:
        with session.begin_transaction() as tx:
            tx.run(query, data=data)

def multi_user_tweet_actions(tweet_user_dump):
    match = ("MATCH (u:twitter_user {screen_name: d.name}), " +
        "(t:tweet {id_str: d.id})")
    
    merge = "MERGE (u)-[:TWEETED]->(t)"
    
    query = '\n'.join(['UNWIND {data} AS d', match, merge])
    
    data = [{'name':user['screen_name'], 'id':id_str} for id_str,user in
        tweet_user_dump.items()]
    
    with neoDb.session() as session:
        with session.begin_transaction() as tx:
            tx.run(query, data=data)    


def tweetLinks(links,src_label,dest_label,relation):
    match = ("MATCH (src:{} {{id_str: d.src_id_str}}),"
        +" (dest:{} {{id_str: d.dest_id_str}})").format(src_label,dest_label)

    merge = "MERGE (src)-[:{}]->(dest)".format(relation)

    query = '\n'.join(['UNWIND {data} AS d', match, merge])

    data = [{'src_id_str':src['id_str'], 'dest_id_str':dest['id_str']} for dest,src in links]
    
    with neoDb.session() as session:

        with session.begin_transaction() as tx:
            tx.run(query, data=data)    


entity_node_lables = {'hashtags': 'hashtag', 'urls':'url', 'media': 'media'}
entity_ids = {'hashtags': 'text', 'urls': 'expanded_url', 'media': 'id_str'}


def entities2neo(entities,entity_type):    
    merge = "MERGE (x:{} {{id: d.id}})".format(entity_node_lables[entity_type])
    
    update = "SET x += d.props"
    
    id_field = entity_ids[entity_type]
    data = [{'id': e[id_field], 'props': e} for e in entities]

    query = '\n'.join(['UNWIND {data} AS d', merge, update])
        
    with neoDb.session() as session:
        with session.begin_transaction() as tx:
            tx.run(query, data=data)


def entity_links(entities, relation, src_label, dest_label, src_prop, dest_prop):
    match = ("MATCH (src:{} {{{}:d.src}}), (dest:{} {{{}:d.dest}})").format(
    src_label,src_prop,dest_label,dest_prop)
    
    merge = "MERGE (src)-[:{}]->(dest)".format(relation)
    
    query = '\n'.join(['UNWIND {data} AS d', match, merge])
    
    data = [{'src':src, 'dest':dest[dest_prop]} for (src,dest) in entities]
    
    with neoDb.session() as session:

        with session.begin_transaction() as tx:
            tx.run(query, data=data) 


def tweetDump2Neo(user, tweetDump):
    """Store a rendered set of tweets by a given user in Neo4J.
       
    Positional arguments:
    user -- screen_name of the author of the tweets
    tweetDump -- tweets, retweets, mentions, hastags, URLs and replies from "decomposeTweets"

    """
    
    # user->[tweeted/RTed/quoted]->(tweet/RT/quoteTweet)
    for label in ['tweet', 'retweet', 'quotetweet']:
        tweets2Neo(tweetDump[label], label=label)
        tweetActions(user, tweetDump[label], label=label)
    
    # push original tweets from RTs/quotes
    for label in ['retweet', 'quotetweet']:
        tweets = [(tw[0],) for tw in tweetDump[label]]
        tweets2Neo(tweets,label='tweet')
    
    # (RT/quote)-[RETWEET_OF/QUOTE_OF]->(tweet)
    tweetLinks(tweetDump['retweet'],'retweet','tweet','RETWEET_OF')
    tweetLinks(tweetDump['quotetweet'],'quotetweet','tweet','QUOTE_OF')

    # push users of original tweets.
    users2Neo(tweetDump['users'].values())
    multi_user_tweet_actions(tweetDump['users'])
    
    # mentions
    #for label in ['tweet', 'retweet', 'quotetweet']:
    #    mentions = [m[1] for m in tweetDump['entities'][label]['user_mentions']]
    #    users2Neo(mentions)
    #    entities = [(m[0],m[1]['screen_name']) for m in tweetDump['entities'][label]['user_mentions']]
    #    entity_links(entities,'MENTIONS',label,'twitter_user','id_str',
    #        'screen_name')

    for label in ['tweet', 'retweet', 'quotetweet']:
        for entity_type in ['hashtags', 'urls', 'media']:
            entities = [e[1] for e in tweetDump['entities'][label][entity_type]]
            entities2neo(entities,entity_type)

        entity_links(tweetDump['entities'][label]['hashtags'], 'TAGGED', label, 'hashtag', 'id_str', 'text')
        entity_links(tweetDump['entities'][label]['urls'], 'LINKS_TO', label, 'url', 'id_str', 'expanded_url')
        entity_links(tweetDump['entities'][label]['media'], 'EMBEDS', label, 'media', 'id_str', 'id_str')
        


def setUserDefunct(user):
    try:
        userNode = next(neoDb.find('twitter_user', property_key='screen_name', property_value=user))
    except:
        return
    userNode.update_properties({'defunct': 'true'})


#[['picopony', 'Dino_Pony', 'SilkyRaven', 'hartclaudia1'],['squirmelia','crispjodi','kingseesar','augeas','victoria_dft','matthewchilton']]
# design of neo graph nodes and relationships for clusters
# node of label type clustering
# when done
# who around
# adjacency matrix criteria (friends-followers mutual follow ;  mutual retweet ; mutual mention ; etc)
# indiv clusters have rel to clustering node (clustered_by?? )
# indiv users have member of rel to clusters (member of cluster)
# users are members of a clusters, clusters belong to a clustering session
def user_clusters_to_neo(labelled_clusters, seed_user, adjacency_criteria):
    clustering_id = clustering_to_neo(seed_user, 'twitter_user', 'screen_name', adjacency_criteria)

    # create cluster with relation 'clustered_by' linking it to Clustering
    # cluster<-member_of-clustering
    # cluster has one property, size
    cluster_match = ("MATCH (a:clustering) WHERE ID(a) = {}").format(clustering_id)
    create = " CREATE (b:cluster {size: $size})-[:CLUSTERED_BY]->(a) RETURN id(b)"
    clustered_by_query = ' '.join([cluster_match, create])
    for cluster in labelled_clusters:
        with neoDb.session() as session:
            with session.begin_transaction() as tx:
                cluster_id = tx.run(clustered_by_query, size=len(cluster)).single().value()

        # match screen_names to users, add relation 'member_of'
        match = ("MATCH (m:twitter_user {screen_name: d}), "
                 + "(c:cluster) WHERE ID(c) = {}").format(cluster_id)
        # user-member_of->cluster
        merge = "MERGE (m)-[:MEMBER_OF]->(c)"
        relation_query = '\n'.join(['UNWIND {data} AS d', match, merge])
        with neoDb.session() as session:
            with session.begin_transaction() as tx:
                tx.run(relation_query, data=cluster)


def clustering_to_neo(seed, seed_type, seed_id_label, adjacency_criteria):
    started = datetime.now()
    rightNow = started.isoformat()

    #push new node to neo4j
    clustering_data = {'timestamp': rightNow, 'adjacency_criteria': adjacency_criteria}
    create_query = '''UNWIND {data} AS d
        CREATE (a:Clustering {timestamp: d.timestamp, adjacency_criteria: d.adjacency_criteria})
        RETURN id(a)'''

    with neoDb.session() as session:
        with session.begin_transaction() as tx:
            clustering_id = tx.run(create_query, data=clustering_data).single().value()

    #relationship: seed--seed_for-->clustering
    match = ("MATCH (c:clustering),"
             + " (s:{} {{{}: d}})"
             + " WHERE ID(c) = {}").format(seed_type, seed_id_label, clustering_id)


    merge = "MERGE (s)-[:SEED_FOR]->(c)"

    relation_query = '\n'.join(['UNWIND {data} AS d', match, merge])
    with neoDb.session() as session:
        with session.begin_transaction() as tx:
            tx.run(relation_query, data=seed)

    return clustering_id



