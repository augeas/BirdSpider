
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
        " (t:{} {{id_str: t.id_str}})").format(user, label)

    merge = "MERGE (u)-[:{}]->(t)".format(actions[label])

    query = '\n'.join(['UNWIND {data} AS d', match, merge])

    tweets = (t[-1] for t in renderedTweets)
    data = [{'id_str':tweet['id_str']} for tweet in tweets]
    
    with neoDb.session() as session:
        with session.begin_transaction() as tx:
            tx.run(query, data=data)


def tweetLinks(links,src_label,dest_label,relation):
    
    match = ("MATCH (src:{} {{id_str: d.src_id_str}}),"
        +" (dst:{} {{id_str: d.dest_id_str}})").format(src_label,dest_label)

    merge = "MERGE (src)-[:{}]->(dst)".format(relation)

    query = '\n'.join(['UNWIND {data} AS d', match, merge])

    data = [{'src_id_str':src['id_str'], 'dest_id_str':dest['id_str']} for src,dest in links]
    
    with neoDb.session() as session:

        with session.begin_transaction() as tx:
            tx.run(query, data=data)    


def tweetDump2Neo(user, tweetDump):
    """Store a rendered set of tweets by a given user in Neo4J.
       
    Positional arguments:
    user -- screen_name of the author of the tweets
    tweetDump -- tweets, retweets, mentions, hastags, URLs and replies from "decomposeTweets"

    """
    
    for label in ['tweet', 'retweet', 'quotetweet']:
        tweets2Neo(tweetDump[label], label=label)
        tweetActions(user, tweetDump['label'], label=label)
        
    for label in ['retweet', 'quotetweet']:
        tweets = [(tw[0],) for tw in tweetDump[label]]
        tweets2Neo(tweets,label='tweet')
        
    tweetLinks(tweetDump['retweet'],'retweet','tweet','RETWEETED_IN')
    tweetLinks(tweetDump['quotetweet'],'quotetweet','tweet','QUOTED_IN')

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
    cluster_match = ("MATCH (a:Clustering) WHERE ID(a) = {}").format(clustering_id)
    create = " CREATE (b:Cluster {size: $size})-[:CLUSTERED_BY]->(a) RETURN id(b)"
    clustered_by_query = ' '.join([cluster_match, create])
    for cluster in labelled_clusters:
        with neoDb.session() as session:
            with session.begin_transaction() as tx:
                cluster_id = tx.run(clustered_by_query, size=len(cluster)).single().value()

        # match screen_names to users, add relation 'member_of'
        match = ("MATCH (m:twitter_user {screen_name: d}), "
                 + "(c:Cluster) WHERE ID(c) = {}").format(cluster_id)
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
    match = ("MATCH (c:Clustering),"
             + " (s:{} {{{}: d}})"
             + " WHERE ID(c) = {}").format(seed_type, seed_id_label, clustering_id)


    merge = "MERGE (s)-[:SEED_FOR]->(c)"

    relation_query = '\n'.join(['UNWIND {data} AS d', match, merge])
    with neoDb.session() as session:
        with session.begin_transaction() as tx:
            tx.run(relation_query, data=seed)

    return clustering_id



