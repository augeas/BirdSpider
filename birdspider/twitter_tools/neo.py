
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

def pushUsers2Neo(renderedTwits):
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
        
def pushConnections2Neo(user, renderedTwits, friends=True):
    """Add friend/follower relationships between an existing user node with screen_name <user> and
    the rendered Twitter users."""
    started = datetime.now()
    rightNow = started.isoformat()
        
    pushUsers2Neo(renderedTwits)
    
    match = ("MATCH (t:twitter_user {{screen_name: '{}'}}),"
        +" (f:twitter_user {{screen_name: d.screen_name}})").format(user)

    if friends:
        merge = u"MERGE (t)-[:FOLLOWS]->(f)".format(user)
        update = "SET {}.friends_last_scraped = '{}'".format('t'+user,rightNow)
    else:
        merge = u"MERGE (f)-[:FOLLOWS]->(t)".format(user)
        update = "SET {}.followers_last_scraped = '{}'".format('t'+user,rightNow)
            
    query = u'\n'.join([u'UNWIND {data} AS d', match, merge])
    
    data = [{'screen_name':twit['screen_name']} for twit in renderedTwits]

    userNode = nodeRef(user, u'twitter_user', {u'screen_name':user})
    update_query = u'\n'.join([mergeNode(userNode, match=True), update])

    with neoDb.session() as session:
        with session.begin_transaction() as tx:
            tx.run(update_query)
            tx.run(query, data=data)
        