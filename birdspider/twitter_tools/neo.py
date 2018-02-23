
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

def setNode(name, properties):
    tName = 't' + name
    return u'SET ' + u', '.join([u"{}.{} = {}".format(tName, prop, cypherVal(val))
        for prop,val in properties.items()])

def pushUsers2Neo(renderedTwits):
    """Store  a list of rendered Twitter users in Neo4J. No relationships are formed."""
    started = datetime.now()
    rightNow = started.isoformat()
    
    for twit in renderedTwits:
        twit['last_scraped'] = rightNow
        
    nodes = [nodeRef(twit['screen_name'], 'twitter_user', {'screen_name': twit['screen_name']}) for twit in renderedTwits]
    
    updates = ['\n'.join([mergeNode(node, match=True), setNode(twit['screen_name'], twit)])
            for node,twit in zip(nodes, renderedTwits)]
    
    query = '\n'.join([mergeNode(twit) for twit in nodes])
    with neoDb.session() as session:
        with session.begin_transaction() as tx:
            tx.run(query)
        with session.begin_transaction() as tx:
            for update_query in updates:
                tx.run(update_query)
                            
def pushConnections2Neo(user, renderedTwits, friends=True):
    """Add friend/follower relationships between an existing user node with screen_name <user> and
    the rendered Twitter users."""
    started = datetime.now()
    rightNow = started.isoformat()
        
    pushUsers2Neo(renderedTwits)
    
    if friends:
        link = lambda target: mergeRel(user, 'FOLLOWS', target)
        update = "SET {}.friends_last_scraped = '{}'".format('t'+user,rightNow) 
    else:
        link = lambda target: mergeRel(target, 'FOLLOWS', user)
        update = "SET {}.followers_last_scraped = '{}'".format('t'+user,rightNow)
    
    userNode = nodeRef(user, 'twitter_user', {'screen_name':user})
    
    update_query = '\n'.join([mergeNode(userNode, match=True), update])
    
    targetNodes = [nodeRef(twit['screen_name'], 'twitter_user', {'screen_name':twit['screen_name']}) for twit in renderedTwits]
        
    with neoDb.session() as session:
        with session.begin_transaction() as tx:
            tx.run(update_query)
        for node, twit in zip(targetNodes, renderedTwits):
            with session.begin_transaction() as tx:
                query = '\n'.join([mergeNode(userNode, node, match=True), link(twit['screen_name'])])
                tx.run(query)
    