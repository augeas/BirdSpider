
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

def mergeNode(name, label, item, match=False):
    if match:
        action = u'MATCH ('
    else:
        action = u'MERGE ('
    properties = u' { ' + u', '.join([u': '.join([prop, cypherVal(val)]) for prop, val in item.items()]) + u' }'
    return action + unicode(name) + u': ' + unicode(label) + properties + u')'

def mergeRel(src, rel, dest):
    return 'MERGE ({})-[:{}]->({})'.format(src, rel, dest)

def pushUsers2Neo(renderedTwits):
    """Store  a list of rendered Twitter users in Neo4J. No relationships are formed."""
    started = datetime.now()
    rightNow = started.isoformat()
    for twit in renderedTwits:
        twit['last_scraped'] = rightNow
    query = '\n'.join([mergeNode(twit['screen_name'], 'twitter_user', twit) for twit in renderedTwits])
    with neoDb.session() as session:
        with session.begin_transaction() as tx:
            tx.run(query)
                
def pushConnections2Neo(user, renderedTwits, friends=True):
    started = datetime.now()
    rightNow = started.isoformat()
    
    if friends:
        link = lambda target: mergeRel(user, 'FOLLOWS', target)
        update = "SET {}.friends_last_scraped = '{}'".format(user,rightNow) 
    else:
        link = lambda target: mergeRel(target, 'FOLLOWS', user)
        update = "SET {}.followers_last_scraped = '{}'".format(user,rightNow)
    
    query = [mergeNode(user, 'twitter_user', {'screen_name':user}, match=True), update]
    
    for twit in renderedTwits:
        twit['last_scraped'] = rightNow
        query.append(mergeNode(twit['screen_name'], 'twitter_user', twit))
        query.append(link(twit['screen_name']))
    
    with neoDb.session() as session:
        with session.begin_transaction() as tx:
            tx.run('\n'.join(query))
    