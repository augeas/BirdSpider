

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

def mergeNode(name, label, item):
    properties = u' { ' + u', '.join([u': '.join([prop, cypherVal(val)]) for prop, val in item.items()]) + u' }'
    return u'MERGE (' + unicode(name) + u': ' + unicode(label) + properties + u')'

def pushUsers2Neo(renderedTwits):
    """Store  a list of rendered Twitter users in Neo4J. No relationships are formed."""
    with neoDb.session() as session:
        with session.begin_transaction() as tx:
            for twit in renderedTwits:
                tx.run(mergeNode(twit['screen_name'], 'twitter_user', twit))