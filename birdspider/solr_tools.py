
import json
import logging
import requests

from datetime import datetime
from db_settings import solrURL


update_url = '/'.join([solrURL,"update/json/docs?commitWithin=1000"])


def tweets2Solr(tweets):
    started = datetime.now()
    json_dump = '\n'.join(map(json.dumps, tweets))
    resp = requests.post(update_url, headers={"Content-Type":"application/json"}, data=json_dump)
    howLong = (datetime.now() - started).seconds
    if resp.status_code != 200:
        logging.warn("*** Can't push Solr docs... ***")
    else:
        logging.info('*** PUSHED %d TWEETS TO SOLR IN %ds ***' % (len(tweets),howLong))

