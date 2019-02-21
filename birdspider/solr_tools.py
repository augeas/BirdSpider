# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt

import json
import logging
import requests

from datetime import datetime
from db_settings import solrURL


update_url = '/'.join([solrURL, "update/json/docs?commitWithin=1000"])


def tweets2Solr(tweets):
    started = datetime.now()
    json_dump = '\n'.join(map(json.dumps, tweets))
    resp = requests.post(update_url, headers={"Content-Type": "application/json"}, data=json_dump)
    how_long = (datetime.now() - started).seconds
    if resp.status_code != 200:
        logging.warning("*** Can't push Solr docs... ***")
    else:
        logging.info('*** PUSHED %d TWEETS TO SOLR IN %ds ***' % (len(tweets), how_long))

