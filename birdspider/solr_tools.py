
import json
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
        print("*** Can't push Solr docs... ***")
    else:
        print('*** PUSHED '+str(len(tweets))+' TWEETS TO SOLR IN '+str(howLong)+'s ***')

