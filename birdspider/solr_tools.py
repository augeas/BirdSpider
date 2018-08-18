
import json
import requests

from datetime import datetime
from db_settings import solrURL

solrFields = {
    'doc_type': {'type':'string','stored':True},
    'tweet_text' : {'type':'text_en','stored':True, 'termVectors':True, 'termPositions':True, 'termOffsets':True},
    'tweet_time' : {'type':'date','stored':True}
}    

#solrDateFields = [ field if solrFields[field]['type'] == 'date' for field in solrFields.keys() ]

def addSolrFields():
    for fieldName in list(solrFields.keys()):
        resp = requests.put(solrURL+'schema/fields/'+fieldName,json.dumps(solrFields[fieldName]))
        if resp.status_code == 200:
            print('Added field: '+fieldName)
        else:
            print("Couldn't add field: '"+fieldName)


def addSolrDocs(docs):
    resp = requests.post(solrURL+'update/json?commit=true',data=json.dumps(docs),headers = {'content-type': 'application/json'})
    if resp.status_code != 200:
        print("*** Can't push Solr docs... ***")


def tweets2Solr(tweets):
    started = datetime.now()
    addSolrDocs([ {'doc_type':'tweet', 'id':tw['id_str'], 'tweet_text':tw['text'],  'tweet_time':tw['isotime']+'Z'} for tw in tweets ])
    howLong = (datetime.now() - started).seconds
    print('*** PUSHED '+str(len(tweets))+' TWEETS TO SOLR IN '+str(howLong)+'s ***')

