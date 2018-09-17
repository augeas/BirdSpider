# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt
from os import environ

from neo4j.v1 import GraphDatabase
import redis

try:
    cache = redis.StrictRedis(host='redis')
except:
    cache = None

uri = "bolt://neo4j:7687"

def get_neo_driver():
    neo_user = environ.get('NEO_USER', False)
    neo_pass = environ.get('NEO_PW', False)
    if neo_user and neo_pass:
        return GraphDatabase.driver(uri, auth=(neo_user, neo_pass))
    else:
        return None

solr_host = "birdspider_solr"

solr_core = "birdspider"

solrURL = "http://{}:8983/solr/{}".format(solr_host, solr_core)
