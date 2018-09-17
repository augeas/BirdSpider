# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt
from os import environ

from neo4j.v1 import GraphDatabase
import redis

try:
    cache = redis.StrictRedis(host='redis')
except:
    cache = None

uri = "bolt://neo4j:7687"

try:
    neoDb = GraphDatabase.driver(uri, auth=(environ['NEO_USER'], environ['NEO_PW']))
except:
    neoDb = None

solr_host = "birdspider_solr"

solr_core = "birdspider"

solrURL = "http://{}:8983/solr/{}".format(solr_host, solr_core)
