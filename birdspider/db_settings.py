# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt
from neo4j.v1 import GraphDatabase
import redis

cache = redis.StrictRedis(host='redis')

uri = "bolt://neo4j:7687"
neoDb = GraphDatabase.driver(uri)


solrURL = 'http://solr:8983/solr/'
