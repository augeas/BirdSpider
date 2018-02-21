# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt
from py2neo import Graph
import redis

cache = redis.StrictRedis()

neoDb = Graph(host='neo4j', bolt=False)

solrURL = 'http://solr:8983/solr/'
