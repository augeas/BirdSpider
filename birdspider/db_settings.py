# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt
from py2neo import neo4j
import redis

cache = redis.StrictRedis()

NeoURL = "http://neo4j:7474/db/data/"
neoDb = neo4j.GraphDatabaseService(NeoURL)

solrURL = 'http://solr:8983/solr/'
