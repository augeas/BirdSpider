
# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt
""" Adjacency matrices for various queries relating to Twitter. """
import re


def twitterFofQuery(user):
    fofQuery = """MATCH (a:twitter_user {screen_name:'SCREEN_NAME'})-[:FOLLOWS]->(b:twitter_user) WITH b
    MATCH (c:twitter_user)-[:FOLLOWS]->(b:twitter_user) WITH DISTINCT c
    MATCH (c)-[:FOLLOWS]->(d:twitter_user) RETURN DISTINCT c.screen_name,COLLECT(d.screen_name)"""
    return re.sub(r'SCREEN_NAME', user, fofQuery)

def twitterTransFofQuery(user):
    fofQuery = """MATCH (a:twitter_user {screen_name:'SCREEN_NAME'})-[:FOLLOWS]->(b:twitter_user)-[:FOLLOWS]->(a) WITH b
    MATCH (c:twitter_user)-[:FOLLOWS]->(b:twitter_user)-[:FOLLOWS]->(c)
    RETURN DISTINCT b.screen_name,COLLECT(c.screen_name)"""
    return re.sub(r'SCREEN_NAME', user, fofQuery)

def twitterMatrix(db, query):
    """Run a Cypher query that returns pairs of Twitter screen_names lists of others to which they are linked."""

    def matrix_query_as_list(tx):
        return list(tx.run(query))

    with db.session() as session:
        result = session.read_transaction(matrix_query_as_list)

    screen_names = [record[0] for record in result if record[0]]
    name_set = set(screen_names)

    def get_row(row):
        row_names = set(result[row][1]).intersection(name_set)
        return [float(1 & (i in row_names)) for i in screen_names]

    return screen_names, [get_row(i) for i in range(len(screen_names))]
    



