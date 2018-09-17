from datetime import datetime
from db_settings import get_neo_driver


#[['picopony', 'Dino_Pony', 'SilkyRaven', 'hartclaudia1'],['squirmelia','crispjodi','kingseesar','augeas','victoria_dft','matthewchilton']]
# design of neo graph nodes and relationships for clusters
# node of label type clustering
# when done
# who around
# adjacency matrix criteria (friends-followers mutual follow ;  mutual retweet ; mutual mention ; etc)
# indiv clusters have rel to clustering node (clustered_by?? )
# indiv users have member of rel to clusters (member of cluster)
# users are members of a clusters, clusters belong to a clustering session
def user_clusters_to_neo(labelled_clusters, seed_user, adjacency_criteria):
    clustering_id = clustering_to_neo(seed_user, 'twitter_user', 'screen_name', adjacency_criteria)

    # create cluster with relation 'clustered_by' linking it to Clustering
    # cluster<-member_of-clustering
    # cluster has one property, size
    
    neoDb = get_neo_driver()
    
    cluster_match = "MATCH (a:clustering) WHERE ID(a) = {}".format(clustering_id)
    create = " CREATE (b:cluster {size: $size})-[:CLUSTERED_BY]->(a) RETURN id(b)"
    clustered_by_query = ' '.join([cluster_match, create])
    for cluster in labelled_clusters:
        with neoDb.session() as session:
            with session.begin_transaction() as tx:
                cluster_id = tx.run(clustered_by_query, size=len(cluster)).single().value()

        # match screen_names to users, add relation 'member_of'
        match = "MATCH (m:twitter_user {screen_name: d}), (c:cluster) WHERE ID(c) = {}".format(cluster_id)
        # user-member_of->cluster
        merge = "MERGE (m)-[:MEMBER_OF]->(c)"
        relation_query = '\n'.join(['UNWIND {data} AS d', match, merge])
        with neoDb.session() as session:
            with session.begin_transaction() as tx:
                tx.run(relation_query, data=cluster)

    neoDb.close()

def clustering_to_neo(seed, seed_type, seed_id_label, adjacency_criteria):
    started = datetime.now()
    right_now = started.isoformat()

    # push new node to neo4j
    clustering_data = {'timestamp': right_now, 'adjacency_criteria': adjacency_criteria}
    create_query = '''UNWIND {data} AS d
        CREATE (a:Clustering {timestamp: d.timestamp, adjacency_criteria: d.adjacency_criteria})
        RETURN id(a)'''

    neoDb = get_neo_driver()

    with neoDb.session() as session:
        with session.begin_transaction() as tx:
            clustering_id = tx.run(create_query, data=clustering_data).single().value()

    # relationship: seed--seed_for-->clustering
    match = "MATCH (c:clustering), (s:{} {{{}: d}}) WHERE ID(c) = {}".format(seed_type, seed_id_label, clustering_id)

    merge = "MERGE (s)-[:SEED_FOR]->(c)"

    relation_query = '\n'.join(['UNWIND {data} AS d', match, merge])
    with neoDb.session() as session:
        with session.begin_transaction() as tx:
            tx.run(relation_query, data=seed)

    neoDb.close()

    return clustering_id
