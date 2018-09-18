from celery import chain, group

from app import app
from clustering.twitter_matrices import twitterMatrix, twitterTransFofQuery, twitterFofQuery
from clustering.matrix_tools import clusterize, labelClusters
from clustering.neo import user_clusters_to_neo
from db_settings import get_neo_driver

@app.task
def cluster(seed, seed_type, query_name):

    if seed_type == 'twitter_user':
        seed_id_name = 'screen_name'
        if query_name == "TransFoF":
            query = twitterTransFofQuery(seed)
        elif query_name == 'FoF':
            query = twitterTransFofQuery(seed)
        else:
            print('*** clustering not yet implemented for seed type ***')
            return
    else:
        print('*** clustering not yet implemented for seed type ***')
        return
    matrix_labels_and_results = twitter_matrix_for_clustering(query)

    cluster_results = clusterize(matrix_labels_and_results[1])

    labelled_clusters = labelClusters(cluster_results[0], matrix_labels_and_results[0])

    if seed_type == 'twitter_user':
        push_user_clusters_to_neo(labelled_clusters, [seed], query)
    else:
        print('*** clustering not yet implemented for seed type ***')


def twitter_matrix_for_clustering(query):
    db = get_neo_driver()
    matrix_results = twitterMatrix(db, query)
    db.close()

    return matrix_results


def push_user_clusters_to_neo(labelled_clusters, seeds, query):
    db = get_neo_driver()
    user_clusters_to_neo(db, labelled_clusters, seeds, query)
    db.close()
