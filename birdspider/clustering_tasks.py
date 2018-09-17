from celery import chain, group

from app import app
from clustering.twitter_matrices import twitterMatrix, twitterTransFofQuery, twitterFofQuery
from clustering.matrix_tools import clusterize, labelClusters
from clustering.neo import user_clusters_to_neo
from db_settings import get_neo_driver

#TODO: redesign this task to call subtasks as proper celery tasks?
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
    matrix_results = twitterMatrix(query)

    cluster_results = clusterize(matrix_results[1])

    labelled_clusters = labelClusters(cluster_results[0], matrix_results[0])

    if seed_type == 'twitter_user':
        db = get_neo_driver()
        user_clusters_to_neo(labelled_clusters, [seed], query)
        db.close()
    else:
        print('*** clustering not yet implemented for seed type ***')