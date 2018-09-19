from celery.utils.log import get_task_logger

from app import app
from clustering.twitter_matrices import twitterMatrix, twitterTransFofQuery, twitterFofQuery
from clustering.matrix_tools import clusterize, labelClusters
from clustering.neo import user_clusters_to_neo
from db_settings import get_neo_driver

logger = get_task_logger(__name__)


@app.task
def cluster(seed, seed_type, query_name):
    logger.info('*** START CLUSTERING: seed %s, seed_type %s, query_name %s ***' % (seed, seed_type, query_name))
    if seed_type == 'twitter_user':
        seed_id_name = 'screen_name'
        if query_name == "TransFoF":
            query = twitterTransFofQuery(seed)
        elif query_name == 'FoF':
            query = twitterTransFofQuery(seed)
        else:
            logger.warn('*** CLUSTERING:  not yet implemented for seed type %s ***' % seed_type)
            return
    else:
        logger.warn('*** CLUSTERING:  not yet implemented for seed type %s ***' % seed_type)
        return

    db = get_neo_driver()

    logger.info('*** CLUSTERING: get matrix for seed %s ***' % seed)
    matrix_labels_and_results = twitterMatrix(db, query)
    logger.info('*** CLUSTERING: find clusters  ***' )
    cluster_results = clusterize(matrix_labels_and_results[1])
    logger.info('*** CLUSTERING: label clusters ***' )
    labelled_clusters = labelClusters(cluster_results[0], matrix_labels_and_results[0])

    if seed_type == 'twitter_user':
        logger.info('*** CLUSTERING: push seed %s ***' % seed)
        user_clusters_to_neo(db, labelled_clusters, [seed], query)
    else:
        logger.warn('*** CLUSTERING: not yet implemented for seed type %s ***' % seed_type)

    db.close()
    logger.info('*** CLUSTERING FINISHED: seed %s, seed_type %s, query_name %s ***' % (seed, seed_type, query_name))
