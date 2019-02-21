
# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt


from datetime import datetime

import logging
import networkx as nx
import numpy as np

__vsmall__ = 0.0001
__nearly1__ = 0.95


def clusterize(matrix, inflate=1.5):
    """Cluster an adjacency matrix by MCL: http://www.micans.org/mcl/"""
    start = datetime.now()
    
    mat = np.array(matrix)
    np.fill_diagonal(mat, 1.0)
    mat = np.nan_to_num(mat / np.sum(mat, 0))
    dim = len(matrix)

    iterations = 1
    converged = False

    while not converged:
        
        new_mat = (np.dot(mat, mat)**inflate)
        new_mat = np.nan_to_num(new_mat / np.sum(new_mat, 0))
        diff = np.fabs(mat-new_mat)
        dev = np.std(diff)
                
        output = 'Iteration: '+str(iterations)
        if dev > __vsmall__:
            output += " No convergence. Deviation: "+str(dev)
            mat = new_mat
            iterations += 1
        else:
            output += ' Converged in ' + str((datetime.now()-start).seconds) + ' seconds.'
            converged = True
        logging.info(output)
 
    labs = np.array(list(range(dim)))
  
    cluster_lists = [list(j) for j in [labs[new_mat[i] > __vsmall__] for i in range(dim)] if j.shape[0] > 2]

    cluster_ref = {}
    for i, clust in enumerate(cluster_lists):
        this_cluster = i+1
        for j in clust:
            cluster_ref[j] = this_cluster
            
    return cluster_lists, cluster_ref


def labelClusters(clusters, labs):
    unique_clusters = []
    cluster_sets = {}
    for c in clusters:
        size = len(c)
        this_set = set(c)
        set_list = cluster_sets.get(size, False)
        if set_list:
            not_there = True
            for i in set_list:
                if i == this_set:
                    not_there = False
                    break
            if not_there:
                unique_clusters.append(c)
                set_list.append(this_set)
        else:
            unique_clusters.append(c)
            cluster_sets[size] = [this_set]
             
    return [[labs[i] for i in c] for c in unique_clusters if len(c) > 3]


def buildgraph(matrix, labels=False, clusters={}, clustermode=False):
    
    if clustermode:
        cluster_labeler = lambda x: x
    else:
        cluster_labeler = lambda x: True
    
    G = nx.Graph()
    dim = len(matrix)
    
    if not labels:
        G.add_nodes_from(list(range(dim)))
    else:
        for i, lab in enumerate(labels):
            G.add_node(i, label=lab)
            
    if clusters:
        for i in range(dim):
            cluster = clusters.get(i,False)
            if cluster:
                G.node[i]['cluster'] = cluster_labeler(cluster)
                
    for i in range(dim):
        G.add_edges_from([(i, j) for j in range(dim) if matrix[i][j]])

#    if clusters:
#        for i in range(dim):
#            for j in range(dim):
        
    return G
