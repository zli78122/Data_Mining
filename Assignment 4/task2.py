import sys
import queue
from operator import add
from time import time
from itertools import combinations
from pyspark import SparkConf, SparkContext


# node_neighbors_dict = {node : [list of neighbors]}
def compute_betweenness(root_node, node_neighbors_dict):
    # Step 1. Perform a BFS of the graph, starting at root_node
    q = queue.Queue()
    q.put(root_node)
    # node_level_dict = {root : 0, n1 : 1, n2 : 1, n3 : 2, n4 : 2, n5 : 2, ...}
    node_level_dict = dict()
    node_level_dict[root_node] = 0
    # level_nodes_dict = {0 : [root], 1 : [n1, n2], 2 : [n3, n4, n5], ...}
    level_nodes_dict = dict()
    bfs_tree_height = 0

    while q.qsize() > 0:
        cur_node = q.get()
        cur_level = node_level_dict[cur_node]
        bfs_tree_height = max(cur_level, bfs_tree_height)
        if cur_level not in level_nodes_dict:
            level_nodes_dict[cur_level] = [cur_node]
        else:
            level_nodes_dict[cur_level].append(cur_node)
        for nei_node in node_neighbors_dict[cur_node]:
            if nei_node not in node_level_dict:
                node_level_dict[nei_node] = cur_level + 1
                q.put(nei_node)

    # Step 2. Label each node by the number of shortest paths that reach it from the root node
    # {node : the number of shortest paths that reach it from the root node}
    node_weight_dict = dict()
    # {node : [list of children]}
    node_children_dict = dict()

    for level in range(bfs_tree_height + 1):
        # traverse nodes in the same level
        for cur_node in level_nodes_dict[level]:
            node_children_dict[cur_node] = []
            if cur_node == root_node:
                node_weight_dict[cur_node] = 1
            else:
                node_weight_dict[cur_node] = 0
            # traverse neighbors of the cur_node
            for nei_node in node_neighbors_dict[cur_node]:
                if node_level_dict[cur_node] - 1 == node_level_dict[nei_node]:
                    # nei_node is a parent of cur_node
                    # node_weight_dict[cur_node] = sum of node_weight_dict[parent_node]
                    node_weight_dict[cur_node] += node_weight_dict[nei_node]
                elif node_level_dict[cur_node] + 1 == node_level_dict[nei_node]:
                    # nei_node is a child of cur_node
                    # node_children_dict[cur_node] = list of child_node
                    node_children_dict[cur_node].append(nei_node)

    # Step 3. Compute betweenness value of each edge
    # {node : credit}
    node_credit_dict = dict()
    # ((node1, node2), betweenness) <=> (edge, betweenness)
    edge_betweenness = []

    # traverse BFS tree from bottom up
    for level in range(bfs_tree_height, -1, -1):
        # traverse nodes in the same level
        for cur_node in level_nodes_dict[level]:
            node_credit_dict[cur_node] = 1
            if len(node_children_dict[cur_node]) > 0:
                # traverse children of the cur_node
                for child_node in node_children_dict[cur_node]:
                    cur_edge = (min(cur_node, child_node), max(cur_node, child_node))
                    # compute betweenness of the cur_edge
                    betweenness = node_credit_dict[child_node] * (node_weight_dict[cur_node] / node_weight_dict[child_node])
                    edge_betweenness.append((cur_edge, betweenness))
                    # update credit of the cur_node
                    node_credit_dict[cur_node] += betweenness

    # ((node1, node2), betweenness) <=> (edge, betweenness)
    return edge_betweenness


def jaccard_similarity(pair, user_states_dict):
    user_set1 = set(user_states_dict[pair[0]])
    user_set2 = set(user_states_dict[pair[1]])
    similarity = float(len(user_set1.intersection(user_set2)) / len(user_set1.union(user_set2)))
    return pair, similarity


def find_all_communities(new_node_neighbors_dict):
    community_list = []
    node_set = set(new_node_neighbors_dict.keys())

    while len(node_set) > 0:
        root_node = node_set.pop()
        q = queue.Queue()
        q.put(root_node)
        visited_node_set = {root_node}
        while q.qsize() > 0:
            cur_node = q.get()
            for nei_node in new_node_neighbors_dict[cur_node]:
                if nei_node not in visited_node_set:
                    node_set.remove(nei_node)
                    visited_node_set.add(nei_node)
                    q.put(nei_node)
        community_list.append(sorted(visited_node_set))

    return community_list


# community_list      : [[nodes in community_1], [nodes in community_2], [nodes in community_3], ...]
# node_neighbors_dict : {node : [list of neighbors]}
# m                   : edge number
# case_number         : 1 or 2
def compute_modularity(community_list, node_neighbors_dict, m, case_number):
    modularity = 0
    for community in community_list:
        for i, j in combinations(community, 2):
            Aij = 1 if j in node_neighbors_dict[i] else 0
            ki = len(node_neighbors_dict[i])
            kj = len(node_neighbors_dict[j])
            if case_number == 1:
                modularity += Aij - ki * kj / m
            elif case_number == 2:
                modularity += Aij - ki * kj / (2 * m)
    return modularity / (2 * m)


# edge_betweenness_list   : ((node1, node2), betweenness)
# new_node_neighbors_dict : {node : [list of neighbors]}, deep copy node_neighbors_dict
def remove_edges(edge_betweenness_list, new_node_neighbors_dict):
    highest_betweenness = edge_betweenness_list[0][1]
    cut_index = 0
    for i in range(len(edge_betweenness_list)):
        if i == len(edge_betweenness_list) - 1 and edge_betweenness_list[i][1] == highest_betweenness:
            cut_index = len(edge_betweenness_list)
            break
        if edge_betweenness_list[i][1] < highest_betweenness:
            cut_index = i
            break
    removed_edge_betweenness = edge_betweenness_list[0:cut_index]
    remain_edge_betweenness = edge_betweenness_list[cut_index:]

    for edge_betweenness in removed_edge_betweenness:
        node1 = edge_betweenness[0][0]
        node2 = edge_betweenness[0][1]
        new_node_neighbors_dict[node1].remove(node2)
        new_node_neighbors_dict[node2].remove(node1)

    return remain_edge_betweenness, new_node_neighbors_dict


if __name__ == '__main__':
    start_time = time()

    # 1 publicdata/user_state_synthetic.csv output/output2-case1-betweenness.txt output/output2-case1-community.txt
    # 2 publicdata/sample_user_state.csv output/output2-case2-betweenness.txt output/output2-case2-community.txt
    case_number = int(sys.argv[1])
    input_file_path = sys.argv[2]
    betweenness_output_file_path = sys.argv[3]
    community_output_file_path = sys.argv[4]

    conf = SparkConf().setMaster("local[3]").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
    sc = SparkContext(appName="TASK2", conf=conf)
    sc.setLogLevel("WARN")

    if case_number == 1:
        # (user, state) => (user, [state]), (state, [user]) => (u, [s1, s2, s3, ...]), (s, [u1, u2, u3, ...])
        node_neighbors_rdd = sc.textFile(input_file_path) \
            .filter(lambda x: x != "user_id,state") \
            .map(lambda x: (x.split(",")[0], x.split(",")[1])) \
            .distinct() \
            .flatMap(lambda x: [(x[0], [x[1]]), (x[1], [x[0]])]) \
            .reduceByKey(add)

        # {u : [s1, s2, s3, ...], s : [u1, u2, u3, ...]} <=> {node : [list of neighbors]}
        node_neighbors_dict = node_neighbors_rdd.collectAsMap()

    elif case_number == 2:
        # (user, state) => (u, [s1, s2, s3, ...])
        user_states_rdd = sc.textFile(input_file_path) \
            .filter(lambda x: x != "user_id,state") \
            .map(lambda x: (x.split(",")[0], [x.split(",")[1]])) \
            .reduceByKey(add) \
            .mapValues(lambda x: list(set(x)))

        # {u : [s1, s2, s3, ...]}
        user_states_dict = user_states_rdd.collectAsMap()

        # (u1, u2) => ((u1, u2), sim) => [(u1, [u2]), (u2, [u1])] => (u1, [u2, u3, u4, ...])
        node_neighbors_rdd = sc.parallelize(list(combinations(sorted(user_states_dict.keys()), 2))) \
            .map(lambda x: jaccard_similarity(x, user_states_dict)) \
            .filter(lambda x: x[1] >= 0.5) \
            .flatMap(lambda x: [(x[0][0], [x[0][1]]), (x[0][1], [x[0][0]])]) \
            .reduceByKey(add)

        # {u1 : [u2, u3, u4, ...]} <=> {node : [list of neighbors]}
        node_neighbors_dict = node_neighbors_rdd.collectAsMap()

    # Step 1. Betweenness Calculation
    # (node, [list of neighbors]) => node => ((node1, node2), betweenness)
    edge_betweenness_list = node_neighbors_rdd.map(lambda x: x[0]) \
        .flatMap(lambda x: compute_betweenness(x, node_neighbors_dict)) \
        .groupByKey() \
        .mapValues(lambda x: sum(x) / 2) \
        .sortBy(lambda x: (-x[1], x[0])) \
        .collect()

    with open(betweenness_output_file_path, "w+") as output:
        for edge_betweenness in edge_betweenness_list:
            output.write(str(edge_betweenness)[1:-1] + "\n")
    output.close()

    # Step 2. Community Detection
    # edge number
    m = len(edge_betweenness_list)
    # highest modularity
    max_modularity = 0
    # community list of the highest modularity
    best_community_list = []
    # {node : [list of neighbors]}, deep copy node_neighbors_dict
    new_node_neighbors_dict = dict()
    for node, neighbors in node_neighbors_dict.items():
        neighbor_list = []
        for nei_node in neighbors:
            neighbor_list.append(nei_node)
        new_node_neighbors_dict[node] = neighbor_list

    while True:
        # community_list = [[nodes in community_1], [nodes in community_2], [nodes in community_3], ...]
        community_list = find_all_communities(new_node_neighbors_dict)

        # compute modularity for current community_list
        modularity = compute_modularity(community_list, node_neighbors_dict, m, case_number)

        if len(best_community_list) == 0 or modularity > max_modularity:
            max_modularity = modularity
            best_community_list = community_list

        if len(edge_betweenness_list) == 0:
            break

        # remove edges with highest betweenness
        edge_betweenness_list, new_node_neighbors_dict = remove_edges(edge_betweenness_list, new_node_neighbors_dict)

        # recompute betweenness
        edge_betweenness_list = node_neighbors_rdd.map(lambda x: x[0]) \
            .flatMap(lambda x: compute_betweenness(x, new_node_neighbors_dict)) \
            .groupByKey() \
            .mapValues(lambda x: sum(x) / 2) \
            .sortBy(lambda x: (-x[1], x[0])) \
            .collect()

    best_community_list = sorted(best_community_list, key=lambda x: (len(x), x))

    with open(community_output_file_path, "w+") as output:
        for edge_betweenness in best_community_list:
            output.write(str(edge_betweenness)[1:-1] + "\n")
    output.close()

    print('Duration: %.2f' % (time() - start_time))
