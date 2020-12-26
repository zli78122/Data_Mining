import sys
from time import time
from itertools import combinations
from graphframes import GraphFrame
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext


# (b, [u1, u2, u3]) => [((u1, u2), b), ((u2, u1), b), ((u1, u3), b), ((u3, u1), b), ((u2, u3), b), ((u3, u2), b)]
def user_pair_business(business_id, user_list):
    result = []
    for u1, u2 in combinations(sorted(user_list), 2):
        result.append(((u1, u2), [business_id]))
        result.append(((u2, u1), [business_id]))
    return result


# spark-submit --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 task1.py 7 publicdata/ub_sample_data.csv output/output1.txt
if __name__ == '__main__':
    start_time = time()

    # 7 publicdata/ub_sample_data.csv output/output1.txt
    filter_threshold = int(sys.argv[1])
    input_file_path = sys.argv[2]
    community_output_file_path = sys.argv[3]

    conf = SparkConf().setMaster("local[3]").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
    sc = SparkContext(appName="TASK1", conf=conf)
    sc.setLogLevel("WARN")
    sparkSession = SparkSession(sc)

    # (business, user) => (b, [u1, u2, u3, ...]) => ((u1, u2), b) => ((u1, u2), [b1, b2, b3, ...]) => (u1, u2)
    graph_edges_rdd = sc.textFile(input_file_path).map(lambda x: (x.split(",")[1], [x.split(",")[0]])) \
        .reduceByKey(lambda a, b: a + b) \
        .mapValues(lambda x: list(set(x))) \
        .filter(lambda x: x[0] != "business_id") \
        .flatMap(lambda x: user_pair_business(x[0], x[1])) \
        .reduceByKey(lambda a, b: a + b) \
        .filter(lambda x: len(x[1]) >= filter_threshold) \
        .map(lambda x: x[0])

    graph_vertices_rdd = graph_edges_rdd.flatMap(lambda x: [x[0], x[1]]).distinct().map(lambda x: (x,))

    # graph_frame = GraphFrame(vertices, edges)
    graph_frame = GraphFrame(graph_vertices_rdd.toDF(['id']), graph_edges_rdd.toDF(["src", "dst"]))

    # Label Propagation Algorithm (LPA)
    # communities = DataFrame[id: string, label: bigint]
    communities = graph_frame.labelPropagation(maxIter=5)

    # (id, label) => (label, id) => (label, [id_1, id_2, id_3, ...]) => [id_1, id_2, id_3, ...]
    result = communities.rdd.map(lambda x: (x[1], [x[0]])) \
        .reduceByKey(lambda a, b: a + b) \
        .mapValues(lambda x: sorted(x)) \
        .map(lambda x: x[1]) \
        .sortBy(lambda x: (len(x), x)) \
        .collect()

    with open(community_output_file_path, "w+") as output:
        for community in result:
            output.write(str(community)[1:-1] + "\n")
    output.close()

    print('Duration: %.2f' % (time() - start_time))
