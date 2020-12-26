from pyspark import SparkContext
import sys
import json

if __name__ == '__main__':
    result = {}

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    partition_type = sys.argv[3]
    n_partitions = int(sys.argv[4])
    n = int(sys.argv[5])

    sc = SparkContext(appName="Task3")
    rddReviewJsonData = sc.textFile(input_file).map(lambda review: json.loads(review))
    rddReviewData = rddReviewJsonData.map(lambda review: (review["business_id"], 1))

    if partition_type == "default":
        result["n_partitions"] = rddReviewData.getNumPartitions()
        result["n_items"] = rddReviewData.glom().map(lambda partitionList: len(partitionList)).collect()
        result["result"] = rddReviewData.reduceByKey(lambda a, b: a + b).filter(lambda _: _[1] > n).collect()

    elif partition_type == "customized":
        rddReviewData = rddReviewData.partitionBy(n_partitions, partitionFunc=lambda key: hash(key))
        result["n_partitions"] = rddReviewData.getNumPartitions()
        result["n_items"] = rddReviewData.glom().map(lambda partitionList: len(partitionList)).collect()
        result["result"] = rddReviewData.reduceByKey(lambda a, b: a + b).filter(lambda _: _[1] > n).collect()

    with open(output_file, "w") as output:
        json.dump(result, output)
    output.close()
