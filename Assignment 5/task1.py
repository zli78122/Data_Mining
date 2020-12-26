import sys
import json
import binascii
from time import time
from pyspark import SparkConf, SparkContext


def do_predict(cur_number, hash_func_number, hash_function, hash_value_set):
    for i in range(hash_func_number):
        if hash_function(i, cur_number) not in hash_value_set:
            return "F"
    return "T"


if __name__ == '__main__':
    start_time = time()

    # publicdata/business_first.json publicdata/business_second.json output/output1.txt
    first_json_path = sys.argv[1]
    second_json_path = sys.argv[2]
    output_filename = sys.argv[3]

    conf = SparkConf().setMaster("local[3]").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
    sc = SparkContext(appName="TASK1", conf=conf)
    sc.setLogLevel("WARN")

    # convert the name of a business into an integer
    name_integer_rdd = sc.textFile(first_json_path).map(lambda x: json.loads(x)["name"]) \
        .map(lambda x: int(binascii.hexlify(x.encode('utf8')), 16))

    # hash function arguments
    hash_func_number = 1
    a_arguments = [2058299889432823652]
    b_arguments = [4327896696933661215]
    m = 1999999999999999999

    # define hash function
    hash_function = lambda index, x: (a_arguments[index] * x + b_arguments[index]) % m

    # get all hash values
    hash_value_rdd = name_integer_rdd.flatMap(lambda x: [hash_function(i, x) for i in range(hash_func_number)]).distinct()
    hash_value_set = set(hash_value_rdd.collect())

    # estimate whether the coming name appeared in the first input file
    result = sc.textFile(second_json_path).map(lambda x: json.loads(x)["name"]) \
        .map(lambda x: int(binascii.hexlify(x.encode('utf8')), 16)) \
        .map(lambda x: do_predict(x, hash_func_number, hash_function, hash_value_set)) \
        .collect()

    # print result
    with open(output_filename, "w+") as output:
        for index, elem in enumerate(result):
            if index == len(result) - 1:
                output.write(elem)
            else:
                output.write(elem + " ")
    output.close()

    print('Duration: %.2f' % (time() - start_time))
