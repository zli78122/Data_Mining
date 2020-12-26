import sys
import json
from itertools import combinations
from operator import add
from time import time
from pyspark import SparkContext


def min_hash(business_users, a_argument_list, hash_function):
    # [u1, u2, u3, ...]
    user_list = business_users[1]
    signature_matrix_column = []
    # execute outer for loop n times, compute signature_matrix[cur_business]
    for i in range(len(a_argument_list)):
        tmp = 9999999
        # execute inner for loop len(user_list) times, compute signature_matrix[cur_business][cur_hash_function]
        for user_index in user_list:
            tmp = min(hash_function(a_argument_list[i], user_index), tmp)
        signature_matrix_column.append(tmp)
    # return (cur_business, signature_matrix[cur_business])
    return business_users[0], signature_matrix_column


def LSH(signature_matrix_column, row, band):
    # n = 4, row = 2, band = 2
    # signature_matrix_column = (b, [h1, h2, h3, h4])
    # result                  = [((band1, sum(h1, h2)), [b]), ((band2, sum(h3, h4)), [b])]
    result = []
    business_index = signature_matrix_column[0]
    hash_values = signature_matrix_column[1]
    row_index = 0
    for band_index in range(band):
        one_band_hash_values = hash_values[row_index:row_index + row]
        row_index += row
        result.append(((band_index, sum(one_band_hash_values)), [business_index]))
    return result


def jaccard_similarity(pair, business_users_dict):
    user_set1 = set(business_users_dict[pair[0]])
    user_set2 = set(business_users_dict[pair[1]])
    similarity = float(len(user_set1.intersection(user_set2)) / len(user_set1.union(user_set2)))
    return pair, similarity


def convert_index_to_id(pair, index_business_dict):
    small = min(index_business_dict[pair[0]], index_business_dict[pair[1]])
    large = max(index_business_dict[pair[0]], index_business_dict[pair[1]])
    return small, large


if __name__ == '__main__':
    start_time = time()

    # dataset/train_review.json output/task1.res
    input_file = sys.argv[1]
    output_file = sys.argv[2]

    sc = SparkContext(appName="Task1")

    input_rdd = sc.textFile(input_file) \
        .map(lambda x: json.loads(x)) \
        .map(lambda x: (x["business_id"], x["user_id"]))

    business_index_rdd = input_rdd.map(lambda x: x[0]).distinct().zipWithIndex()
    business_index_dict = business_index_rdd.collectAsMap()
    index_business_dict = business_index_rdd.map(lambda x: (x[1], x[0])).collectAsMap()

    user_index_rdd = input_rdd.map(lambda x: x[1]).distinct().zipWithIndex()
    user_index_dict = user_index_rdd.collectAsMap()
    index_user_dict = user_index_rdd.map(lambda x: (x[1], x[0])).collectAsMap()

    # (b1, [u1, u2, u3, ...])
    business_users_rdd = input_rdd.map(lambda x: (business_index_dict[x[0]], [user_index_dict[x[1]]])) \
        .reduceByKey(add).map(lambda x: (x[0], list(set(x[1]))))
    # {b1 : [u1, u2, u3, ...]}
    business_users_dict = business_users_rdd.collectAsMap()

    n = 100
    row = 1
    band = int(n / row)
    a_argument_list = [665, 315, 490, 84, 280, 329, 266, 105, 56, 112, 28, 119, 553, 483, 308, 77, 441, 560, 70, 161,
                       336, 469, 210, 182, 217, 357, 63, 532, 196, 637, 203, 378, 175, 343, 525, 455, 518, 399, 434, 7,
                       287, 14, 476, 497, 616, 588, 658, 609, 546, 168, 364, 385, 574, 224, 623, 301, 133, 511, 602, 651,
                       686, 567, 504, 420, 245, 679, 98, 294, 91, 49, 371, 427, 238, 21, 672, 259, 693, 700, 462, 350,
                       595, 42, 630, 140, 581, 252, 322, 406, 147, 539, 273, 448, 413, 231, 644, 35, 189, 154, 126, 392]
    b = 13333
    p = 1333333
    # m = 26184
    m = user_index_rdd.count()

    def hash_function(argument_b, argument_p, argument_m):
        def hash_func(argument_a, argument_x):
            return ((argument_a * argument_x + argument_b) % argument_p) % argument_m
        return hash_func

    hash_function = hash_function(b, p, m)

    # min_hash : compute and get signature matrix
    # (b, [h1(b), h2(b), h3(b), ...])
    signature_matrix_rdd = business_users_rdd.map(lambda x: min_hash(x, a_argument_list, hash_function))

    # apply LSH algorithm
    # (b, [h1, h2, h3, h4]) => [((band1, sum(h1, h2)), [b]), ((band2, sum(h3, h4)), [b])]
    split_signature_matrix_rdd = signature_matrix_rdd.flatMap(lambda x: LSH(x, row, band))

    # Step 1. find all candidate pairs
    #     ((band1, sum(h1, h2)), [b1]) + ((band1, sum(h1, h2)), [b2])
    #     => ((band1, sum(h1, h2)), [b1, b2])
    #     => [b1, b2]
    # Step 2. compute the similarity of all candidate pairs
    #     [b1, b2] => ([b1, b2], similarity)
    # Step 3. filter all final business pairs with similarity >= 0.055
    # Step 4. convert business_index to business_id
    similar_business_pairs = split_signature_matrix_rdd.reduceByKey(lambda x, y: x + y) \
        .map(lambda x: x[1]) \
        .filter(lambda x: len(x) > 1) \
        .flatMap(lambda x: combinations(sorted(x), 2)) \
        .distinct() \
        .map(lambda x: jaccard_similarity(x, business_users_dict)) \
        .filter(lambda x: x[1] >= 0.055) \
        .map(lambda x: (convert_index_to_id(x[0], index_business_dict), x[1])) \
        .collect()

    # print result
    with open(output_file, "w+") as output:
        for index, elem in enumerate(similar_business_pairs):
            if index == len(similar_business_pairs) - 1:
                output.write(json.dumps({'b1': elem[0][0], 'b2': elem[0][1], 'sim': elem[1]}))
            else:
                output.write(json.dumps({'b1': elem[0][0], 'b2': elem[0][1], 'sim': elem[1]}) + "\n")
    output.close()

    end_time = time()

    print('Duration: %.2f' % (end_time - start_time))
    print("Accuracy Rate: %.2f" % (len(similar_business_pairs) / 37737))
