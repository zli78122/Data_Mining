import sys
import math
import time
from pyspark import SparkContext
from itertools import combinations

sc = SparkContext(appName="Task1")


# generate all possible frequent item_set of size > two
def next_possible_candidates(previous_level_candidate):
    next_candidates = []
    for i in range(len(previous_level_candidate) - 1):
        for j in range(i + 1, len(previous_level_candidate)):
            a = previous_level_candidate[i]
            b = previous_level_candidate[j]
            if a[:-1] == b[:-1]:
                next_candidates.append(sorted(list(set(a).union(set(b)))))
            else:
                break
    return next_candidates


# get all candidates
def get_candidates(partition_index, baskets, support, total_basket_num):
    index = 1
    all_candidates = {}

    # cur_partition_support: the support in current partition
    # once a item_set >= cur_partition_support, the item_set can be potentially regarded as frequent item_set
    cur_partition_support = math.ceil(support * len(baskets) / total_basket_num)

    # Step 1. compute all frequent item_set of size one
    candidate_one = []
    candidate_one_dict = dict()
    for basket in baskets:
        for e in basket:
            if e in candidate_one_dict:
                candidate_one_dict[e] += 1
            else:
                candidate_one_dict[e] = 1
    for k, v in sorted(candidate_one_dict.items()):
        if v >= cur_partition_support:
            candidate_one.append(k)
    if len(candidate_one) == 0:
        return [inner for outer in all_candidates.values() for inner in outer]
    else:
        candidate_one_tuple = []
        for num in candidate_one:
            candidate_one_tuple.append((num,))
        all_candidates[index] = candidate_one_tuple
        index += 1

    # Step 2. eliminate the items that are not in candidate_one
    candidate_one_set = set(candidate_one)
    for i in range(len(baskets)):
        baskets[i] = sorted(set(baskets[i]) & candidate_one_set)

    # Step 3. generate all possible frequent item_set of size two, then get all frequent item_set of size two
    # generate all possible frequent item_set of size two
    candidate_two = []
    possible_candidate_two_dict = {}
    for basket in baskets:
        for item_set in (combinations(basket, 2)):
            if item_set in possible_candidate_two_dict:
                possible_candidate_two_dict[item_set] += 1
            else:
                possible_candidate_two_dict[item_set] = 1
    # get all frequent item_set of size two
    for k, v in possible_candidate_two_dict.items():
        if v >= cur_partition_support:
            candidate_two.append(k)
    if len(candidate_two) == 0:
        return [inner for outer in all_candidates.values() for inner in outer]
    else:
        candidate_two = sorted(candidate_two)
        all_candidates[index] = candidate_two
        index += 1

    # Step 4. generate all possible frequent item_set of size > two, then get all frequent item_set of size > two
    last_candidate = candidate_two
    while len(last_candidate) > 1:
        # generate all possible frequent item_set of size > two
        next_possible_candidate = next_possible_candidates(last_candidate)
        # get all frequent item_set of size > two
        next_candidates = []
        for candidate in next_possible_candidate:
            count = 0
            for basket in baskets:
                if set(candidate).issubset(set(basket)):
                    count += 1
            if count >= cur_partition_support:
                next_candidates.append(tuple(candidate))
        if len(next_candidates) == 0:
            break
        next_candidates = sorted(next_candidates)
        all_candidates[index] = next_candidates
        index += 1
        last_candidate = next_candidates

    return [inner for outer in all_candidates.values() for inner in outer]


# check if the candidate is a true frequent item
def is_frequent_item(candidate, baskets, support):
    candidate = set(candidate)
    count = 0
    for basket in baskets:
        if candidate.issubset(set(basket)):
            count += 1
    return True if count >= support else False


# write data into output file
def write_file(output_file, candidate_business, frequent_items):
    with open(output_file, "w") as output:
        output.write("Candidates:\n")
        write_result(output, candidate_business)

        output.write("Frequent Itemsets:\n")
        write_result(output, frequent_items)
    output.close()


def write_result(output, data):
    length = len(data)
    for i in range(length):
        if len(data[i]) == 1:
            output.write(str(data[i])[:-2] + ")")
        else:
            output.write(str(data[i]))

        if i + 1 < length and len(data[i]) == len(data[i + 1]):
            output.write(",")
        else:
            output.write("\n\n")


if __name__ == '__main__':
    start_time = time.time()

    case_number = int(sys.argv[1])
    support = int(sys.argv[2])
    input_file = sys.argv[3]
    output_file = sys.argv[4]

    rddInputData = sc.textFile(input_file)
    if case_number == 1:
        # format input data: [[business1, business2], [business2, business3], [business3, business4]]
        rddInputData = rddInputData.map(lambda x: (x.split(",")[0], x.split(",")[1])) \
            .distinct() \
            .groupByKey().mapValues(lambda x: [e for e in x]) \
            .filter(lambda x: x[0] != "user_id") \
            .map(lambda x: x[1])

    elif case_number == 2:
        # format input data: [[user1, user2], [user2, user3], [user3, user4]]
        rddInputData = rddInputData.map(lambda x: (x.split(",")[1], x.split(",")[0])) \
            .distinct() \
            .groupByKey().mapValues(lambda x: [e for e in x]) \
            .filter(lambda x: x[0] != "business_id") \
            .map(lambda x: x[1])

    baskets = rddInputData.collect()
    total_basket_num = rddInputData.count()

    # get all candidates
    candidates = rddInputData \
        .mapPartitionsWithIndex(lambda index, x: get_candidates(index, list(x), support, total_basket_num)) \
        .distinct() \
        .sortBy(lambda x: (len(x), x)) \
        .collect()

    # get all true frequent items
    frequent_items = sc.parallelize(candidates) \
        .filter(lambda x: is_frequent_item(x, baskets, support)) \
        .collect()

    # write data into output file
    write_file(output_file, candidates, frequent_items)

    end_time = time.time()
    print('Duration: %.2f' % (end_time - start_time))
