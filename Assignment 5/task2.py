import sys
import json
import time
import math
import binascii
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext


def Flajolet_Martin(stream_data, hash_func_number, hash_function, group, output_filename):
    if stream_data.count() == 0:
        return

    state_number_set = set(stream_data.collect())

    # estimate counts, i.e. produce hash_func_number samples
    estimate_count_list = []
    for i in range(hash_func_number):
        cur_R = 0
        for state_number in state_number_set:
            hash_value = hash_function(i, state_number)
            for bit in range(32):
                if (hash_value >> bit) & 1 == 1:
                    cur_R = max(cur_R, bit)
                    break
        estimate_count_list.append(math.pow(2, cur_R))

    # partition samples into small groups, and take the average of groups
    avg_estimate_count_list = []
    group_size = int(hash_func_number / group)
    for i in range(group):
        group_element_list = estimate_count_list[i * group_size:(i + 1) * group_size]
        avg_estimate_count_list.append(sum(group_element_list) / len(group_element_list))

    # take the median of the averages
    avg_estimate_count_list = sorted(avg_estimate_count_list)
    estimate_count = int(round(avg_estimate_count_list[int(group / 2)]))

    result = str(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())) + "," + str(len(state_number_set)) + "," + str(estimate_count) + "\n"

    with open(output_filename, "a+") as output:
        output.write(result)
    output.close()


# java -cp publicdata/generate_stream.jar StreamSimulation publicdata/business.json 9999 100
if __name__ == '__main__':
    # 9999 output/output2.csv
    port = int(sys.argv[1])
    output_filename = sys.argv[2]

    conf = SparkConf().setMaster("local[3]").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
    sc = SparkContext(appName="TASK2", conf=conf)
    sc.setLogLevel("ERROR")

    # hash function arguments
    hash_func_number = 300
    group = 75
    a_arguments = [26773, 28728, 36928, 893, 41533, 55230, 15324, 59366, 23719, 53032, 54728, 21007, 53283, 39072, 15906,
                   9377, 1465, 6534, 63521, 51425, 64223, 61879, 57242, 42246, 50040, 3705, 8806, 51406, 46371, 56286,
                   49248, 11973, 648, 34740, 28409, 53680, 9276, 19345, 57143, 17301, 20013, 6650, 19146, 35346, 6510,
                   28337, 18390, 6897, 36384, 32758, 20854, 11162, 20424, 32800, 61319, 30497, 13223, 47241, 57392, 61669,
                   43208, 29033, 1224, 20666, 37363, 43388, 46868, 38369, 16663, 11219, 58498, 54562, 17807, 32031, 40792,
                   59753, 35735, 19556, 24737, 5869, 29352, 30450, 60522, 33582, 33100, 13131, 4418, 41898, 22503, 2109,
                   15301, 44183, 34478, 4608, 42658, 33886, 18131, 3885, 51009, 60476, 60687, 48930, 35013, 18158, 24398,
                   843, 44345, 60780, 10592, 43781, 5360, 49678, 22205, 59247, 55389, 20598, 22076, 31196, 6728, 14542,
                   17357, 8300, 50287, 13326, 38165, 26864, 29399, 55280, 11496, 34961, 27510, 51975, 36855, 44782, 5661,
                   26146, 690, 53895, 7181, 49652, 64449, 18237, 3077, 31029, 55669, 14686, 507, 15890, 25909, 26614,
                   43282, 1081, 10944, 17456, 2289, 11119, 65108, 25975, 55064, 9020, 52184, 25187, 12244, 19052, 22617,
                   5400, 7100, 35388, 19569, 35168, 4487, 15903, 52739, 31036, 21199, 5640, 47965, 11369, 45507, 14413,
                   47915, 59152, 27529, 52713, 26053, 2407, 49668, 43084, 80, 56829, 14641, 27974, 29059, 58197, 35341,
                   42560, 11641, 40259, 37340, 13509, 7230, 3328, 49988, 56182, 65186, 45361, 32540, 8687, 55706, 30271,
                   12553, 52547, 48885, 48412, 18236, 38049, 22421, 33936, 3850, 6619, 22643, 34466, 21523, 15763, 19440,
                   1556, 6399, 7983, 59039, 27341, 50971, 38566, 1939, 46469, 37356, 18434, 63034, 36402, 17996, 11349,
                   38798, 15457, 8597, 55040, 59165, 18680, 50200, 22161, 35934, 64806, 45727, 13005, 6351, 36287, 16822,
                   22718, 11045, 21263, 29151, 1705, 26593, 23049, 62080, 59860, 50893, 10904, 22681, 63184, 34398, 8727,
                   3771, 57114, 40469, 8595, 39363, 8430, 29398, 18016, 13527, 13057, 64443, 59445, 7922, 64253, 6940,
                   3827, 48908, 31934, 60115, 30291, 48657, 65001, 31864, 4718, 11092, 6148, 53708, 53303, 9039, 10582]
    b_arguments = [28045, 29571, 22887, 9297, 39448, 16596, 29740, 5550, 27539, 55039, 2029, 21770, 41752, 50631, 15798,
                   474, 24213, 54612, 48282, 6576, 50102, 55172, 30367, 7039, 2481, 62316, 551, 62623, 31006, 53819, 62295,
                   10182, 39835, 43783, 28943, 6942, 3994, 32870, 4122, 16772, 48952, 29958, 1338, 33326, 38090, 62368,
                   37439, 17648, 5178, 35947, 2222, 15416, 19425, 22081, 36077, 49401, 25742, 46243, 59797, 23893, 63848,
                   1723, 3265, 56084, 38455, 61143, 21976, 59250, 48442, 33679, 65270, 24557, 61045, 39034, 26160, 32430,
                   23988, 51301, 3617, 20662, 5972, 65402, 26971, 52579, 45374, 19236, 48556, 20389, 43069, 56355, 9616,
                   61365, 27701, 3633, 14178, 42391, 52268, 59811, 22924, 33550]
    m = 128

    # define hash function
    hash_function = lambda index, x: (a_arguments[index] * x + b_arguments[index % len(b_arguments)]) % m

    with open(output_filename, "w+") as output:
        output.write("Time,Gound Truth,Estimation\n")
    output.close()

    # batch duration = 5s
    ssc = StreamingContext(sc, 5)
    # window duration = 30s, slide duration = 10s
    stream_data = ssc.socketTextStream('localhost', port).window(30, 10)

    stream_data.map(lambda x: json.loads(x)["state"]) \
        .map(lambda x: int(binascii.hexlify(x.encode("utf8")), 16)) \
        .foreachRDD(lambda x: Flajolet_Martin(x, hash_func_number, hash_function, group, output_filename))

    ssc.start()
    ssc.awaitTermination()
