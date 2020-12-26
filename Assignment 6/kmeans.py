import sys
import math
from time import time


def Euclidean_distance(dimension, point1, point2):
    distance = 0
    for i in range(dimension):
        distance += math.pow((point1[i] - point2[i]), 2)
    return math.sqrt(distance)


if __name__ == '__main__':
    start_time = time()

    # publicdata/kmeans_data1.text 3 output/k_means1.txt
    # publicdata/kmeans_data2.text 10 output/k_means2.txt
    input_path = sys.argv[1]
    n_cluster = int(sys.argv[2])
    out_file1 = sys.argv[3]

    # {point_index : [list of coordinate]}
    point_dict = {}
    try:
        with open(input_path) as input_file:
            while True:
                point = input_file.readline()
                if not point:
                    break
                point_dict[int(point.split(",")[0])] = [float(i) for i in point.split(",")[1:]]
    except FileNotFoundError:
        print(f'{input_file} not exists')
    input_file.close()

    # dimension = 10
    dimension = len(point_dict[0])
    # point_num = 1000 / 10000
    point_num = len(point_dict)

    center_point = [0] * dimension
    for cur_coordinates in point_dict.values():
        for i in range(len(cur_coordinates)):
            center_point[i] += cur_coordinates[i]
    center_point = [i / point_num for i in center_point]

    # find the first initial centroid : the longest distance to center_point
    centroid_dict = dict()
    max_distance = -1
    first_centroid = (-1, [])
    for k, v in point_dict.items():
        cur_distance = Euclidean_distance(dimension, v, center_point)
        if cur_distance > max_distance:
            max_distance = cur_distance
            first_centroid = (k, v)
    # {point_index : [list of coordinate]}
    centroid_dict[first_centroid[0]] = first_centroid[1]

    # find the remaining initial centroid
    for _ in range(n_cluster - 1):
        max_distance = -1
        new_centroid = (-1, [])
        for cur_index, cur_coordinates in point_dict.items():
            if cur_index not in centroid_dict:
                distance_sum = 0
                for _, v in centroid_dict.items():
                    distance_sum += Euclidean_distance(dimension, v, cur_coordinates)
                if distance_sum > max_distance:
                    max_distance = distance_sum
                    new_centroid = (cur_index, cur_coordinates)
        centroid_dict[new_centroid[0]] = new_centroid[1]

    # attach each point to the nearest cluster center
    # [(centroid coordinates, [list of point_index])]
    centroid_points = []
    total_distance = 0
    for v in centroid_dict.values():
        centroid_points.append((v, []))
    for cur_point, cur_coordinates in point_dict.items():
        min_distance = 999999999
        min_cluster_index = -1
        for i in range(n_cluster):
            cur_distance = Euclidean_distance(dimension, centroid_points[i][0], cur_coordinates)
            if cur_distance < min_distance:
                min_distance = cur_distance
                min_cluster_index = i
        total_distance += min_distance
        centroid_points[min_cluster_index][1].append(cur_point)

    while True:
        # [(centroid coordinates, [list of point_index])]
        new_centroid_points = []
        # find the new centroid
        for i in range(n_cluster):
            new_centroid_coordinates = [0] * dimension
            for cur_point in centroid_points[i][1]:
                cur_coordinates = point_dict[cur_point]
                for dimension_index in range(dimension):
                    new_centroid_coordinates[dimension_index] += cur_coordinates[dimension_index]
            cluster_num = len(centroid_points[i][1])
            new_centroid_coordinates = [j / cluster_num for j in new_centroid_coordinates]
            new_centroid_points.append((new_centroid_coordinates, []))

        # attach each point to the nearest new cluster center
        new_total_distance = 0
        for cur_point, cur_coordinates in point_dict.items():
            min_distance = 999999999
            min_cluster_index = -1
            for i in range(n_cluster):
                cur_distance = Euclidean_distance(dimension, new_centroid_points[i][0], cur_coordinates)
                if cur_distance < min_distance:
                    min_distance = cur_distance
                    min_cluster_index = i
            new_total_distance += min_distance
            new_centroid_points[min_cluster_index][1].append(cur_point)

        # terminate while loop when we find the minimum total_distance
        if new_total_distance < total_distance:
            total_distance = new_total_distance
            centroid_points = new_centroid_points
        else:
            break

    result = []
    for i in range(n_cluster):
        for cur_point in centroid_points[i][1]:
            result.append((cur_point, i))
    result.sort(key=lambda x: x[0])

    with open(out_file1, "w+") as output:
        output.write("{")
        for index, elem in enumerate(result):
            if index == len(result) - 1:
                output_str = "\"" + str(elem[0]) + "\": " + str(elem[1])
                output.write(output_str)
            else:
                output_str = "\"" + str(elem[0]) + "\": " + str(elem[1]) + ", "
                output.write(output_str)
        output.write("}")
    output.close()

    print('Duration: %.2f' % (time() - start_time))
