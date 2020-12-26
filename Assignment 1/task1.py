from pyspark import SparkContext
import sys
import json

stop_word_set = set()
punctuation_set = {"(", "[", ",", ".", "!", "?", ":", ";", "]", ")"}


def build_stop_word_set(path):
    try:
        with open(path) as stop_word_file:
            while True:
                stop_word = stop_word_file.readline()[0:-1]
                if not stop_word:
                    break
                stop_word_set.add(stop_word)
    except FileNotFoundError:
        print(f'{stop_word_file} not exists')
    stop_word_file.close()


def format_word(word):
    start = 0
    while start < len(word):
        if 'a' <= word[start] <= 'z' or 'A' <= word[start] <= 'Z' or word[start] not in punctuation_set:
            break
        else:
            start += 1
    end = len(word) - 1
    while end >= 0:
        if 'a' <= word[end] <= 'z' or 'A' <= word[end] <= 'Z' or word[end] not in punctuation_set:
            break
        else:
            end -= 1
    return word[start:end + 1].lower() if start <= end else ""


if __name__ == '__main__':
    result = {}

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    stop_word_file = sys.argv[3]
    year = sys.argv[4]
    m = sys.argv[5]
    n = sys.argv[6]

    build_stop_word_set(stop_word_file)

    sc = SparkContext(appName="Task1")
    rddReviewJsonData = sc.textFile(input_file)
    rddReviewData = rddReviewJsonData.map(lambda review: json.loads(review))

    # A. The total number of reviews (0.4pts)
    result["A"] = rddReviewData.count()

    # B. The number of reviews in a given year, y (0.8pts)
    result["B"] = rddReviewData.filter(lambda review: review["date"][0:4] == year).count()

    # C. The number of distinct businesses which have the reviews (0.8pts)
    result["C"] = rddReviewData.map(lambda review: review["business_id"]).distinct().count()

    # D. Top m users who have the largest number of reviews and its count (0.8pts)
    # Sort by values (descending): RDD.takeOrdered(5, key = lambda x: -x[1])
    result["D"] = rddReviewData.map(lambda review: (review["user_id"], 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .takeOrdered(int(m), key=lambda x: (-x[1], x[0]))

    # E. Top n frequent words in the review text. The words should be in lower cases.
    frequent_words = rddReviewData.map(lambda review: review["text"]) \
        .flatMap(lambda text: text.split(" ")) \
        .map(lambda word: format_word(word)) \
        .filter(lambda word: len(word) > 0 and word not in stop_word_set) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .takeOrdered(int(n), key=lambda x: (-x[1], x[0]))
    result["E"] = [word[0] for word in frequent_words]

    with open(output_file, "w") as output:
        json.dump(result, output)
    output.close()
