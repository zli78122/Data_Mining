from pyspark import SparkContext
import sys
import json
import heapq

if __name__ == '__main__':
    result = {}

    review_file = sys.argv[1]
    business_file = sys.argv[2]
    output_file = sys.argv[3]
    if_spark = sys.argv[4]
    n = sys.argv[5]

    # compute the average stars for each business category and output top n categories with the highest average stars.
    if if_spark == "spark":
        # a version with Spark
        sc = SparkContext(appName="Task2")

        rddReviewJsonData = sc.textFile(review_file)
        rddBusinessJsonData = sc.textFile(business_file)
        rddReviewData = rddReviewJsonData.map(lambda review: json.loads(review))
        rddBusinessData = rddBusinessJsonData.map(lambda business: json.loads(business))

        businessStatsRdd = rddReviewData.map(lambda review: (review["business_id"], review["stars"])) \
            .groupByKey().mapValues(lambda stars: [float(star) for star in stars]) \
            .map(lambda business_stats: (business_stats[0], (sum(business_stats[1]), len(business_stats[1]))))

        businessCategoriesRdd = rddBusinessData.map(lambda business: (business["business_id"], business["categories"])) \
            .filter(lambda business_categories: (business_categories[1] is not None) and (len(business_categories[1]) > 0)) \
            .mapValues(lambda categories: [category.strip() for category in categories.split(",")])

        # ('1SWheh84yJXfytovILXOAQ', (['Golf', 'Active Life'], (14.0, 3)))
        joinRdd = businessCategoriesRdd.leftOuterJoin(businessStatsRdd)

        result["result"] = joinRdd.map(lambda business_catsAndStar: business_catsAndStar[1]) \
            .filter(lambda catsAndStar: catsAndStar[1] is not None) \
            .flatMap(lambda catsAndStar: [(cat, catsAndStar[1]) for cat in catsAndStar[0]]) \
            .reduceByKey(lambda catAndStar1, catAndStar2: (catAndStar1[0] + catAndStar2[0], catAndStar1[1] + catAndStar2[1])) \
            .map(lambda catAndStar: [catAndStar[0], float(catAndStar[1][0] / catAndStar[1][1])]) \
            .takeOrdered(int(n), key=lambda x: (-x[1], x[0]))

    elif if_spark == "no_spark":
        # a version without Spark
        reviewData = {}
        businessData = {}
        words = {}

        try:
            with open(review_file) as reviews:
                while True:
                    review = reviews.readline()[0:-1]
                    if not review:
                        break
                    reviewJson = json.loads(review)

                    business_star = reviewData.get(reviewJson["business_id"])
                    if business_star is not None:
                        reviewData[reviewJson["business_id"]] = (business_star[0] + reviewJson["stars"], business_star[1] + 1)
                    else:
                        reviewData[reviewJson["business_id"]] = (reviewJson["stars"], 1)
        except FileNotFoundError:
            print(f'{reviews} not exists')
        reviews.close()

        try:
            with open(business_file) as businesses:
                while True:
                    business = businesses.readline()[0:-1]
                    if not business:
                        break
                    businessJson = json.loads(business)

                    if (businessJson["categories"] is None) or (len(businessJson["categories"]) == 0):
                        continue

                    businessData[businessJson["business_id"]] = [category.strip() for category in businessJson["categories"].split(",")]
        except FileNotFoundError:
            print(f'{businesses} not exists')
        businesses.close()

        # build words dict. e.g. {'Golf': (5.0, 2), 'active life': (5.0, 2)}
        for business_id, categoryList in businessData.items():
            # get (review sum stars, review number) of current business
            stars = reviewData.get(business_id)

            if stars is None:
                continue

            for category in categoryList:
                words_value = words.get(category)
                if words_value is not None:
                    words[category] = (words_value[0] + stars[0], words_value[1] + stars[1])
                else:
                    words[category] = (stars[0], stars[1])

        # compute average stars. e.g. {'golf': 2.5, 'active life': 2.5}
        for word, star in words.items():
            words[word] = float(star[0] / star[1])

        wordRes = []
        wordList = [(-star, word) for word, star in words.items()]
        heapq.heapify(wordList)
        for _ in range(int(n)):
            star, word = heapq.heappop(wordList)
            wordRes.append([word, -star])

        result["result"] = wordRes

    with open(output_file, "w") as output:
        json.dump(result, output)
    output.close()
