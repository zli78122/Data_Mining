import sys
import json
from pyspark import SparkContext

if __name__ == '__main__':
    business_file = sys.argv[1]
    review_file = sys.argv[2]
    output_file = sys.argv[3]

    sc = SparkContext(appName="Pre-Processing Task")

    businessRdd = sc.textFile(business_file) \
        .map(lambda x: json.loads(x)) \
        .map(lambda x: (x["business_id"], x["state"], x["stars"])) \
        .filter(lambda x: float(x[2]) >= 4.0) \
        .map(lambda x: (x[0], x[1])) \
        .collect()

    business_state_dict = dict(businessRdd)

    reviewRdd = sc.textFile(review_file) \
        .map(lambda x: json.loads(x)) \
        .map(lambda x: (x["user_id"], x["business_id"])) \
        .filter(lambda x: x[1] in business_state_dict) \
        .map(lambda x: (x[0], business_state_dict[x[1]])) \
        .collect()

    with open(output_file, "w") as output:
        output.write("user_id,state\n")
        for pair in reviewRdd:
            output.write("%s,%s\n" % (pair[0], pair[1]))
    output.close()
