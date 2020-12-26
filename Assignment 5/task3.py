import sys
import string
import random
import tweepy

TWITTER_API_KEY = "9HjtxbUmhPwgNCGYHIJ74saf5"
TWITTER_SECRET_KEY = "l4FdI6IdLBjo9dF4poqLXJ4p3LapuHXhQb6d9i6ee1emo9dvYG"
TWITTER_ACCESS_TOKEN = "1324547794648985604-vD3E6SgLA0fZglL0V9JZBwgZX4KaST"
TWITTER_TOKEN_SECRET = "XhG7yvfeCNCAymvbtRZyakFQjnXRCqXjY1HITnullMvoW"

sequence_number = [0]
hashtags_count = [0]
reservoir_capacity = 100
reservoir_samples = []
valid_characters = set(string.printable)


# Step 1. Creating a StreamListener
class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        # Get all valid hashtags
        all_valid_hashtags = []
        hashtag_dict_list = status.entities["hashtags"]
        for hashtag_dict in hashtag_dict_list:
            if len(hashtag_dict["text"].strip()) == 0:
                continue
            is_valid_tag = True
            for ch in hashtag_dict["text"]:
                if ch not in valid_characters:
                    is_valid_tag = False
                    break
            if is_valid_tag:
                all_valid_hashtags.append(hashtag_dict["text"])

        if len(all_valid_hashtags) == 0:
            return
        else:
            sequence_number[0] += 1

        # Reservoir Sampling
        for hashtag in all_valid_hashtags:
            hashtags_count[0] += 1
            if hashtags_count[0] <= reservoir_capacity:
                reservoir_samples.append(hashtag)
            else:
                random_number = random.randint(0, hashtags_count[0] - 1)
                if random_number < reservoir_capacity:
                    reservoir_samples[random_number] = hashtag

        # Find the tags in the sample list with the top 3 frequencies
        hashtag_count_dict = dict()
        for sample in reservoir_samples:
            if sample not in hashtag_count_dict:
                hashtag_count_dict[sample] = 1
            else:
                hashtag_count_dict[sample] += 1

        count_hashtag_dict = dict()
        for hashtag, count in hashtag_count_dict.items():
            if count not in count_hashtag_dict:
                count_hashtag_dict[count] = [hashtag]
            else:
                count_hashtag_dict[count].append(hashtag)

        rev_sorted_counts = sorted(count_hashtag_dict.keys(), reverse=True)
        rev_sorted_counts = rev_sorted_counts if len(rev_sorted_counts) <= 3 else rev_sorted_counts[0:3]

        # Print result
        with open(output_filename, "a+") as output:
            output.write("The number of tweets with tags from the beginning: %d\n" % (sequence_number[0]))
            for count in rev_sorted_counts:
                for hashtag in sorted(count_hashtag_dict[count]):
                    output.write("%s : %d\n" % (hashtag, count))
            output.write("\n")
        output.close()


# sudo pip install tweepy
if __name__ == '__main__':
    # 9999 output/output3.csv
    port = int(sys.argv[1])
    output_filename = sys.argv[2]

    with open(output_filename, "w+") as output:
        output.write("")
    output.close()

    auth = tweepy.OAuthHandler(TWITTER_API_KEY, TWITTER_SECRET_KEY)
    auth.set_access_token(TWITTER_ACCESS_TOKEN, TWITTER_TOKEN_SECRET)

    # Step 2. Creating a Stream
    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth=auth, listener=myStreamListener)

    # Step 3. Starting a Stream
    myStream.filter(track=['Amazon', 'Google', 'Apple'], languages=["en"])
