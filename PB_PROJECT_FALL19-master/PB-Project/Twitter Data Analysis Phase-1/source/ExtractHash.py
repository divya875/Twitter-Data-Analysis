# -*- coding: utf-8 -*-
"""

@author: Vamsi
"""

# Import libraries
import json


# We use this function to take the .txt file as input
# and returns tweets as a list of json
def tweets_parsing():
    twt_data_path = 'tweets.txt'
    tweet_data = []
    tweets_file = open(twt_data_path, "r")
    for line in tweets_file:
        try:
            tweet = json.loads(line)
            tweet_data.append(tweet)
        except:
            continue

    print('\t parsed data into the json format successfully')
    return tweet_data


# We use this function to extract hashtags and urls from the tweets data
def hashtags_urls_extraction(tweet_data):
    # Extract hashtags and urls into separate files
    hashtagfile = 'hashtags.txt'
    urlsfile = 'urls.txt'

    htoutfile = open(hashtagfile, 'w', encoding="utf-8")
    urlsoutfile = open(urlsfile, 'w', encoding="utf-8")

    for i in range(len(tweet_data)):

        # Extracting the hashtags
        ht = tweet_data[i].get('entities').get('hashtags')
        for j in range(len(ht)):
            htoutfile.write(ht[j].get('text'))
            htoutfile.write('\n')

        # Extracting the urls (expanded urls only)
        url = tweet_data[i].get('entities').get('urls')
        for k in range(len(url)):
            urlsoutfile.write(url[k].get('expanded_url'))
            urlsoutfile.write(' ')

    print('\n The extraction is done')


# Main Activity
if __name__ == '__main__':
    # Parsing the data
    print('\n..........Parsing the data..............')
    tweets_data = tweets_parsing()

    # Extracting hashtags and urls
    print('\n...Extracting the hashtags and urls..')
    hashtags_urls_extraction(tweets_data)