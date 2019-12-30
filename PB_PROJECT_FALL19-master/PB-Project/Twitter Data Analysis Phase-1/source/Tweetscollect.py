# -*- coding: utf-8 -*-
"""
Created on Thu Sep 15 12:16:47 2019

@author: VAMSI
"""

from __future__ import absolute_import, print_function

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json

# Go to http://apps.twitter.com and create an app.
# The consumer key and secret will be generated for you after
consumer_key="Ekjn0s5BGpFQakrWLNZ1BHZ6Z"
consumer_secret="zixnRQ7LxAjSnl6za1sfu0ZUjYRwrPtVchnLYKpLsTUOTUF9KW"

# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section

access_token="817980148070891520-ggQXjgUkZx37d7M3bDppzW6y3wjXctC"
access_token_secret="zXvw36v88NyE18CnWr2vf1pyndWvkt4WcHnuUvo4WaDxj"

class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    
    def on_data(self, data):
        try:
            with open('data1.json', 'a') as outfile:
                json.dump(data,outfile)
            with open('data2.json','a') as outputj:
                outputj.write(data)
            with open('tweets.txt', 'a') as tweets:
                tweets.write(data)
                tweets.write('\n')
            outfile.close()
            tweets.close()
            outputj.close()
        except BaseException as e:
            print('there is trouble in collecting the tweets',str(e))
        return True

    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(track=['NBA'])