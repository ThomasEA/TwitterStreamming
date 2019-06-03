#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun  2 22:11:25 2019

@author: ethomas

Client to retrieve tweets from Twitter API
"""

import socket
import sys
import requests
import requests_oauthlib
import json
from time import sleep

ACCESS_TOKEN = '74010733-EPjdOoF7cK1KgqezblyPc7BM3e8eqgeesOfu2TbbA'
ACCESS_SECRET = 'XzoTJoLs5tM59zpESGjycg7PBv8rpvCT9ZudM73b86wY4'
CONSUMER_KEY = 'n6dZ9wom2lrjNjfLFtJi3JvEG'
CONSUMER_SECRET = 'gdLBLzSgR7RLXk2PuvJinfSleGoueqqhSa548YFfoT0IPEuzB2'

my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)

def send_tweets_to_spark_temp(tcp_connection):
    for i in range(50):
        sleep(1)
        print('>>> ' + str(i))
        tweet_data = bytes('#teste #teste #abc #abc #abc' + '\n', 'utf-8')
        tcp_connection.send(tweet_data)

def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            print("Tweet Text: " + tweet_text)
            print ("------------------------------------------")
            #tcp_connection.send(tweet_text + '\n')
            #tweet_data = bytes('#teste #teste #abc #abc #abc' + '\n', 'utf-8')
            tweet_data = bytes(tweet_text + '\n', 'utf-8')
            tcp_connection.send(tweet_data)
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)


def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    #query_data = [('language', 'en'), ('locations', '-130,-20,100,50'),('track','#')]
    query_data = [('locations', '-130,-20,100,50'), ('track', '#')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response


TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connection received from [%s]... Starting getting tweets." % str(addr))
resp = get_tweets()
send_tweets_to_spark(resp,conn)
#send_tweets_to_spark_temp(conn)