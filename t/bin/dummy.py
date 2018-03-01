#!/usr/bin/python
# 
# 
# 
# 
# Kim Brugger (28 Feb 2018), contact: kim@brugger.dk

import sys
import pprint
import time
import argparse
pp = pprint.PrettyPrinter(indent=4)
import random


if __name__ == "__main__":



    parser = argparse.ArgumentParser(description='dummy script for testing that can sleep and fail')

    parser.add_argument('-s', '--sleep-time', default=5, help="sleep time for the script, default 5")

    parser.add_argument('-r', '--random-sleep-time', help="random sleep time for the script")

    parser.add_argument('-f', '--fail-rate', default=0,  help="fail rate, float between 0-1, default 0")
    parser.add_argument('-m', '--message', default="",  help="message to print at exit")


    args = parser.parse_args()

    if args.random_sleep_time:
        args.sleep_time = random.randint(0, int(args.random_sleep_time))


    time.sleep( float(args.sleep_time) )


    if random.random() < float(args.fail_rate):
        print("Failed: {}".format( args.message))
        exit(10)
    else:
        print("Success: {}".format( args.message))
