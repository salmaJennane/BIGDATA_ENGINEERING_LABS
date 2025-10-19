#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from operator import itemgetter
import sys

current_word = None
current_count = 0
word = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    
    # splitting the data on the basis of tab provided in mapper.py
    try:
        word, count = line.split('\t', 1)
    except ValueError:
        # ignore/discard this line if it doesn't have exactly 2 parts
        continue
    
    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # ignore/discard this line if count is not a number
        continue
    
    # Hadoop sorts map output by key (word) before it is passed to the reducer
    if current_word == word:
        current_count += count
    else:
        if current_word:
            # write result to STDOUT
            print('%s\t%s' % (current_word, current_count))
        current_count = count
        current_word = word

# output the last word
if current_word == word:
    print('%s\t%s' % (current_word, current_count))