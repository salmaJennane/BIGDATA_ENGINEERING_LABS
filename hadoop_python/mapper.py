#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys

# input comes from standard input STDIN
for line in sys.stdin:
    # remove leading and trailing whitespaces
    line = line.strip()
    
    # split the line into words and returns as a list
    words = line.split()
    
    for word in words:
        # write the results to standard output STDOUT
        # format: word\t1
        print('%s\t%s' % (word, 1))