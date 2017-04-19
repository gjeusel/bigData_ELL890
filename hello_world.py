#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os, re
reload(sys)
sys.setdefaultencoding('utf8') # problem with encoding

import argparse
import subprocess

from pyspark import SparkContext

def main():

    logFile = "README.md"  # Should be some file on your system
    sc = SparkContext("local", "Simple App")
    logData = sc.textFile(logFile).cache()

    numAs = logData.filter(lambda s: 'a' in s).count()
    numBs = logData.filter(lambda s: 'b' in s).count()

    print "Lines with a: %i, lines with b: %i" % (numAs, numBs)


if __name__ == '__main__':
    main()
