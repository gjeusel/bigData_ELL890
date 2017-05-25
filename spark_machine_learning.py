#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

import sys, os, re
reload(sys)
sys.setdefaultencoding('utf8') # problem with encoding

import argparse

import matplotlib
matplotlib.use("Qt4Agg") # enable plt.show() to display
import matplotlib.pyplot as plt

import logging as log

import pandas as pd
import numpy as np


from pyspark import SparkConf
from pyspark.sql import SparkSession

from pyspark.mllib.stat import Statistics


# Global variables :
script_path = os.path.abspath(sys.argv[0])
default_csv_path = os.path.dirname(script_path)+"/ml-latest-small/"
default_srt_path  = os.path.dirname(script_path)+"/subtitles/"

def find_subtitles(spark_session, df_movies, srt_path = default_srt_path): #{{{
    """ Find associated subtitles of dataframe"""
    # for row in df_movies.rdd.collect():
    #     print("what's inside row = %d %s %s" % row[0],row[1],row[2])
    df_reduced = df_movies.take(5)
    print(df_reduced)


#}}}

def setup_argparser(): #{{{
    """ Define and return the command argument parser. """
    parser = argparse.ArgumentParser(description=''' Spark Machine Learning
                                     -- Big Data.''')

    parser.add_argument('--csv_path', dest='csv_path', required=False,
                        default=default_csv_path, action='store',
                        help='''Path to directory that contains csv files.
                        ''')

    parser.add_argument('--srt_path', dest='srt_path', required=False,
                        action='store', default=default_srt_path,
                        help='''Path to directory thaht contains subtitles
                        files.
                             ''')

    parser.add_argument('--log', dest='logfile', required=False,
                        action='store', default="ML_spark.log",
                        help='Log file path.')

    parser.add_argument('-v', '--verbose', dest='verbose', required=False,
                        default=0, type=int,
                        help='Verbose level: 0 for errors, 1 for info or 2 for debug.')

    return parser
#}}}


def main(argv=None):

    parser = setup_argparser()

    try:
        args = parser.parse_args()

    except argparse.ArgumentError as exc:
        log.exception('Error parsing options.')
        parser.error(str(exc.message))
        raise

    verbose  = args.verbose
    logfile  = args.logfile
    csv_path = args.csv_path
    srt_path  = args.srt_path

    spark = SparkSession.builder \
    .appName("Python Spark Big Data Machine Learning") \
    .getOrCreate()

# The datatype can be inferred, which means deduced during the process of
# reading. But if multi-TB+ data, better provide an explicit pre-defined
# schema manually :
# from pyspark.sql.types import StructType, StructField, IntegerType,\
# StringType, BooleanType
#   df_movies_schema = StructType([\
#     StructField('movieId', IntegerType(), True),\
#     StructField('title', StringType(), True),\
#     StructField('genres', StringType(), True)])

    df_movies = spark.read.csv(csv_path+"movies.csv", header=True, \
        inferSchema=True) #,schema=df_movies_schema)
    df_movies.printSchema()
    df_movies.show()
    print('number of rows : %d' % df_movies.count())

    find_subtitles(spark, df_movies)
    spark.stop()

if __name__ == "__main__":
    sys.exit(main())
