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
from pyspark.sql.functions import *

from pyspark.sql.types import StructType, StructField, IntegerType,\
    StringType, BooleanType


# Global variables :
script_path = os.path.abspath(sys.argv[0])
default_csv_dir = os.path.dirname(script_path)+"/ml-latest-small/"
default_srt_dir  = os.path.dirname(script_path)+"/subtitles/"

# A wrapper to construct and access differents dataframes
class DataTools:
    """
    - df_movies : pyspark.sql.DataFrame of movies.csv
                schema = ['movieId', 'title', 'genres', 'srt_path']
    """


    def __init__(self, spark, limit_movies=None,
            csv_path=default_csv_dir+"movies.csv",
            srt_dir=default_srt_dir):
#{{{
        """
        Description : constructor
            - Read csv file
            - Filter rows if more than limit_movies
            - Filter rows for which srt file not found
                convention used : srt_path = srt_dir + 'title' + '.en.srt'
        """

        print("Reading %s ..." % csv_path)
        # The datatype can be inferred, which means deduced during the process of
        # reading. But if multi-TB+ data, better provide an explicit pre-defined
        # schema manually :
        # df_movies_schema = StructType([
        #     StructField('movieId', IntegerType(), True),
        #     StructField('title', StringType(), True),
        #     StructField('genres', StringType(), True)])

        df_tmp = spark.read.csv(csv_path, header=True,
                inferSchema=True) #schema=df_movies_schema)

        if(limit_movies is None):
            self.df_movies = df_tmp
        else:
            self.df_movies = df_tmp.limit(limit_movies)

        n_init = self.df_movies.count()

        df_movies_without_subs = self.filter_subs_not_found(spark)
        if (df_movies_without_subs.count() is not 0):
            print ("Subtitles were not found for %d/%d movies." %
                    (df_movies_without_subs.count(), n_init))
            # df_movies_without_subs.show()
#}}}


    def filter_subs_not_found(self, spark, srt_dir=default_srt_dir):
#{{{
        """
        Description : filter df_movies dataframe from movies which subtitles
                were not found in srt_dir.

        return : pyspark.sql.dataframe.DataFrame of movies which subtitles
                were not found.
        """

        # Adding srt_path column that is a function of 'title' :
        self.df_movies = self.df_movies.withColumn(
                colName="srt_path",
                col = concat(lit(srt_dir), self.df_movies['title'],
                    lit('.en.srt') ))

        # Checking for the existence of str files, if not exists delete row:

        # TO DO : do the filter using pyspark.sql.DataFrame.filter functino
        # self.df_movies = self.df_movies.filter(
        #         lambda row : os.path.isfile(row['srt_path'] ))
        # return : TypeError: condition should be string or Column

        rdd_tmp = self.df_movies.rdd.map(lambda x:
                (x['movieId'], os.path.isfile(x['srt_path'])))
        schema_tmp = StructType([
            StructField('movieId', IntegerType(), True),
            StructField('srt_exists', BooleanType(), True),
        ])
        self.df_movies = self.df_movies.join(
                other=spark.createDataFrame(rdd_tmp, schema=schema_tmp),
                on="movieId")

        df_movies_without_subs = self.df_movies.filter(
                self.df_movies['srt_exists'] == False)

        self.df_movies = self.df_movies.filter(
                self.df_movies['srt_exists'] == True)

        df_movies_without_subs = df_movies_without_subs.drop('srt_exists')
        self.df_movies = self.df_movies.drop('srt_exists')

        return df_movies_without_subs
#}}}


def setup_argparser():
#{{{
    """ Define and return the command argument parser. """
    parser = argparse.ArgumentParser(description=''' Spark Machine Learning
                                     -- Big Data.''')

    parser.add_argument('--csv_dir', dest='csv_dir', required=False,
                        default=default_csv_dir, action='store',
                        help='''Path to directory that contains csv files.
                        ''')

    parser.add_argument('--srt_dir', dest='srt_dir', required=False,
                        action='store', default=default_srt_dir,
                        help='''Path to directory thaht contains subtitles
                        files.
                             ''')

    parser.add_argument('--limit_movies', dest='limit_movies', required=False,
                        action='store', default=None, type=int,
                        help='''Number maximum of subtitles to download,
                        downloaded by movieId''')

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
    csv_dir = args.csv_dir
    srt_dir  = args.srt_dir
    limit_movies = args.limit_movies


    spark = SparkSession.builder \
    .appName("Python Spark Big Data Machine Learning") \
    .getOrCreate()

    # Constructor :
    dfs = DataTools(spark, limit_movies)
    dfs.df_movies.printSchema()
    dfs.df_movies.show()
    print('Continuing with df_movies.count() = %d ...' % dfs.df_movies.count())
    dfs.df_movies.cache()



    spark.stop()

if __name__ == "__main__":
    sys.exit(main())
