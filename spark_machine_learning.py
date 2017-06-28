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

import nltk # Natural Language ToolKit
from nltk.corpus import stopwords # Import the stop word list

from pyspark import SparkConf
from pyspark.sql import SparkSession

from pyspark.mllib.stat import Statistics
from pyspark.sql.functions import *

from pyspark.sql.types import StructType, StructField, IntegerType,\
    StringType, BooleanType


# Global variables :
script_path = os.path.abspath(sys.argv[0])
default_tsv_dir = os.path.dirname(script_path)+"/ml-training/"
default_csv_dir = os.path.dirname(script_path)+"/ml-latest-small/"
default_srt_dir  = os.path.dirname(script_path)+"/subtitles/"

# A wrapper to construct and access differents dataframes
class DataTools:
    """
    - df_movies : pyspark.sql.DataFrame of movies.csv
                schema = ['movieId', 'title', 'genres', 'srt_path']

    - n_movies : <int> number of movies considered.

    - df_test : pyspark.sql.DataFrame of movies's content subtitles
                schema = ['movieId', 'text', 'words']

    - df_train : pyspark.sql.DataFrame containing
    """

    def __init__(self, spark, limit_movies=None,
            csv_path = default_csv_dir+"movies.csv",
            srt_dir = default_srt_dir,
            tsv_path = default_tsv_dir+"train.tsv"):

        self.init_movie_set(spark, limit_movies, csv_path, srt_dir)

        self.init_test_set(spark)

        self.init_training_set(spark, tsv_path)


    def init_movie_set(self, spark, limit_movies, csv_path, srt_dir):
#{{{
        print("Reading %s ..." % csv_path)
        self.df_movies = read_movies_csv(spark, limit_movies, csv_path)

        # Removing registers for which no subtitles were found
        print("Removing registers for which no subtitles were found ...")
        self.df_movies, df_movies_without_subs = \
            filter_subs_not_found(spark, self.df_movies, srt_dir)

        self.n_movies = self.df_movies.count()

        if (df_movies_without_subs.count() is not 0):
            print ("Subtitles were not found for %d/%d movies." %
                    (df_movies_without_subs.count(), self.n_movies))
#}}}


    def init_test_set(self, spark):
#{{{
        print("Reading subtitles ...")
        self.df_test = read_subtitles(spark, self.df_movies)

        print("Applying bag_of_words to df_test ...")
        self.df_test = bag_of_words(self.df_test, nameInputCol="Phrase")
#}}}


    def init_training_set(self, spark, tsv_path):
#{{{
        print("Reading %s ..." % tsv_path)
        self.df_train = spark.read.csv(path=tsv_path, sep="\t",
                header=True, inferSchema=True)

        print("Applying bag_of_words to df_train ...")
        self.df_train = bag_of_words(df=self.df_train, nameInputCol="Phrase")
#}}}


# Functions for df_movies and df_test construction :
#{{{

def read_movies_csv(spark, limit_movies, csv_path):
    """
    Description :
    - Read csv file movies.csv
    - Filter rows if more than limit_movies
    """
#{{{
    df_tmp = spark.read.csv(csv_path, header=True,
            inferSchema=True) #schema=df_movies_schema)

    if(limit_movies is None):
        df_movies = df_tmp
    else:
        df_movies = df_tmp.limit(limit_movies)

    return df_movies
#}}}


def filter_subs_not_found(spark, df_movies, srt_dir):
    """
    Description : filter df_movies dataframe from movies which subtitles
            were not found in srt_dir.

    return :
    - df_movies_without_subs : DataFrame containing only movies for
                    which subtitles were not found.
    """
#{{{
    # Adding srt_path column that is a function of 'title' :
    df_movies = df_movies.withColumn(
            colName="srt_path",
            col = concat(lit(srt_dir), df_movies['title'],
                lit('.en.srt') ))

    # Checking for the existence of str files, if not exists delete row:

    # TO DO : do the filter using pyspark.sql.DataFrame.filter functino
    # df_movies = df_movies.filter(
    #         lambda row : os.path.isfile(row['srt_path'] ))
    # return : TypeError: condition should be string or Column

    rdd_tmp = df_movies.rdd.map(lambda x:
            (x['movieId'], os.path.isfile(x['srt_path'])))
    schema_tmp = StructType([
        StructField('movieId', IntegerType(), True),
        StructField('srt_exists', BooleanType(), True),
    ])
    df_movies = df_movies.join(
            other=spark.createDataFrame(rdd_tmp, schema=schema_tmp),
            on="movieId")

    df_movies_without_subs = df_movies.filter(
            df_movies['srt_exists'] == False)

    df_movies = df_movies.filter(
            df_movies['srt_exists'] == True)

    df_movies_without_subs = df_movies_without_subs.drop('srt_exists')
    df_movies = df_movies.drop('srt_exists')

    return df_movies, df_movies_without_subs
#}}}


def read_subtitles(spark, df_movies):
    """
    Description :
    - read all srt files and stores them into a rdd
    - convert this rdd to DataFrame
    - adjust datas by reducing on movieId

    Infos :
    - To read a whole directory use :
        SparkContext.wholeTextFiles lets you read a directory containing
        multiple small text files, and returns each of them as
        (filename, content) pairs.
    """
#{{{
    # Generate rdd by reading subtitles :
    fullRDD = spark.sparkContext.emptyRDD()
    for i, e in df_movies.select('movieId', 'srt_path').collect():
        rdd_tmp = spark.sparkContext.textFile(e, use_unicode=True)
        fullRDD = fullRDD.union(rdd_tmp.map(lambda x: [i, x]))

    # Converting rdd to DF :
    schem = StructType( [
        StructField('movieId', IntegerType(), True),
        StructField('Phrase', StringType(), True)
    ] )
    df_test = fullRDD.toDF(schema=schem)

    # # reduce on movieId and concat lines of subtitles to make only 1 string:
    # from pyspark.sql.functions import concat_ws
    # df_test = df_test.groupBy('movieId').agg(
    #         concat_ws(' ', collect_list('text')).alias('text'))
#}}}
    return df_test

#}}}

def bag_of_words(df, nameInputCol):
    """
    Description :
    - RegexTokenizer : tokenizer according a regex
    - remove stopwords
    - remove duplicates in words_regex to delete empty token

    """
#{{{
    from pyspark.ml.feature import RegexTokenizer

    patt = "[^a-zA-Z']"
    regexTokenizer = RegexTokenizer(inputCol=nameInputCol,
            outputCol='words_regex', pattern=patt)
    df = regexTokenizer.transform(df)

    # remove stopwords :
    from pyspark.ml.feature import StopWordsRemover
    remover = StopWordsRemover(inputCol="words_regex",
            outputCol="filtered")
    df = remover.transform(df)

    # remove doubles :
    df = df.dropDuplicates(['filtered'])

    return df

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
    # dfs.df_movies.printSchema()
    # dfs.df_movies.show()
    print('Continuing with df_movies.count() = %d ...' % dfs.df_movies.count())

    # Sentiment Analysis :
    print('------------ Sentiment Analysis -----------')
    print('Training set : ')
    dfs.df_test.show()

    print('Testing set : ')
    # dfs.df_train.show()






    spark.stop()

if __name__ == "__main__":
    sys.exit(main())
