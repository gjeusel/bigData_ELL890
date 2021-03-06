import sys, os, re

from pyspark import SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.functions import *

from pyspark.sql.types import StructType, StructField, IntegerType,\
    StringType, BooleanType

# Global variables :
script_path = "/home/gjeusel/projects/bigData_ELL890"
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
                schema = ['movieId', 'text', 'words_regex', 'filtered']

    - df_train : pyspark.sql.DataFrame containing phrases from resume with
                 Sentiment note.
                 The sentiment labels are:
                    0 - negative
                    1 - somewhat negative
                    2 - neutral
                    3 - somewhat positive
                    4 - positive
                 schema = ['PhraseId', 'SentenceId', 'Phrase', Sentiment',
                           'words_regex', 'filtered']
    """

    def __init__(self, spark, limit_movies=None, limit_training_set=None,
            csv_path = default_csv_dir+"movies.csv",
            srt_dir = default_srt_dir,
            tsv_path = default_tsv_dir+"train.tsv"):

        self.init_movie_set(spark, limit_movies, csv_path, srt_dir)

        self.init_test_set(spark)

        self.init_training_set(spark, tsv_path, limit_training_set)


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
            print ("Subtitles were not found for %d movie(s)." %
                    df_movies_without_subs.count())
#}}}


    def init_training_set(self, spark, tsv_path, limit_training_set):
#{{{
        print("Reading %s ..." % tsv_path)
        df_tmp =  spark.read.csv(path=tsv_path, sep="\t",
                header=True, inferSchema=True)

        if(limit_training_set is None):
            self.df_train = df_tmp
        else:
            self.df_train = df_tmp.limit(limit_training_set)

        print("Applying bag_of_words to df_train ...")
        self.df_train = bag_of_words(df=self.df_train)
#}}}


    def init_test_set(self, spark):
#{{{
        print("Reading subtitles ...")
        self.df_test = read_subtitles(spark, self.df_movies)

        print("Applying bag_of_words to df_test ...")
        self.df_test = bag_of_words(df=self.df_test)
#}}}


    def perform_countVec(self):
        print('Fitting CountVectorizer model ...')
        model = fit_countVec(self.df_train, self.df_test)

        print('Transforming train and test sets ...')
        self.df_train = model.transform(self.df_train)
        self.df_test = model.transform(self.df_test)


    def classification(self, **kwargs):
        from pyspark.ml.classification import LogisticRegression
        lr = LogisticRegression(featuresCol='features',
                labelCol='Sentiment', predictionCol='Sentiment_Predicted',
                **kwargs)

        print('Fitting LogisticRegression model ...')
        lrModel = lr.fit(self.df_train)

        print('Avaliation for subtitles ...')
        self.df_test = lrModel.transform(self.df_test)

        print('Formatting results ...')
        self.construct_df_results()

        return lrModel


    def construct_df_results(self):
#{{{
        self.df_results = self.df_movies.select('movieId', 'title')

        df_avg_with_neutral = self.df_test.select('movieId',
                'Sentiment_Predicted') \
                .groupBy('movieId') \
                .agg(avg('Sentiment_Predicted').alias(
                'avg_Sentiment_Predicted'))\
                .sort('movieId')

        df_avg_without_neutral = self.df_test.select('movieId',
                'Sentiment_Predicted')\
                .filter( self.df_test['Sentiment_Predicted'] != 2)\
                .groupBy('movieId')\
                .agg(avg('Sentiment_Predicted').alias(
                'avg_Sentiment_Pred_without_neutral'))\
                .sort('movieId')

        # Bypassing error obtained when trying to merge previous 2
        df1 = df_avg_with_neutral.selectExpr('movieId as movieId1',
            'avg_Sentiment_Predicted as score')

        df2 = df_avg_without_neutral.selectExpr('movieId as movieId2',
            'avg_Sentiment_Pred_without_neutral as score_without_neutral')

        self.df_results = self.df_results.join(df1, col('movieId') ==
                col('movieId1')).join(df2, col('movieId') ==
                col('movieId2')).drop('movieId1', 'movieId2')

        self.df_results = self.df_results.sort('movieId')
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

def bag_of_words(df, nameInputCol="Phrase"):
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

def fit_countVec(df_train, df_test, nameInputCol="filtered"):
    df_tmp = df_train.select(nameInputCol).union(
            df_test.select(nameInputCol))

    from pyspark.ml.feature import CountVectorizer
    CV = CountVectorizer(inputCol=nameInputCol, outputCol='features')
    model = CV.fit(df_tmp)

    return(model)

# Constructor :
dfs = DataTools(spark, limit_movies=5,
        csv_path="ml-latest-small/movies.csv",
        srt_dir="subtitles/",
        tsv_path="ml-training/train.tsv")

# dfs.df_movies.printSchema()
# dfs.df_movies.show()
print('Continuing with df_movies.count() = %d ...' % dfs.df_movies.count())

# Sentiment Analysis :
print('------------ Sentiment Analysis -----------')
dfs.perform_countVec()
print('Testing set : ')
dfs.df_train.show(5)
print('Training set : ')
dfs.df_test.show(5)

lrModel = dfs.classification()
dfs.df_results.show(truncate=False)




