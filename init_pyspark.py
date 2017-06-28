from pyspark.sql.functions import *

from pyspark.sql.types import StructType, StructField, IntegerType,\
    StringType, BooleanType

import nltk
from nltk.corpus import stopwords # Import the stop word list
import re

df_movies = spark.read.csv("ml-latest-small/movies.csv",
                           header=True, inferSchema=True)

df_movies = df_movies.limit(5)

df_movies = df_movies.withColumn(colName="srt_path",
            col= concat(lit("subtitles/"), df_movies['title'], lit('.en.srt')))

rdd = df_movies.rdd.map(lambda x: (x['movieId'], os.path.isfile(x['srt_path'])))

schema_tmp = StructType([
    StructField('movieId', IntegerType(), True),
    StructField('srt_exists', BooleanType(), True),
])

df_movies = df_movies.join(other=sqlContext.createDataFrame(
            rdd, schema=schema_tmp), on="movieId")

df = df_movies.filter(df_movies['srt_exists']==True)

# Generate rdd by reading subtitles :
fullRDD = spark.sparkContext.emptyRDD()
for i, e in df_movies.select('movieId', 'srt_path').collect():
    rdd_tmp = spark.sparkContext.textFile(e, use_unicode=True)
    fullRDD = fullRDD.union(rdd_tmp.map(lambda x: [i, x]))

# Converting rdd to DF :
schem = StructType( [
    StructField('movieId', IntegerType(), True),
    StructField('text', StringType(), True)
] )
df_content = fullRDD.toDF(schema=schem)

# # reduce on movieId and concat lines of subtitles to make only 1 string:
# from pyspark.sql.functions import concat_ws
# df_content = df_content.groupBy('movieId').agg(
#     concat_ws(' ', collect_list('text')).alias('text'))


from pyspark.ml.feature import Tokenizer, RegexTokenizer

patt = "[^a-zA-Z']"
regexTokenizer = RegexTokenizer(inputCol='text',
        outputCol='words_regex', pattern=patt)
df_content = regexTokenizer.transform(df_content)

from pyspark.ml.feature import StopWordsRemover
remover = StopWordsRemover(inputCol="words_regex",
        outputCol="filtered")
df_content = remover.transform(df_content)

df_content = df_content.dropDuplicates(['filtered'])



nltk.download("stopwords")
stopwords_en = stopwords.words('english')
stops = set(stopwords_en)
for i in range(0, len(rdds)):
    rdds[i][1] = rdds[i][1].map(lambda l: re.sub("[^a-z A-Z]", "", l))
    rdds[i][1] = rdds[i][1].map(lambda l: re.sub("^ *", "", l))
    rdds[i][1] = rdds[i][1].filter(lambda l: len(l)!=0)
    rdds[i][1] = rdds[i][1].map(lambda l: l.lower().split())
    rdds[i][1] = rdds[i][1].flatMap(lambda xs: [x for x in xs])
    rdds[i][1] = rdds[i][1].filter(lambda l: l not in stops)
