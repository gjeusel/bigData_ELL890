from pyspark.sql.functions import *

from pyspark.sql.types import StructType, StructField, IntegerType,\
    StringType, BooleanType

import nltk
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

rdds = []
for e in df_movies.select('srt_path').collect():
    print(e[0].encode('utf8'))
    rdds.append(spark.sparkContext.textFile(e[0], use_unicode=True))

for rdd in rdds:
    rdd = rdd.map(lambda l: re.sub("[^a-z A-Z]", "", l))
    rdd = rdd.filter(lambda l: re.sub("^ *", "", l))
    rdd = rdd.filter(lambda l: len(l)!=0)
    rdd = rdd.map(lambda l: l.lower().split())

