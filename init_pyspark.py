
from pyspark.sql.functions import *

from pyspark.sql.types import StructType, StructField, IntegerType,\
    StringType, BooleanType

df_movies = spark.read.csv("ml-latest-small/movies.csv",
                           header=True, inferSchema=True)

df_movies = df_movies.withColumn(colName="srt_path",
            col= concat(lit("subtitles/"), df_movies['title'], lit('.en.srt')))

rdd = df_movies.rdd.map(lambda x: (x['movieId'], os.path.isfile(x['srt_path'])))

schema_tmp = StructType([
    StructField('movieId', IntegerType(), True),
    StructField('srt_exists', BooleanType(), True),
])

df_movies = df_movies.join(other=sqlContext.createDataFrame(
            rdd, schema=schema_tmp), on="movieId")

