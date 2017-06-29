[<img src="http://www.euridis-blog.com/wp-content/uploads/2017/02/TOP-BIG-DATA.jpg" width="500px" alt="logo" />](https://github.com/gjeusel/bigData_ELL890)

# EEL890 Big Data : [Spark](https://spark.apache.org/) Project

----
## Purpose : Sentiment analysis based on subtitles

Train a [Multinomial logistic regression](https://spark.apache.org/docs/latest/ml-classification-regression.html#multinomial-logistic-regression)
using [Rotten Tomatoes dataset](https://www.kaggle.com/c/sentiment-analysis-on-movie-reviews)
that give a score for each resume of a film.

Natural Language Processing Model used : Bag-Of-Word\
[<img src="https://i.ytimg.com/vi/OGK9SHt8SWg/maxresdefault.jpg" width="200px" alt="logo" />](https://www.youtube.com/watch?v=OGK9SHt8SWg)

Due to this selection of model, the training phase can only be done after
subtitles files has been read (cf [CountVectorize](https://spark.apache.org/docs/latest/ml-features.html#countvectorizer) from pyspark doc).


----
## Datasets presentation :

### Training set :
From [Rotten Tomatoes dataset](https://www.kaggle.com/c/sentiment-analysis-on-movie-reviews)

Overview train.tsv :

|PhraseId|SentenceId|              Phrase|Sentiment|
|--------|----------|--------------------|---------|
|     293|        11|cliched dialogue and|        1|
|     292|        11|cliched dialogue ...|        1|
|      93|         3|                  's|        2|
|     165|         5|        manipulative|        2|
|     208|         7|is a plodding mess .|        1|

The sentiment labels are:
- 0 - negative
- 1 - somewhat negative
- 2 - neutral
- 3 - somewhat positive
- 4 - positive


### Testing set :
From [GroupLens Research](https://grouplens.org/datasets/movielens/).

[Download Dataset](http://files.grouplens.org/datasets/movielens/ml-latest-small.zip)

Overview movies.csv :

|movieId|               title   |              genres                           |
|-------|-----------------------|-----------------------------------------------|
|      1|Toy Story (1995)       |Adventure\|Animation\|Children\|Comedy\|Fantasy|
|      2|Jumanji (1995)         |Adventure\|Children\|Fantasy                   |
|      3|Grumpier Old Men (1995)|Comedy\|Romance                                |

Subtitles files will be downloaded using download\_subs.py that use
[subliminal](https://github.com/Diaoul/subliminal) python library.

srt files format :\
1\
00:00:14,482 --> 00:00:19,696\
All right, everyone! This... is a stickup!\
Don't anybody move!

2\
00:00:20,530 --> 00:00:22,720\
Now, empty that safe!

3\
00:00:24,597 --> 00:00:27,622\
Ooh-hoo-hoo! Money, money, money!

----
## Usage :
For debugging purpose :
> spark-submit spark_machine_learning --limit_movies 10

Help :
> spark-submit spark_machine_learning.py --help


----
# In detail explanation of spark_machine_learning.py

## Important parts of the code :

- Top level Class :
```
class DataTools:
    """
    - df_movies : pyspark.sql.DataFrame of movies.csv
                schema = ['movieId', 'title', 'genres', 'srt_path']

    - n_movies : <int> number of movies considered.

    - df_test : pyspark.sql.DataFrame of movies's content subtitles
                schema = ['movieId', 'text', 'words_regex', 'filtered']

    - df_train : pyspark.sql.DataFrame containing phrases from resume with
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
```
----

- Bag of words function :

```
def bag_of_words(df, nameInputCol="Phrase"):
    """
    Description :
    - RegexTokenizer : tokenizer according a regex
    - remove stopwords
    - remove duplicates in words_regex to delete empty token
    """
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
```
----

- Fit the CountVectorizer model with both datasets (training and testing) :

```
def fit_countVec(df_train, df_test, nameInputCol="filtered"):
    """
    Description :
    - union between df_train and df_test for the filtered column
    - fit the CountVectorizer model with all the words included
    """
    df_tmp = df_train.select(nameInputCol).union(
            df_test.select(nameInputCol))

    from pyspark.ml.feature import CountVectorizer
    CV = CountVectorizer(inputCol=nameInputCol, outputCol='features')
    model = CV.fit(df_tmp)

    return(model)
```
----

- Obtaining Sentiment Note :

```
    def classification(self, **kwargs):
        """
        Description :
        - fit the LogisticRegression model with the training set
        - predict values for the testing set
        - format a nice df_results
        """
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
```

## df show(5) gives at the end :

- df_movies :

|movieId|               title|            srt_path|
|-------|--------------------|--------------------|
|      1|    Toy Story (1995)|subtitles/Toy Sto...|
|      2|      Jumanji (1995)|subtitles/Jumanji...|
|      3|Grumpier Old Men ...|subtitles/Grumpie...|
|      4|Waiting to Exhale...|subtitles/Waiting...|
|      5|Father of the Bri...|subtitles/Father ...|

- df_train :

|              Phrase|Sentiment|         words_regex|            filtered|            features|
|--------------------|---------|--------------------|--------------------|--------------------|
|cliched dialogue and|        1|[cliched, dialogu...| [cliched, dialogue]|(4338,[536,543],[...|
|cliched dialogue ...|        1|[cliched, dialogu...|[cliched, dialogu...|(4338,[504,535,53...|
|                  's|        2|                ['s]|                ['s]|  (4338,[211],[1.0])|
|        manipulative|        2|      [manipulative]|      [manipulative]| (4338,[1016],[1.0])|
|is a plodding mess .|        1|[is, a, plodding,...|    [plodding, mess]|(4338,[586,890],[...|

- df_test :

|movieId|              Phrase|         words_regex|         filtered|            features|Sentiment_Predicted|
|-------|--------------------|--------------------|-----------------|--------------------|-------------------|
|      3|          And Carlo.|        [and, carlo]|          [carlo]| (4338,[1142],[1.0])|                2.0|
|      2|- Of course I hea...|[of, course, i, h...|  [course, heard]|(4338,[210,663],[...|                2.0|
|      2|the floor is quicker|[the, floor, is, ...| [floor, quicker]|(4338,[1033,3111]...|                2.0|
|      5|  - I found a thrill|[i, found, a, thr...|  [found, thrill]|(4338,[238,3402],...|                2.0|
|      5|But, George, we h...|[but, george, we,...|[george, haven't]|(4338,[19,170],[1...|                2.0|

- df_results :

|movieId|               title|             score|score_without_neutral|
|-------|--------------------|------------------|---------------------|
|      1|    Toy Story (1995)|2.0089974293059125|    2.608695652173913|
|      2|      Jumanji (1995)| 2.009450171821306|   2.7333333333333334|
|      3|Grumpier Old Men ...|2.0106951871657754|   2.5714285714285716|
|      4|Waiting to Exhale...|2.0104849279161208|   2.5517241379310347|
|      5|Father of the Bri...|2.0162852112676055|   2.7551020408163267|

