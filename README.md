[<img src="http://www.euridis-blog.com/wp-content/uploads/2017/02/TOP-BIG-DATA.jpg" width="500px" alt="logo" />](https://github.com/gjeusel/bigData_ELL890)

# EEL890 Big Data : [Spark](https://spark.apache.org/) Project

----
## Purpose : Classify film genres from subtitles

From .srt files :  
1  
00:00:14,482 --> 00:00:19,696  
All right, everyone! This... is a stickup!  
Don't anybody move!  

2  
00:00:20,530 --> 00:00:22,720  
Now, empty that safe!  

3  
00:00:24,597 --> 00:00:27,622  
Ooh-hoo-hoo! Money, money, money!  

Train a [Multilayer perceptron classifier](http://spark.apache.org/docs/latest/ml-classification-regression.html#multilayer-perceptron-classifier)
to identify the genre of the movie :  
Adventure\|Animation\|Children\|Comedy\|Fantasy ...


Natural Language Processing Model used : Bag-Of-Word  
[<img src="https://i.ytimg.com/vi/OGK9SHt8SWg/maxresdefault.jpg" width="200px" alt="logo" />](https://www.youtube.com/watch?v=OGK9SHt8SWg)



----
## Dataset :

From [GroupLens Research](https://grouplens.org/datasets/movielens/).

[Download Dataset](http://files.grouplens.org/datasets/movielens/ml-latest-small.zip)

Overview movies.csv :

|movieId|               title   |              genres                           |
|-------|-----------------------|-----------------------------------------------|
|      1|Toy Story (1995)       |Adventure\|Animation\|Children\|Comedy\|Fantasy|
|      2|Jumanji (1995)         |Adventure\|Children\|Fantasy                   |
|      3|Grumpier Old Men (1995)|Comedy\|Romance                                |


----
## Usage :
For debugging purpose :
> spark-submit spark_machine_learning --limit_movies 100

Help :
> spark-submit spark_machine_learning.py --help

usage: spark_machine_learning.py [-h] [--csv_dir CSV_DIR] [--srt_dir SRT_DIR]
                                 [--limit_movies LIMIT_MOVIES] [--log LOGFILE]
                                 [-v VERBOSE]
