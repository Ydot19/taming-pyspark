# Taming Pyspark

Completely the initial learning of running PySpark jobs locally and in a cluser 

## Environment Variables

```.env
BASE_DATA_PATH= # data directory relative to root project directory (ex. BASE_DATA_PATH=taming_pyspark/data)
MOVIE_LENS= 
FRIENDS=
TEMP_1800S=
WORD_COUNT=
CONSUMER_SPENDING=
HEROES=
```

- Based on the folder names in the data.zip file
- See [config.py](./taming_pyspark/config.py)

## Navigating taming_pyspark directory

- Section describes what each folder focuses on

### [**taming_pyspark/utils**](./taming_pyspark/utils)

Objective:
- Location for functions used in multiple locations



### [**taming_pyspark/book**](./taming_pyspark/book)

Objective: Count unique words in the word_count/Book.txt file in data.zip

| Files | Topic | Additional Notes |
| :--       | :--    | :--  |
| [word_count_rdd.py](./taming_pyspark/book/word_count_rdd.py) | `- RDD` <br /> `- flatMap` | - Data in the `word_count` in the data.zip file <br /> - Word count includes special characters|
| [word_count_df.py](./taming_pyspark/book/word_count_df.py) | `- Dataframes` <br /> `- RDD to Dataframes` <br /> `- Sorting` | - Data in the `word_count` in the data.zip file <br /> - Word count includes special characters |
| [word_count_regex_rdd.py](./taming_pyspark/book/word_count_regex_rdd.py) | `- RDD` <br /> `- Regular Expressions` <br /> `- Sorting` <br /> `- re module` | - Normalize words and remove special characters |
| [word_count_regex_df.py](./taming_pyspark/book/word_count_regex_df.py) | `- Dataframes` <br /> `- RDD to Dataframes` <br /> `- Regular Expressions` <br /> `- Sorting` <br /> `- re module` | - Normalize words and remove special characters |


### [**taming_pyspark/consumer_spending**](./taming_pyspark/consumer_spending)

Objective: 
- Use the customer-purchases/customer_orders.csv file in data.zip
- Find the total amount of spend by consumer id
- Sort by consumer id total spend


| Files | Topic | Additional Notes |
| :--       | :--    | :--  |
| [spending_df.py](./taming_pyspark/consumer_spending/spending_df.py) | `- Dataframes ` <br /> `- Aggregrate Functions ` | |
| [spending_rdd.py](./taming_pyspark/consumer_spending/spending_rdd.py) | `- RDDs ` <br /> `- reduceByKey` || 


### [**taming_pyspark/friends**](./taming_pyspark/friends)

Objective: 
- Use the friends-dataset/fakefriends.csv file in data.zip
- Psuedo friends/peers dataset
- Use pyspark to determine number of friends/peers by age

| Files | Topic | Additional Notes |
| :--       | :--    | :--  |
| [fake_friends_df.py](./taming_pyspark/friends/fake_friends_df.py) | `- Dataframes ` <br /> `- Aggregrate Functions ` | |
| [fake_friends_rdd.py](./taming_pyspark/friends/fake_friends_rdd.py) | `- RDDs ` <br /> `- reduceByKey ` <br /> `- Text to CSV Parsing` | |

### [**taming_pyspark/heroes**](./taming_pyspark/heroes)

Objective:
- Use the friends-dataset/fakefriends.csv file in data.zip 
- Determines the most popular hero based on how many uniquely different hours they have had an encounter with


| Files | Topic | Additional Notes |
| :--       | :--    | :--  |
| [most_popular_hero.py](./taming_pyspark/heroes/most_popular_hero.py) | `- udf (user-defined functions)` <br /> `- Dataframes` <br /> `- Adding Columns (withColumns)` <br /> `- RDD to DF`

### [**taming_pyspark/movies**](./taming_pyspark/movies)

Objective:
- Uses the data found in the ml-100k/ folder in data.zip
- Find the most watched movies
- Count and group movies by rating
- Recommend movie based on previous users using least squared algorithm

| Files | Topic | Additional Notes |
| :--       | :--    | :--  |
| [ratings_counter.py](./taming_pyspark/movies/ratings_counter.py) | `- RDD` <br /> `- Text to CSV Parsing` <br /> `- Ordered Dictionary` <br /> `- countByValue` ||
| [most_watched.py](./taming_pyspark/movies/most_watched.py) |  `- DataFrames` <br /> `- udf (user defined functions)` <br /> `- udf with multiple inputs` <br /> `- Passing Dictionary to UDF` ||
| [recommendation.py](./taming_pyspark/movies/recommendations.py) | `- Dataframes` <br />`- Complex Aggregations` <br /> `- Class Based PySpark Runs` ||


### [**taming_pyspark/streams**](./taming_pyspark/streams)

Objective:
- Introduction to PySpark Structured Streaming
- Log Ingestion for HTTP Requests
- Uses the streams/logs.txt file in data.zip to be the sample logs
- Run the stream and add a copy of the logs.txt file to the logs directory that gets created when running the script

| Files | Topic | Additional Notes |
| :--       | :--    | :--  |
| [stream_logs.py](./taming_pyspark/streams/stream_logs.py) | `- Structured Streaming` ||


### [**taming_pyspark/temp_1800s**](./taming_pyspark/temp_1800s)

Objective:
- Shows min weather temperature observed for each weather station by default

| Files | Topic | Additional Notes |
| :--       | :--    | :--  |
|[min_max_temp_df.py](./taming_pyspark/temp_1800s/min_max_temp_df.py)| `- DataFrames` <br /> `- filter`| |
|[min_max_temp_rdd.py](./taming_pyspark/temp_1800s/min_max_temp_rdd.py)| `- RDDs` <br /> `- filter` <br /> `- reduceByKey`| |


