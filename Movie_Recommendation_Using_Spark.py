#!/usr/bin/env python
# coding: utf-8

# In[ ]:


get_ipython().system('apt-get -y install openjdk-8-jre-headless')
get_ipython().system('pip install pyspark')
from pyspark.sql import SparkSession
from pyspark import SparkContext
spark = SparkSession.builder.master("local").getOrCreate()
sc = SparkContext.getOrCreate()


# In[ ]:


rdd = sc.parallelize(["Hello Spark"])
counts = rdd.flatMap(lambda line: line.split(" "))     .map(lambda word: (word, 1))     .reduceByKey(lambda a, b: a + b)     .collect()
print(counts)


# In[ ]:


from google.colab import drive

drive.mount('/content/drive')

get_ipython().system('ls "/content/drive/My Drive/Colab Notebooks/MSBD 5003"')

get_ipython().system('cp -r \'/content/drive/My Drive/Colab Notebooks/MSBD 5003/Data\' "./data"')

get_ipython().system('ls')


# In[ ]:


from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession


# In[ ]:


#Import data
IMDb_movies_df = spark.read.csv('./data/IMDb movies.csv', header=True, inferSchema=True)
IMDb_names_df = spark.read.csv('./data/IMDb names.csv', header=True, inferSchema=True)
IMDb_ratings_df = spark.read.csv('./data/IMDb ratings.csv', header=True, inferSchema=True)
IMDb_title_principals_df = spark.read.csv('./data/IMDb title_principals.csv', header=True, inferSchema=True)

netflix_titles_df = spark.read.csv('./data/netflix_titles.csv', header=True, inferSchema=True)

tmdb_5000_movies_df = spark.read.csv('./data/tmdb_5000_movies.csv', header=True, inferSchema=True)
tmdb_5000_credits_df = spark.read.csv('./data/tmdb_5000_credits.csv', header=True, inferSchema=True)

credits_df = spark.read.csv('./data/credits.csv', header=True, inferSchema=True)
keywords_df = spark.read.csv('./data/keywords.csv', header=True, inferSchema=True)
links_small_df = spark.read.csv('./data/links_small.csv', header=True, inferSchema=True)
links_df = spark.read.csv('./data/links.csv', header=True, inferSchema=True)
movies_metadata_df = spark.read.csv('./data/movies_metadata.csv', header=True, inferSchema=True)
ratings_small_df = spark.read.csv('./data/ratings_small.csv', header=True, inferSchema=True)
ratings_df = spark.read.csv('./data/ratings.csv', header=True, inferSchema=True)


# In[ ]:


movies_metadata_df.take(5)


# In[ ]:


#Join movielens data
movielens_df = movies_metadata_df.join(credits_df, 'id')                                 .join(keywords_df, 'id')


# In[ ]:


movielens_df.show()


# In[ ]:


#Join IMDB data with rating
IMDb_movies_df_2 = IMDb_movies_df.join(IMDb_ratings_df,'imdb_title_id')
IMDb_movies_df_2.take(5)


# In[ ]:


# Register the dataframe as a temporary view called IMDb_title_principals_temp
IMDb_title_principals_df.createOrReplaceTempView('IMDb_title_principals_temp')


# In[ ]:


#Split the title principles into different category
IMDb_actor = spark.sql('SELECT * FROM IMDb_title_principals_temp WHERE category="actor"')
IMDb_actress = spark.sql('SELECT * FROM IMDb_title_principals_temp WHERE category="actress"')
IMDb_archive_footage = spark.sql('SELECT * FROM IMDb_title_principals_temp WHERE category="archive_footage"')
IMDb_archive_sound = spark.sql('SELECT * FROM IMDb_title_principals_temp WHERE category="archive_sound"')
IMDb_cinematographer = spark.sql('SELECT * FROM IMDb_title_principals_temp WHERE category="cinematographer"')
IMDb_composer = spark.sql('SELECT * FROM IMDb_title_principals_temp WHERE category="composer"')
IMDb_director = spark.sql('SELECT * FROM IMDb_title_principals_temp WHERE category="director"')
IMDb_editor = spark.sql('SELECT * FROM IMDb_title_principals_temp WHERE category="editor"')
IMDb_producer = spark.sql('SELECT * FROM IMDb_title_principals_temp WHERE category="producer"')
IMDb_production_designer = spark.sql('SELECT * FROM IMDb_title_principals_temp WHERE category="production_designer"')
IMDb_self = spark.sql('SELECT * FROM IMDb_title_principals_temp WHERE category="self"')
IMDb_writer = spark.sql('SELECT * FROM IMDb_title_principals_temp WHERE category="writer"')
IMDb_actor.show()


# In[ ]:


# Register the dataframe as a temporary view called IMDb_names_temp
IMDb_names_df.createOrReplaceTempView('IMDb_names_temp')


# In[ ]:


IMDb_names = spark.sql('SELECT * FROM IMDb_names_temp WHERE imdb_name_id LIKE "nm%" ')
IMDb_names.take(5)


# In[ ]:


IMDb_names = spark.sql('SELECT imdb_name_id, name, height, substr(date_of_birth,1,4) as year_birth, REVERSE(SUBSTR(REVERSE(place_of_birth),1,LOCATE(" ,", REVERSE(place_of_birth))-1)) AS country_of_birth, spouses, divorces, spouses_with_children, children FROM IMDb_names_temp WHERE imdb_name_id LIKE "nm%" and substr(date_of_birth,1,4)>"1900" ')
IMDb_names.take(10)


# In[ ]:


IMDb_main_actor = spark.sql('SELECT a.imdb_title_id, min(a.ordering) as min_ordering FROM IMDb_title_principals_temp a WHERE category="actor" GROUP BY imdb_title_id')
IMDb_main_actor.take(5)


# In[ ]:


# Register the dataframe as a temporary view called IMDb_names_temp
IMDb_actor.createOrReplaceTempView('IMDb_actor_temp')
IMDb_main_actor.createOrReplaceTempView('IMDb_main_actor_temp')
IMDb_names.createOrReplaceTempView('IMDb_names_temp')


# In[ ]:


IMDb_main_actor_2 = spark.sql('SELECT a.imdb_title_id                             , b.imdb_name_id AS imdb_main_actor_id                             , c.name as imdb_main_actor_name                             , c.height as imdb_main_actor_height                             , c.year_birth as imdb_main_actor_year_birth                             , c.country_of_birth as imdb_main_actor_country_birth                             , c.spouses as imdb_main_actor_spouses                            , c.divorces as imdb_main_actor_divorces                             , c.spouses_with_children as imdb_main_actor_swc                             , c.children as imdb_main_actor_children                             FROM IMDb_main_actor_temp a                             LEFT JOIN IMDb_actor_temp b                             ON a.imdb_title_id=b.imdb_title_id                             AND a.min_ordering=b.ordering                             LEFT JOIN IMDb_names_temp c                             ON b.imdb_name_id=c.imdb_name_id                             ORDER BY c.height DESC')
IMDb_main_actor_2.take(10)


# In[ ]:


#Repeat the process for main actress
IMDb_main_actress = spark.sql('SELECT imdb_title_id                                , min(ordering) as min_ordering                                FROM IMDb_title_principals_temp                                WHERE category="actress"                                GROUP BY imdb_title_id')

IMDb_actress.createOrReplaceTempView('IMDb_actress_temp')
IMDb_main_actress.createOrReplaceTempView('IMDb_main_actress_temp')

IMDb_main_actress_2 = spark.sql('SELECT a.imdb_title_id                             , b.imdb_name_id AS imdb_main_actress_id                             , c.name as imdb_main_actress_name                             , c.height as imdb_main_actress_height                             , c.year_birth as imdb_main_actress_year_birth                             , c.country_of_birth as imdb_main_actress_country_birth                             , c.spouses as imdb_main_actress_spouses                            , c.divorces as imdb_main_actress_divorces                             , c.spouses_with_children as imdb_main_actress_swc                             , c.children as imdb_main_actress_children                             FROM IMDb_main_actress_temp a                             LEFT JOIN IMDb_actress_temp b                             ON a.imdb_title_id=b.imdb_title_id                             AND a.min_ordering=b.ordering                             LEFT JOIN IMDb_names_temp c                             ON b.imdb_name_id=c.imdb_name_id                             ORDER BY c.height DESC')

IMDb_main_actress_2.take(5)


# In[ ]:


# Register the dataframe as a temporary view
IMDb_movies_df_2.createOrReplaceTempView('IMDb_movies_df_2_temp')
IMDb_main_actor_2.createOrReplaceTempView('IMDb_main_actor_2_temp')
IMDb_main_actress_2.createOrReplaceTempView('IMDb_main_actress_2_temp')


# In[ ]:


#Link up the info for IMDb
IMDb_combined = spark.sql('SELECT a.*                             , b.imdb_main_actor_name                             , b.imdb_main_actor_height                             , b.imdb_main_actor_year_birth                             , b.imdb_main_actor_country_birth                             , b.imdb_main_actor_spouses                            , b.imdb_main_actor_divorces                             , b.imdb_main_actor_swc                             , b.imdb_main_actor_children                             , c.imdb_main_actress_id                             , c.imdb_main_actress_name                             , c.imdb_main_actress_height                             , c.imdb_main_actress_year_birth                             , c.imdb_main_actress_country_birth                             , c.imdb_main_actress_spouses                            , c.imdb_main_actress_divorces                             , c.imdb_main_actress_swc                             , c.imdb_main_actress_children                            FROM IMDb_movies_df_2_temp a                            LEFT JOIN IMDb_main_actor_2_temp b                            ON a.imdb_title_id=b.imdb_title_id                            LEFT JOIN IMDb_main_actress_2_temp c                            ON a.imdb_title_id=c.imdb_title_id')

IMDb_combined.take(5)


# In[ ]:


# Register the dataframe as a temporary view
movielens_df.createOrReplaceTempView('movielens_df_temp')
IMDb_combined.createOrReplaceTempView('IMDb_combined_temp')


# In[ ]:


# Join data over movielens and IMDB
movielens_IMDb = spark.sql('SELECT a.*                             , b.year as imdb_year                             , b.genre as imdb_genre                             , b.director as imdb_director                             , b.writer as imdb_writer                             , b.avg_vote as imdb_avg_vote                             , b.votes as imdb_votes                             , b.metascore as imdb_metascore                             , b.reviews_from_users as imdb_reviews_from_users                             , b.reviews_from_critics as imdb_reviews_from_critics                             , b.weighted_average_vote                             , b.total_votes                             , b.mean_vote                             , b.median_vote                             , b.votes_10                             , b.votes_9                             , b.votes_8                             , b.votes_7                             , b.votes_6                             , b.votes_5                             , b.votes_4                             , b.votes_3                             , b.votes_2                             , b.votes_1                             , b.allgenders_0age_avg_vote                             , b.allgenders_0age_votes                             , b.allgenders_18age_avg_vote                             , b.allgenders_18age_votes                             , b.allgenders_30age_avg_vote                             , b.allgenders_30age_votes                             , b.allgenders_45age_avg_vote                             , b.allgenders_45age_votes                             , b.males_allages_avg_vote                             , b.males_allages_votes                             , b.males_0age_avg_vote                             , b.males_0age_votes                             , b.males_18age_avg_vote                             , b.males_18age_votes                             , b.males_30age_avg_vote                             , b.males_30age_votes                             , b.males_45age_avg_vote                             , b.males_45age_votes                             , b.females_allages_avg_vote                             , b.females_allages_votes                             , b.females_0age_avg_vote                             , b.females_0age_votes                             , b.females_18age_avg_vote                             , b.females_18age_votes                             , b.females_30age_avg_vote                             , b.females_30age_votes                             , b.females_45age_avg_vote                             , b.females_45age_votes                             , b.top1000_voters_rating                             , b.top1000_voters_votes                             , b.us_voters_rating                             , b.us_voters_votes                             , b.non_us_voters_rating                             , b.non_us_voters_votes                             , b.imdb_main_actor_name                             , b.imdb_main_actor_height                             , b.imdb_main_actor_year_birth                             , b.imdb_main_actor_country_birth                             , b.imdb_main_actor_spouses                            , b.imdb_main_actor_divorces                             , b.imdb_main_actor_swc                             , b.imdb_main_actor_children                             , b.imdb_main_actress_id                             , b.imdb_main_actress_name                             , b.imdb_main_actress_height                             , b.imdb_main_actress_year_birth                             , b.imdb_main_actress_country_birth                             , b.imdb_main_actress_spouses                            , b.imdb_main_actress_divorces                             , b.imdb_main_actress_swc                             , b.imdb_main_actress_children                            FROM movielens_df_temp a                            LEFT JOIN IMDb_combined_temp b                            ON a.imdb_id=b.imdb_title_id')

movielens_IMDb.take(5)


# In[ ]:


movielens_IMDb.columns


# In[ ]:


ratings_df.show()


# In[ ]:


# Register the dataframe as a temporary view
movielens_IMDb.createOrReplaceTempView('movielens_IMDb_temp')
links_small_df.createOrReplaceTempView('links_temp')
ratings_df.createOrReplaceTempView('ratings_temp')


# In[ ]:


# Combine ratings and movie details via links
movie_rating = spark.sql('SELECT a.*, c.*                           FROM ratings_temp a                           LEFT JOIN links_temp b                           ON a.movieId=b.movieId                           LEFT JOIN movielens_IMDb_temp c                           ON b.tmdbId=c.id')

movie_rating.take(5)


# In[ ]:


#Check whether there are duplicate records
spark.sql('SELECT userId, movieId, COUNT(*) as rating_count FROM ratings_temp GROUP BY userId, movieId HAVING COUNT(*)>1').show()


# In[ ]:


#Remove number of users who rated fewer than 3 movies and movies with less than 3 ratings
ratings_df_2 = spark.sql('SELECT * FROM ratings_temp WHERE userId IN (SELECT userId FROM ratings_temp GROUP BY userId HAVING COUNT(*)>=3) and movieId IN (SELECT movieId FROM ratings_temp GROUP BY movieId HAVING COUNT(*)>=3)')
ratings_df_2.show()


# In[ ]:


#Convert ratings_df_2 from Dataframe to RDD
ratings_rdd = ratings_df_2.rdd.map(list)


# In[ ]:


ratings_rdd.take(5)


# In[ ]:


# Check the number of ratings by each users
Rating_no_by_users = ratings_rdd.map(lambda record: (record[0], 1)).reduceByKey(lambda x, y: x + y)
Rating_no_by_users.take(10)


# In[ ]:


import plotly
plotly.__version__


# In[ ]:


from __future__ import print_function #python 3 support
print(sc)


# In[ ]:


from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


# In[ ]:


get_ipython().system('pip install chart_studio')


# In[ ]:


#import plotly.plotly as py
import chart_studio.plotly as py
import plotly.graph_objs as go
import pandas as pd
import requests
requests.packages.urllib3.disable_warnings()

from plotly.offline import init_notebook_mode, iplot
#init_notebook_mode()


# In[ ]:


X = Rating_no_by_users.map(lambda p: p[0])
Y = Rating_no_by_users.map(lambda p: p[1])
X.take(5)
Y.take(5)


# In[ ]:


def enable_plotly_in_cell():
  import IPython
  from plotly.offline import init_notebook_mode
  display(IPython.core.display.HTML('''<script src="/static/components/requirejs/require.js"></script>'''))
  init_notebook_mode(connected=False)


# In[ ]:


#test
enable_plotly_in_cell()
fig = go.Figure(go.Scatter(x=[1,2,3], y=[1,3,2] ) )
fig.show()

#iplot(fig)
#plotly.offline.iplot(fig)


# In[ ]:


enable_plotly_in_cell()

trace = go.Histogram(x = ratings_df_2.toPandas()['rating'])
data = [trace]
#py.plot(data)

plotly.offline.iplot(data)


# In[ ]:


# Register the dataframe as a temporary view called ratings_temp
ratings_df_2.createOrReplaceTempView('ratings_temp_2')


# In[ ]:


#Plot the no. of ratings by users
rating_no_by_user = spark.sql('SELECT userId, COUNT(*) AS rating_no_by_user FROM ratings_temp_2 GROUP BY userId')

enable_plotly_in_cell()

trace = go.Histogram(x = rating_no_by_user.toPandas()['rating_no_by_user'])
data = [trace]
#py.plot(data)

plotly.offline.iplot(data)


# In[ ]:


#Plot the no. of ratings by movies
rating_no_by_movie = spark.sql('SELECT movieId, COUNT(*) AS rating_no_by_movie FROM ratings_temp_2 GROUP BY movieId')

enable_plotly_in_cell()

trace = go.Histogram(x = rating_no_by_movie.toPandas()['rating_no_by_movie'])
data = [trace]
#py.plot(data)

plotly.offline.iplot(data)


# In[ ]:


# Register the dataframe as a temporary view called ratings_temp
ratings_df_2.createOrReplaceTempView('ratings_temp_2')


# In[ ]:


#Plot the average ratings by movies
avg_rating_by_movie = spark.sql('SELECT movieId, avg(rating) AS avg_rating_by_movie FROM ratings_temp_2 GROUP BY movieId')

enable_plotly_in_cell()

trace = go.Histogram(x = avg_rating_by_movie.toPandas()['avg_rating_by_movie'])
data = [trace]
#py.plot(data)

plotly.offline.iplot(data)


# In[ ]:


# Register the dataframe as a temporary view called ratings_temp
ratings_df_2.createOrReplaceTempView('ratings_temp_2')


# In[ ]:


# Register the dataframe as a temporary view called ratings_temp
ratings_df.createOrReplaceTempView('ratings_temp')


# In[ ]:


#Remove number of users who rated fewer than 3 movies and movies with less than 3 ratings
ratings_df_2 = spark.sql('SELECT * FROM ratings_temp WHERE userId IN (SELECT userId FROM ratings_temp GROUP BY userId HAVING COUNT(*)>=3) and movieId IN (SELECT movieId FROM ratings_temp GROUP BY movieId HAVING COUNT(*)>=3)')
ratings_df_2.show()


# In[ ]:


# Register the dataframe as a temporary view called ratings_temp
ratings_df_2.createOrReplaceTempView('ratings_2_temp')


# In[ ]:


#Plot the average ratings by movies
avg_rating_by_movie = spark.sql('SELECT movieId, avg(rating) AS avg_rating_by_movie FROM ratings_temp GROUP BY movieId')

enable_plotly_in_cell()

trace = go.Histogram(x = avg_rating_by_movie.toPandas()['avg_rating_by_movie'])
data = [trace]
#py.plot(data)

plotly.offline.iplot(data)


# In[ ]:


#Plot the average ratings by movies
avg_rating_by_movie = spark.sql('SELECT movieId, avg(rating) AS avg_rating_by_movie FROM ratings_2_temp GROUP BY movieId')

enable_plotly_in_cell()

trace = go.Histogram(x = avg_rating_by_movie.toPandas()['avg_rating_by_movie'])
data = [trace]
#py.plot(data)

plotly.offline.iplot(data)


# In[ ]:


# Register the dataframe as a temporary view called ratings_temp
movielens_IMDb.createOrReplaceTempView('movielens_IMDb_temp')


# In[ ]:


#Plot the average ratings by movies
movie_by_year = spark.sql('SELECT imdb_year, count(*) AS movie_by_year FROM movielens_IMDb_temp where imdb_year is NOT NULL GROUP BY imdb_year')

enable_plotly_in_cell()

trace = go.Histogram(x = movie_by_year.toPandas()['movie_by_year'])
data = [trace]
#py.plot(data)

plotly.offline.iplot(data)


# In[ ]:


enable_plotly_in_cell()

trace = go.Bar(x = movie_by_year.toPandas()['imdb_year'], y = movie_by_year.toPandas()['movie_by_year'])
data = [trace]
#py.plot(data)

plotly.offline.iplot(data)


# In[ ]:


#Plot the average ratings by movies
actor_height = spark.sql('SELECT females_allages_avg_vote,imdb_main_actor_height FROM movielens_IMDb_temp where females_allages_avg_vote is NOT NULL and imdb_main_actor_height is NOT NULL and imdb_main_actor_height>"140"')

enable_plotly_in_cell()

trace = go.Scatter(x = actor_height.toPandas()['imdb_main_actor_height'], y = actor_height.toPandas()['females_allages_avg_vote'],mode='markers')
data = [trace]
#py.plot(data)

plotly.offline.iplot(data)


# In[ ]:


#Plot the average ratings by movies
actor_height = spark.sql('SELECT males_allages_avg_vote,imdb_main_actor_height FROM movielens_IMDb_temp where males_allages_avg_vote is NOT NULL and imdb_main_actor_height is NOT NULL and imdb_main_actor_height>"140"')

enable_plotly_in_cell()

trace = go.Scatter(x = actor_height.toPandas()['imdb_main_actor_height'], y = actor_height.toPandas()['males_allages_avg_vote'],mode='markers')
data = [trace]
#py.plot(data)

plotly.offline.iplot(data)


# In[ ]:


#Plot the average ratings by movies
actor_height = spark.sql('SELECT (females_allages_avg_vote-males_allages_avg_vote) AS diff ,imdb_main_actor_height FROM movielens_IMDb_temp where males_allages_avg_vote is NOT NULL and females_allages_avg_vote is NOT NULL and males_allages_avg_vote is NOT NULL and imdb_main_actor_height>"140"')

enable_plotly_in_cell()

trace = go.Scatter(x = actor_height.toPandas()['imdb_main_actor_height'], y = actor_height.toPandas()['diff'],mode='markers')
data = [trace]
#py.plot(data)

plotly.offline.iplot(data)


# In[ ]:


#Plot the average ratings by movies
actor_height = spark.sql('SELECT females_18age_avg_vote,imdb_main_actor_height FROM movielens_IMDb_temp where females_18age_avg_vote is NOT NULL and imdb_main_actor_height is NOT NULL and imdb_main_actor_height>"140"')

enable_plotly_in_cell()

trace = go.Scatter(x = actor_height.toPandas()['imdb_main_actor_height'], y = actor_height.toPandas()['females_18age_avg_vote'],mode='markers')
data = [trace]
#py.plot(data)

plotly.offline.iplot(data)


# In[ ]:


#Plot the average ratings by movies
actor_height = spark.sql('SELECT females_30age_avg_vote,imdb_main_actor_height FROM movielens_IMDb_temp where females_30age_avg_vote is NOT NULL and imdb_main_actor_height is NOT NULL and imdb_main_actor_height>"140"')

enable_plotly_in_cell()

trace = go.Scatter(x = actor_height.toPandas()['imdb_main_actor_height'], y = actor_height.toPandas()['females_30age_avg_vote'],mode='markers')
data = [trace]
#py.plot(data)

plotly.offline.iplot(data)


# In[ ]:


#Plot the average ratings by movies
actor_height = spark.sql('SELECT females_45age_avg_vote,imdb_main_actor_height FROM movielens_IMDb_temp where females_45age_avg_vote is NOT NULL and imdb_main_actor_height is NOT NULL and imdb_main_actor_height>"140"')

enable_plotly_in_cell()

trace = go.Scatter(x = actor_height.toPandas()['imdb_main_actor_height'], y = actor_height.toPandas()['females_45age_avg_vote'],mode='markers')
data = [trace]
#py.plot(data)

plotly.offline.iplot(data)


# In[ ]:


#Plot the average ratings by movies
actor_height = spark.sql('SELECT females_allages_avg_vote, males_allages_avg_vote FROM movielens_IMDb_temp where females_allages_avg_vote is NOT NULL and males_allages_avg_vote is NOT NULL ')

enable_plotly_in_cell()

trace = go.Scatter(x = actor_height.toPandas()['males_allages_avg_vote'], y = actor_height.toPandas()['females_allages_avg_vote'],mode='markers')
data = [trace]
#py.plot(data)

plotly.offline.iplot(data)


# In[ ]:


#Plot the average ratings by movies
actor_height = spark.sql('SELECT females_allages_avg_vote, males_allages_avg_vote FROM movielens_IMDb_temp where females_allages_avg_vote is NOT NULL and males_allages_avg_vote is NOT NULL ')

enable_plotly_in_cell()

trace = go.Scatter(x = actor_height.toPandas()['males_allages_avg_vote'], y = actor_height.toPandas()['females_allages_avg_vote'],mode='markers')
data = [trace]
#py.plot(data)

plotly.offline.iplot(data)


# In[ ]:


#Plot the average ratings by movies
actor_height = spark.sql('SELECT us_voters_rating, non_us_voters_rating FROM movielens_IMDb_temp where us_voters_rating is NOT NULL and non_us_voters_rating is NOT NULL ')

enable_plotly_in_cell()

trace = go.Scatter(x = actor_height.toPandas()['us_voters_rating'], y = actor_height.toPandas()['non_us_voters_rating'],mode='markers')
data = [trace]
#py.plot(data)

plotly.offline.iplot(data)


# In[ ]:


#Plot the average ratings by movies
actor_height = spark.sql('SELECT us_voters_rating, non_us_voters_rating FROM movielens_IMDb_temp where us_voters_rating is NOT NULL and non_us_voters_rating is NOT NULL ')

enable_plotly_in_cell()

trace = go.Scatter(x = actor_height.toPandas()['us_voters_rating'], y = actor_height.toPandas()['non_us_voters_rating'],mode='markers')
data = [trace]
#py.plot(data)

plotly.offline.iplot(data)


# In[ ]:


#Plot the average ratings by movies
gender_diff = spark.sql('SELECT original_title, imdb_genre, females_allages_avg_vote, males_allages_avg_vote, (females_allages_avg_vote-males_allages_avg_vote) AS gender_diff FROM movielens_IMDb_temp where females_allages_avg_vote is NOT NULL and males_allages_avg_vote is NOT NULL AND imdb_year>"2000" ORDER BY (females_allages_avg_vote-males_allages_avg_vote) ')
gender_diff.take(10)


# In[ ]:


#Plot the average ratings by movies
gender_diff = spark.sql('SELECT original_title, imdb_genre, females_allages_avg_vote, males_allages_avg_vote, (females_allages_avg_vote-males_allages_avg_vote) AS gender_diff FROM movielens_IMDb_temp where females_allages_avg_vote is NOT NULL and males_allages_avg_vote is NOT NULL AND imdb_year>"2000" ORDER BY (females_allages_avg_vote-males_allages_avg_vote) DESC')
gender_diff.take(10)


# In[ ]:


#Plot the average ratings by movies
gender_diff = spark.sql('SELECT imdb_genre, avg(females_allages_avg_vote) AS FEMALE, avg(males_allages_avg_vote) AS MALE, avg(females_allages_avg_vote-males_allages_avg_vote) AS gender_diff FROM movielens_IMDb_temp where females_allages_avg_vote is NOT NULL and males_allages_avg_vote is NOT NULL AND imdb_year>"2000" GROUP BY imdb_genre ORDER BY avg(females_allages_avg_vote-males_allages_avg_vote) ')
gender_diff.take(10)


# In[ ]:


#Plot the average ratings by movies
#gender_diff = spark.sql('SELECT imdb_genre, avg(females_allages_avg_vote) AS FEMALE, avg(males_allages_avg_vote) AS MALE, avg(females_allages_avg_vote-males_allages_avg_vote) AS gender_diff FROM movielens_IMDb_temp where females_allages_avg_vote is NOT NULL and males_allages_avg_vote is NOT NULL AND imdb_year>"2000" GROUP BY imdb_genre ORDER BY avg(females_allages_avg_vote-males_allages_avg_vote) ')
gender_diff.collect()


# 

# In[ ]:


from pyspark.sql.functions import split, regexp_replace

movielens_df_2 = movielens_df.withColumn(
    "col3",
    split(regexp_replace("genres", "^/|/$", ""), ", ")
)
movielens_df_2.show()


# In[ ]:


#Convert ratings_df_2 from Dataframe to RDD
movielens_rdd = movielens_df.rdd.map(list)
movielens_rdd.take(1)


# In[ ]:


# Distribution of Genres
import ast

genres_rdd = movielens_rdd.map(lambda line: ast.literal_eval(line[4]))                          .flatMap(lambda x: list(x))                          .map(lambda x: x['name'])                          .map(lambda x: (x, 1))                          .reduceByKey(lambda x, y: x + y)                          .sortBy(lambda x: x[1], False)
genres_rdd.collect()


# In[ ]:


# Register the dataframe as a temporary view called ratings_temp
movielens_df.createOrReplaceTempView('movielens_temp')


# In[ ]:


#Language
langauage_by_movie = spark.sql('SELECT original_language, count(*) AS no_movie FROM movielens_temp GROUP BY original_language ORDER BY count(*) DESC LIMIT 20')

enable_plotly_in_cell()

trace = go.Bar(x = langauage_by_movie.toPandas()['original_language'], y = langauage_by_movie.toPandas()['no_movie'])
data = [trace]
#py.plot(data)

plotly.offline.iplot(data)


# # Movie recommendation system

# In[ ]:


movielens_IMDb_subset = movielens_IMDb.select("title","overview","imdb_director","imdb_genre").distinct()


# Content-based recommender using Tf-Idf Vectorizer and Count Vectorizer

# In[ ]:


from sklearn.feature_extraction.text import TfidfVectorizer


# For Movie Lens

# In[ ]:


#Convert the spark dataframe to pandas dataframe
movielens_IMDb_subset_pandas = movielens_IMDb_subset.select("*").toPandas()


# In[ ]:


#removing stopwords
tfidf = TfidfVectorizer(stop_words='english')

#Replace NaN with an empty string
movielens_IMDb_subset_pandas['overview'] = movielens_IMDb_subset_pandas['overview'].fillna('')

#Construct the required TF-IDF matrix by fitting and transforming the data
tfidf_matrix = tfidf.fit_transform(movielens_IMDb_subset_pandas['overview'])

#Output the shape of tfidf_matrix
tfidf_matrix.shape


# There are about 61674 words described for the 30369 movies in this dataset.

# In[ ]:


movielens_IMDb_subset_pandas.count


# In[ ]:


# Import linear_kernel instead of cosine_similarity
from sklearn.metrics.pairwise import linear_kernel

# Compute the cosine similarity matrix
cosine_sim = linear_kernel(tfidf_matrix, tfidf_matrix)

indices = pd.Series(movielens_IMDb_subset_pandas.index, index=movielens_IMDb_subset_pandas['title']).drop_duplicates()


# In[ ]:


def get_recommendations(title, cosine_sim=cosine_sim):
    idx = indices[title]

    # Get the pairwsie similarity scores of all movies with that movie
    sim_scores = list(enumerate(cosine_sim[idx]))

    # Sort the movies based on the similarity scores
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)

    # Get the scores of the 10 most similar movies
    sim_scores = sim_scores[1:11]

    # Get the movie indices
    movie_indices = [i[0] for i in sim_scores]

    # Return the top 10 most similar movies
    return movielens_IMDb_subset_pandas['title'].iloc[movie_indices]


# In[ ]:


get_recommendations('The Santa Clause 2')


# Content based filtering on multiple metrics

# In[ ]:


filledna=movielens_IMDb_subset_pandas.fillna('')
filledna.head(1)


# In[ ]:


filledna[(filledna['title']=='The Santa Clause 2')]


# In[ ]:


#remove spacing and lower capitalize value
def clean_data(x):
        return str.lower(x.replace(" ", ""))


# In[ ]:


#features=['title','director','cast','listed_in','description']
features=['title','overview','imdb_director','imdb_genre']
filledna=filledna[features]


# In[ ]:


filledna.head(2)


# In[ ]:


for feature in features:
    filledna[feature] = filledna[feature].apply(clean_data)
    
filledna.head(2)


# In[ ]:


#Link field values together
def create_soup(x):
    return x['title']+ ' ' + x['overview'] + ' ' + x['imdb_director'] + ' ' + x['imdb_genre']


# In[ ]:


filledna['soup'] = filledna.apply(create_soup, axis=1)


# In[ ]:


filledna.head(2)


# In[ ]:


from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

count = CountVectorizer(stop_words='english')
count_matrix = count.fit_transform(filledna['soup'])

cosine_sim2 = cosine_similarity(count_matrix, count_matrix)
filledna=filledna.reset_index()
indices = pd.Series(filledna.index, index=filledna['title'])


# In[ ]:


def get_recommendations_new(title, cosine_sim=cosine_sim2):
    title=title.replace(' ','').lower()
    idx = indices[title]

    # Get the pairwsie similarity scores of all movies with that movie
    sim_scores = list(enumerate(cosine_sim2[idx]))

    # Sort the movies based on the similarity scores
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)

    # Get the scores of the 10 most similar movies
    sim_scores = sim_scores[1:11]

    # Get the movie indices
    movie_indices = [i[0] for i in sim_scores]

    # Return the top 10 most similar movies
    return movielens_IMDb_subset_pandas['title'].iloc[movie_indices]


# In[ ]:


get_recommendations_new('The Santa Clause 2',cosine_sim2)


# Collborative Filtering (MLlib - ALS)

# In[ ]:


from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row


# In[ ]:


#import ratings data
file_location = "/content/drive/My Drive/Colab Notebooks/MSBD 5003/Data/ratings.csv"
file_type = "csv"

# CSV options
infer_schema = "True"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
ratings = spark.read.format(file_type)   .option("inferSchema", infer_schema)   .option("header", first_row_is_header)   .option("sep", delimiter)   .load(file_location)

display(ratings)

# Create Temporary Tables
ratings.createOrReplaceTempView("ratings")


# In[ ]:


ratings.show(5)


# In[ ]:


als = ALS( userCol="userId", itemCol="movieId", ratingCol="rating",
          coldStartStrategy="drop", nonnegative = True, implicitPrefs = False)


# In[ ]:


from pyspark.ml.tuning import ParamGridBuilder, CrossValidator 

# Hyper parameters tuning
param_grid = ParamGridBuilder()             .addGrid(als.rank, [10, 50, 75, 100])             .addGrid(als.maxIter, [5, 50, 75, 100])             .addGrid(als.regParam, [.01, .05, .1, .15])             .build()


# In[ ]:


# Define evaluator as RMSE
evaluator = RegressionEvaluator(metricName = "rmse", 
                                labelCol = "rating", 
                                predictionCol = "prediction")
# Print length of evaluator
print ("Num models to be tested using param_grid: ", len(param_grid))


# In[ ]:


# Build cross validation using CrossValidator - 5 Folds are used
cv = CrossValidator(estimator = als, 
                    estimatorParamMaps = param_grid, 
                    evaluator = evaluator, 
                    numFolds = 5)


# In[ ]:


(training, test) = ratings.randomSplit([0.8, 0.2])


# In[ ]:


model = als.fit(training)


# In[ ]:


model = cv.fit(training)


# In[ ]:


predictions = model.transform(test)


# In[ ]:


predictions.show(n = 10)


# In[ ]:


# Evaluate the model by computing the RMSE on the test data
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))


# In[ ]:


# Generate top 10 movie recommendations for each user
userRecs = model.recommendForAllUsers(10)
# Generate top 10 user recommendations for each movie
movieRecs = model.recommendForAllItems(10)


# In[ ]:


#Display top 10 movies recommended to the user
userRecs.take(1)


# In[ ]:


#Display the top 10 users that the movie is recommended to them
movieRecs.take(1)


# #Streaming

# **Install BeautifulSoup and Set up Webdriver**

# In[ ]:


get_ipython().system('pip install selenium')
get_ipython().system('pip install BeautifulSoup4')

get_ipython().system('apt-get update # to update ubuntu to correctly run apt install')
get_ipython().system('apt install chromium-chromedriver')
get_ipython().system('cp /usr/lib/chromium-browser/chromedriver /usr/bin')
import sys
sys.path.insert(0,'/usr/lib/chromium-browser/chromedriver')

import tweepy, json
from selenium import webdriver
from bs4 import BeautifulSoup
import pandas as pd

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome('chromedriver',options=chrome_options)


# **Extract the Movies in Theatre**

# In[ ]:


driver.get('https://www.cinemacity.com.hk/en/movie/nowshowing')
content = driver.page_source
soup = BeautifulSoup(content, 'html.parser')
movie=[]
Director=[]
Cast =[]
i = 0

for a in soup.findAll(attrs={'class':'col-lg-12 movieName'}):
    movie.append(a.text)
    
for a in soup.findAll(attrs={'class':'text-c ellipsis'}):
    if i % 2 == 0:
        Director.append(a.text)
    else:
        Cast.append(a.text)
    i+=1

new_movie_df = pd.DataFrame({'Movie Name':movie, 'Director':Director, 'Cast':Cast})


# **Extract the Available Seats and Other Information for the new movies**

# In[ ]:


chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome('chromedriver',options=chrome_options)


# In[ ]:


count = 0
for i in range(10109207, 10109232):
  seat =[]
  driver.get('https://www.cinemacity.com.hk/en/ticketing/seatplan/' + str(i))
  content2 = driver.page_source
  soup2 = BeautifulSoup(content2, 'html.parser')

  for seat_plan in soup2.findAll(attrs={'class':'seatplan'}):
      for a in seat_plan.findAll(attrs={'class':'block normal available '}):
        seat.append(a['data-seatdisplay'])

  for movie_info in soup2.findAll(attrs={'class':'show_info desktop_view'}):
    for b in movie_info.findAll(attrs={'class':'movie_title'}): movie_title = b.text
    for b in movie_info.findAll(attrs={'class':'movie_duration'}): 
      for c in b.findAll(attrs={'class':'duration'}): duration = c.text
    for b in movie_info.findAll(attrs={'class':'house'}): 
      house = b.text
      break
    for b in movie_info.findAll(attrs={'class':'movie_showtime'}):
      for c in b.findAll(attrs={'class':'showDate'}): showDate = c.text
      for c in b.findAll(attrs={'class':'showTime'}): showTime = c.text

  #df = pd.DataFrame({'Seat': seat})
  str1 = " ".join(str(x) for x in seat)
  df2 = pd.DataFrame({'Movie': movie_title, 'Duration': duration, 'House': house, 'showDate': showDate, 'showTime': showTime, 'seat': str1}, index=[i])
  if count == 0:
    movie_on_show_df = df2
  else:
    movie_on_show_df = movie_on_show_df.append(df2)
  count += 1


# **Recommendation using Tf-Idf**

# In[ ]:


from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer



def clean_data(x):
        return str.lower(x.replace(" ", ""))

def create_bag_of_word(x):
    return x['Movie Name'] #+ ' ' + x['Director'] +' ' + x['Cast']

def get_recommendations_tfidf(title):
    filledna2=new_movie_df.fillna('')
    title=title.replace(' ','').lower()
    
    features2=['Movie Name'] #,'Director','Cast']
    tmp_df = pd.DataFrame({"Movie Name": title},index=[99])
    filledna2=filledna2[features2].append(tmp_df)
    for feature in features2:
        filledna2[feature] = filledna2[feature].apply(clean_data)

    filledna2['bag_of_words'] = filledna2.apply(create_bag_of_word, axis=1)

    tfidf = TfidfVectorizer(stop_words='english')
    tfidf_matrix2 = tfidf.fit_transform(filledna2['bag_of_words'])

    cosine_sim2 = cosine_similarity(tfidf_matrix2, tfidf_matrix2)

    filledna2=filledna2.reset_index()
    indices2 = pd.Series(filledna2.index, index=filledna2['Movie Name'])

    idx = indices2[title]
    sim_scores = list(enumerate(cosine_sim2[idx]))
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
    sim_scores = sim_scores[1:6]
    movie_indices2 = [i[0] for i in sim_scores]

    return new_movie_df['Movie Name'].iloc[movie_indices2]


# **Load Streaming**

# In[ ]:


from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


# In[ ]:



# Auxiliar functions
def equivalent_type(f):
    if f == 'datetime64[ns]': return TimestampType()
    elif f == 'int64': return LongType()
    elif f == 'int32': return IntegerType()
    elif f == 'float64': return FloatType()
    else: return StringType()

def define_structure(string, format_type):
    try: typo = equivalent_type(format_type)
    except: typo = StringType()
    return StructField(string, typo)

# Given pandas dataframe, it will return a spark's dataframe.
def pandas_to_spark(pandas_df):
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types): 
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return sqlContext.createDataFrame(pandas_df, p_schema)


# In[ ]:


#Dummy User Info and Preference
UserID = 1
max_value = ratings_df.filter("userID = " + str(UserID)).agg({"rating": "max"}).collect()[0][0]
user_df = ratings_df.filter("userID = " + str(UserID))          .where("rating = " +str(max_value))          .select("userID", "movieId", "rating")          .withColumnRenamed('movieId', 'id')
User_favourite_df = user_df.join(movielens_df,"id")                            .select('original_title','cast','crew')


# In[ ]:


filledna=User_favourite_df.fillna('')
datacollect = filledna.collect()
soup = ""
for row in datacollect:
  soup = soup + ' ' + row['original_title']


# In[ ]:


get_recommendations_tfidf(soup)


# In[ ]:


# Create a StreamingContext with batch interval of 5 seconds
ssc = StreamingContext(sc, 5)

tmp_df = pandas_to_spark(movie_on_show_df.loc[movie_on_show_df['Movie'] == get_recommendations_tfidf(soup)[0]])
rdd = tmp_df.rdd
rddQueue = rdd.randomSplit([1]*10, 123)
lines = ssc.queueStream(rddQueue)

movies_sorted = lines.transform(lambda rdd: rdd.sortBy(lambda x: x[1], False))

def printResults(rdd):
    print("The recommended movies:")
    print(rdd.take(10))

movies_sorted.foreachRDD(printResults)

ssc.start()  # Start the computation
ssc.awaitTermination(25)  # Wait for the computation to terminate
ssc.stop(False)
print("Finished")


# Create GraphFrames for Seat Selection

# In[ ]:


sc.addPyFile("../graphframes-0.8.0-spark3.0-s_2.12.jar")
from graphframes import *
from pyspark.sql.functions import *


# In[ ]:


#Initialize

# Vertics DataFrame
result = []

for i in range(20):
    for j in "ABCDEFGHIJKLMNOPQRST":
        result.append([str(j) + str(i+1), ord(j)-64])
df1 = spark.createDataFrame(result,['id', 'Row'])

# Edges DataFrame
result2 = []

for j in "ABCDEFGHIJKLMNOPQRST":
    for i in range(20-1):
        result2.append([str(j) + str(i+1), str(j) + str(i+2), "neighbour"])
        
for j in "ABCDEFG":
    for i in range(20-1):
        result2.append([str(j) + str(i+1), str(chr(ord(j)+1)) + str(i+1), "row_neighbour"])
        
df2 = spark.createDataFrame(result2,["src", "dst", "relationship"])


# In[ ]:


tmp =[]
available_seat = movie_on_show_df.loc[movie_on_show_df['Movie'] == get_recommendations_tfidf(soup)[0]]['seat'].iloc[0]
for i in available_seat:
    tmp.append([str(i), "available"])
df3 = spark.createDataFrame(tmp,['id','availability'])

available_seat_df = df1.join(df3,"id").select("id","Row")
tmp_df = df2.join(df3).filter("src=id").select("src", "dst","relationship").distinct()
available_seat_edges_df = tmp_df.join(df3).filter("dst=id").select("src", "dst","relationship").distinct()


# In[ ]:


# Create a GraphFrame
g = GraphFrame(available_seat_df, available_seat_edges_df)

g.vertices.show()
g.edges.show()


# In[ ]:


#Assume two people and Distance is within 5 and 10

e1 = g.find("(a)-[e1]->(b)").filter("e1.relationship='neighbour'").filter("a.row>5").filter("a.row<10")
e2 = e1.select("a", "b").distinct()
e2.show()


# ## **Mongo DB**
# 

# In[ ]:


get_ipython().system('apt-get install openjdk-8-jdk-headless -qq > /dev/null')
get_ipython().system('wget -q https://www-us.apache.org/dist/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz')
get_ipython().system('tar xf spark-3.0.1-bin-hadoop2.7.tgz')
get_ipython().system('pip install -q findspark')
get_ipython().system('pip install pyspark')
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"


# In[ ]:


get_ipython().system('sudo wget https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/2.4.0/mongo-spark-connector_2.12-2.4.0.jar')
get_ipython().system('sudo wget https://repo1.maven.org/maven2/org/mongodb/mongodb-driver/3.10.1/mongodb-driver-3.10.1.jar')
get_ipython().system('sudo wget https://repo1.maven.org/maven2/org/mongodb/mongo-hadoop-core/1.3.0/mongo-hadoop-core-1.3.0.jar')


# In[ ]:


get_ipython().system('pip install dnspython')


# **After running !pip install dnspython, please re-run all of the previous code, "restart the execution"**

# In[ ]:


from pymongo import MongoClient
import pymongo
from datetime import datetime, timedelta
from bson import ObjectId
from tabulate import tabulate
import pandas as pd
import numpy as np


# In[ ]:


IMDB_URL = "mongodb+srv://msbd5003:msbd5003@cluster0.7slsj.mongodb.net/IMDB"
client = MongoClient(IMDB_URL)
db = client.get_database("IMDB")
db.list_collection_names()


# In[ ]:


imdb_movies = db.get_collection(db.list_collection_names()[0])
imdb_title_principals = db.get_collection(db.list_collection_names()[1])
imdb_ratings = db.get_collection(db.list_collection_names()[2])
imdb_names = db.get_collection(db.list_collection_names()[3])


# In[ ]:


TMDB_URL = "mongodb+srv://msbd5003:msbd5003@cluster0.esymd.mongodb.net/TMDB"
client = MongoClient(TMDB_URL)
db = client.get_database("TMDB")
db.list_collection_names()


# In[ ]:


tmdb_5000_movies = db.get_collection(db.list_collection_names()[0])
tmdb_5000_credits = db.get_collection(db.list_collection_names()[1])


# In[ ]:


NETFLIX_URL = "mongodb+srv://msbd5003:msbd5003@cluster0.esymd.mongodb.net/NETFLIX"
client = MongoClient(NETFLIX_URL)
db = client.get_database("NETFLIX")
db.list_collection_names()


# In[ ]:


netflix_titles = db.get_collection(db.list_collection_names()[0])


# In[ ]:


MOVIELENS_URL = "mongodb+srv://msbd5003:msbd5003@cluster0.esymd.mongodb.net/MOVIELENS"
client = MongoClient(MOVIELENS_URL)
db = client.get_database("MOVIELENS")
db.list_collection_names()


# In[ ]:


movielens_links = db.get_collection(db.list_collection_names()[0])
movielens_keywords = db.get_collection(db.list_collection_names()[1])
movielens_ratings_small = db.get_collection(db.list_collection_names()[2])
movielens_links_small = db.get_collection(db.list_collection_names()[3])
movielens_credits = db.get_collection(db.list_collection_names()[4])
movielens_metadata = db.get_collection(db.list_collection_names()[6])


# Mongo DB is stored as JSON format as show below.

# In[ ]:


imdb_movies.find_one()


# In[ ]:


imdb_movies.find_one({"imdb_title_id":"tt0000009"})


# In[ ]:


for doc in imdb_movies.find({"country": "USA"}).sort("avg_vote", pymongo.DESCENDING).limit(5):
  print(doc)


# Convert to Dataframe

# In[ ]:


IMDb_movies_df = pd.DataFrame(list(imdb_movies.find()))
IMDb_movies_df


# In[ ]:


IMDb_movies_df = IMDb_movies_df.replace(r'^\s*$', np.nan, regex=True)


# In[ ]:


IMDb_names_df = pd.DataFrame(list(imdb_names.find()))
IMDb_ratings_df = pd.DataFrame(list(imdb_ratings.find()))
IMDb_title_principals_df = pd.DataFrame(list(imdb_title_principals.find()))

netflix_titles_df = pd.DataFrame(list(netflix_titles.find()))

tmdb_5000_movies_df = pd.DataFrame(list(tmdb_5000_movies.find()))
tmdb_5000_credits_df = pd.DataFrame(list(tmdb_5000_credits.find()))

credits_df = pd.DataFrame(list(movielens_credits.find()))
keywords_df = pd.DataFrame(list(movielens_keywords.find()))
links_small_df = pd.DataFrame(list(movielens_links_small.find()))
links_df = pd.DataFrame(list(movielens_links.find()))
movies_metadata_df = pd.DataFrame(list(movielens_metadata.find()))
ratings_small_df = pd.DataFrame(list(movielens_ratings_small.find()))


# # After data processing, convert Dataframe to Datadict and import to MongoDB

# In[ ]:


testdf = IMDb_movies_df.head(5)
testdf


# In[ ]:


TEST_URL = "mongodb+srv://msbd5003:msbd5003@cluster0.muzcc.mongodb.net/"
client = MongoClient(TEST_URL)
db = client["IMPORT"]
collection = db["IMPORT_TEST"]


# In[ ]:


data_dict = testdf.to_dict("records")
data_dict
collection.insert_many(data_dict)
# collection.insert_one(data_dict)


# In[ ]:


test_data = db.get_collection(db.list_collection_names()[0])


# In[ ]:


test_data.find_one()

