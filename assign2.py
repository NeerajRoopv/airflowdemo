from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import tweepy
import pandas as pd
import sqlite3
import sqlalchemy
from sqlalchemy import create_engine


def create_dataFrame(**kwargs):
	ti=kwargs['ti']
	auth = tweepy.OAuthHandler('API_key','API_secret_key')
	auth.set_access_token('Access_token_key','Access_token_secret_key')
	api=tweepy.API(auth)

	tweets=[]
	likes=[]
	time=[]

	for i in tweepy.Cursor(api.user_timeline, id='aadl' , tweet_mode="extended").items(100):
		tweets.append(i.full_text)
		likes.append(i.favorite_count)
		time.append(str(i.created_at))

	ti.xcom_push(key='tweets',value=tweets)
	ti.xcom_push(key='likes',value=likes)
	ti.xcom_push(key='time',value=time)

def store_in_database(**kwargs):
	ti=kwargs['ti']
	engine=sqlalchemy.create_engine('sqlite:///Tweets.db', echo=False)
	
	tweets=ti.xcom_pull(key='tweets')
	likes=ti.xcom_pull(key='likes')
	time=ti.xcom_pull(key='time')

	df=pd.DataFrame({'tweets':tweets,'likes':likes,'time':time})
	df.to_sql('aadl_tweets',engine, if_exists='replace', index=False)


with DAG("twitter_DAG", start_date=datetime(2021,9,21),schedule_interval="@daily", catchup=False) as dag:
	task1 = PythonOperator(
		task_id="task1",
		python_callable=create_dataFrame
		)

	task2 = PythonOperator(
		task_id="task2",
		python_callable=store_in_database
		)

	task1>>task2
