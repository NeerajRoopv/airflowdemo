from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import os
import subprocess
from git import Repo

def pull_from_git(**kwargs):
	ti =kwargs['ti']
	# to pull all files from git repositories
	path=os.chdir('/home/neeraj/airflow/gitrep/airflowdemo')

	ti.xcom_push(key='path_name', value=path)
	#perform git pull
	git_pull = subprocess.Popen(["git","pull"], stdout=subprocess.PIPE)
	op= git_pull.communicate()[0]



def create_file(**kwargs):
	ti = kwargs['ti']
	path_from_above = os.chdir('/home/neeraj/airflow/gitrep/airflowdemo')
	time=datetime.now()
	current_time=time.strftime("%H:%M:%S")
	filename="File:"+str(current_time)+".txt"
	ti.xcom_push(key='filename',value=filename)
	f=open(filename,"w")
	f.write("test fill addition")
	f.close()

def push_to_git(**kwargs):

	ti = kwargs['ti']
	path_from_above =os.chdir('/home/neeraj/airflow/gitrep/airflowdemo')
	fname=ti.xcom_pull(key='filename')
	repo=Repo(path_from_above)
	repo.index.add([fname])
	repo.index.commit(fname + ' commit')
	repo.git.push()




with DAG("my_dag", start_date=datetime(2021,9,21),schedule_interval="@daily", catchup=False) as dag:
	task1 = PythonOperator(
		task_id="task1",
		python_callable=pull_from_git
		)

	task2 = PythonOperator(
		task_id="task2",
		python_callable=create_file
		)

	task3 = PythonOperator(
		task_id='task3',
		python_callable=push_to_git
		)

	task1>>task2>>task3

