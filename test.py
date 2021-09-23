import os
import subprocess
from datetime import datetime
from git import Repo

#moving to git repository
path=os.chdir('/home/neeraj/airflow/gitrep/airflowdemo')

#perform git pull
git_pull = subprocess.Popen(["git","pull"], stdout=subprocess.PIPE)
op= git_pull.communicate()[0]


#list direcotry
list_files = subprocess.run(["ls"])
print("%d" % list_files.returncode)

#create file
time=datetime.now()
current_time=time.strftime("%H:%M:%S")
filename="File:"+str(current_time)+".txt"
f=open(filename,"w")
f.write("test fill addition")
f.close()

#list files and directories
list_files = subprocess.run(["ls"])
print("%d" % list_files.returncode)

#git push

repo=Repo(path)
repo.index.add([filename])
repo.index.commit(filename + ' commit')
repo.git.push()





