import re
import pandas as pd
import os

#Volumes/UBC/Block6/586-AdvanceMachineLearning/project/
os.chdir("/Users/user/Desktop/machinelearningproject")
 
input_file = r"/Users/user/Desktop/machinelearningproject/data/raw_data/HDFS/HDFS_2k.log"


output_path = r"/Users/user/Desktop/machinelearningproject/data/processed_data/HDFS"

date1=[]
time1=[]
pid1=[]
level1=[]
component1=[]
content1=[]

parsed_data=[]

with open("data/raw_data/HDFS/HDFS_2k.log","r") as file:
    for line in file:
        
        before_colon = line.split(':',1)[0].strip()
        
        date = before_colon.split()[0]
        time = before_colon.split()[1]
        pid = before_colon.split()[2]
        level = before_colon.split()[3]
        component = before_colon.split()[4]

        content = line.split(':',1)[1].strip() #after colon

        date1.append(date)
        time1.append(time)
        pid1.append(pid)
        level1.append(level)
        component1.append(component)
        content1.append(content)

    parsed_data = pd.DataFrame({'Date':date1, 'Time':time1, 'PID':pid1, 'Level':level1, 'Component':component1, 'Content':content1})
    #parsed_data.columns.values[0] = "LineID"
    print(parsed_data)

    parsed_data.to_csv('data/processed_data/HDFS/HDFS_2k_structured.csv', index=False)

 
