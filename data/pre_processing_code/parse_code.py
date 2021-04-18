import re
import pandas as pd
import os
import datetime
os.chdir("/Volumes/UBC/Block6/586-AdvanceMachineLearning/project/machinelearningproject")
 
input_file = r"/Volumes/UBC/Block6/586-AdvanceMachineLearning/project/machinelearningproject/data/raw_data/HDFS/HDFS_2k.log"


output_path = r"/Volumes/UBC/Block6/586-AdvanceMachineLearning/project/machinelearningproject/data/processed_data/HDFS"

date1=[]
time1=[]
pid1=[]
level1=[]
component1=[]
content1=[]
type_of_content1=[]

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
        type_of_content = (line.split(':',1)[1].strip()).split(' ', 1)[0]

        date1.append(date)
        time1.append(time)
        pid1.append(pid)
        level1.append(level)
        component1.append(component)
        content1.append(content)
        type_of_content1.append(type_of_content)

    parsed_data = pd.DataFrame({'Date':date1, 'Time':time1, 'PID':pid1, 'Level':level1, 'Component':component1, 'Content':content1, 'TypeOfContent': type_of_content1})
    parsed_data['datetime_id'] = parsed_data['Date']+parsed_data['Time']
    parsed_data['Tag'] = parsed_data['TypeOfContent']

    parsed_data1 = parsed_data[(parsed_data['Tag'] == "PacketResponder") | 
                               (parsed_data['Tag'] == "BLOCK*") |
                               (parsed_data['Tag'] == "Received") |  
                               (parsed_data['Tag'] == "Receiving") |
                               (parsed_data['Tag'] == "Verification") |
                               (parsed_data['Tag'] == "Deleting")]
    parsed_data2 = parsed_data[(parsed_data['Tag'] != "PacketResponder") & 
                               (parsed_data['Tag'] != "BLOCK*") &
                               (parsed_data['Tag'] != "Received") &  
                               (parsed_data['Tag'] != "Receiving") &
                               (parsed_data['Tag'] != "Verification") &
                               (parsed_data['Tag'] != "Deleting")]
    
    parsed_data2_GOT = parsed_data2[parsed_data2['Tag'].str.contains('Got')]
    parsed_data2_GOT['Tag'] = "Others_IP_WARN"

    parsed_data2_non_GOT = parsed_data2[-parsed_data2['Tag'].str.contains('Got')]
    parsed_data2_non_GOT['Tag'] = "Others_IP_INFO"

    frames = [parsed_data1, parsed_data2_GOT, parsed_data2_non_GOT]

    parsed_data = pd.concat(frames)
    parsed_data['Date'] = pd.to_datetime(parsed_data['Date'], format='%d%m%Y', errors='ignore')
    parsed_data['Date'] = parsed_data['Date'].str[:10]
    
    parsed_data.to_csv('data/processed_data/HDFS/HDFS_2k_structured.csv', index=False)

 
