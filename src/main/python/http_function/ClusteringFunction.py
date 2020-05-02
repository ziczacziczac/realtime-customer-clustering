# Copyright Google Inc. 2018
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import time
from google.cloud import pubsub
import pandas

#DATA_PATH = "gs://demo-code/data_sample.csv"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "file:///C:/Users/ADMIN/Downloads/real-time-clustering-3ddb5eca0a2d.json"
DATA_PATH = sys.argv[0]
STOP_PUBLISH = False
DELAY_INTERVAL = sys.argv[1]

PROJECT_ID = "halogen-emblem-263413"
TOPIC_NAME = "topic"
publisher = pubsub.PublisherClient()
topic_url = 'projects/{project_id}/topics/{topic}'.format(
        project_id = PROJECT_ID,
        topic = TOPIC_NAME
        )
def read_data():
    df = pandas.read_csv(DATA_PATH, header = None)
    idx = [x for x in range(1, len(df[0]) + 1)]
    return idx, df

def mapping(i, zipped_list):
    res = str(i) + "\n"
    for zipped in zipped_list:
        res += "{idx},{value}\n".format(
                idx = zipped[0],
                value = round(zipped[1], 4)
                )
    return res.strip()
        
def start_publish_data():
    global topic_url
    
    idx, df = read_data()
    row = 0
    df_lenth = 48
    while STOP_PUBLISH == False:
        if row > 12:
            time.sleep(DELAY_INTERVAL)
        data = mapping(row, list(zip(idx, df.loc[:, row % df_lenth])))
        #publisher.publish(topic_url, data.encode("utf-8"))
        row = row + 1
        print("current rows: " + str(row))
        print(data)

def stop_publish_data():
    global STOP_PUBLISH
    STOP_PUBLISH = True    

#def get_current_cluster_result():
    
#def get_all_cluster_result():
#stop_publish_data()
start_publish_data()
