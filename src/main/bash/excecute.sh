#!/bin/bash
gcloud services enable dataproc.googleapis.com pubsub.googleapis.com cloudfunctions.googleapis.com datastore.googleapis.com

gcloud pubsub topics create topic

gcloud pubsub subscriptions create topic-subscription --topic=topic

export SERVICE_ACCOUNT_NAME="datdq5"
export PROJECT=$(gcloud info --format='value(config.project)')
export JAR="test-1.0-SNAPSHOT.jar"
export SPARK_PROPERTIES="spark.dynamicAllocation.enabled=false,spark.streaming.receiver.writeAheadLog.enabled=true"
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME

gcloud projects add-iam-policy-binding $PROJECT --role roles/dataproc.worker --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding $PROJECT --role roles/datastore.user --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT.iam.gserviceaccount.com"

gcloud beta pubsub subscriptions add-iam-policy-binding topic-subscription --role roles/pubsub.subscriber --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT.iam.gserviceaccount.com"

gcloud dataproc clusters create demo-cluster --region us-central1 --service-account "$SERVICE_ACCOUNT_NAME@$PROJECT.iam.gserviceaccount.com" --subnet default --zone us-central1-f --scopes=datastore --master-machine-type n1-standard-2 --master-boot-disk-size 500 --num-workers 2 --worker-machine-type n1-standard-2 --worker-boot-disk-size 500 --image-version 1.3-deb9 --project real-time-clustering

gcloud dataproc jobs submit spark --region us-central1 --cluster demo-cluster --async --jar target/$JAR --properties $SPARK_PROPERTIES -- gs://datazzz/200k 0.0000001 200 false 3 8 0 44 random 0.05 50 12
gcloud dataproc jobs submit spark --region us-central1 --cluster demo-cluster --async --jar target/$JAR --properties $SPARK_PROPERTIES -- gs://datazzz/200k 0.0000001 200 true 3 8 0 44 random 0.05 50 12
gcloud dataproc jobs submit spark --region us-central1 --cluster demo-cluster --async --jar target/$JAR --properties $SPARK_PROPERTIES -- gs://datazzz/200k 0.0000001 200 true 3 8 0 44 furthest 0.05 50 12
gcloud dataproc jobs submit spark --region us-central1 --cluster demo-cluster --async --jar target/$JAR --properties $SPARK_PROPERTIES -- gs://datazzz/200k 0.0000001 200 true 3 8 0 44 furthest_in_cluster 0.05 50 12
gcloud dataproc jobs submit spark --region us-central1 --cluster demo-cluster --async --jar target/$JAR --properties $SPARK_PROPERTIES -- gs://datazzz/200k 0.0000001 200 true 3 8 0 44 old_mean 0.05 50 12