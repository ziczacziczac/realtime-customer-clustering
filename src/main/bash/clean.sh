#!/bin/bash
gcloud dataproc clusters delete demo-cluster --quiet --region us-central1
gcloud pubsub topics delete topic --quiet
gcloud pubsub subscriptions delete topic-subscription --quiet
gcloud iam service-accounts delete $SERVICE_ACCOUNT_NAME@$PROJECT.iam.gserviceaccount.com --quiet