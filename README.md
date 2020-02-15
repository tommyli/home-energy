# GCP Python Functions to Fetch Data on Home Energy Usage

## Solar Panels Data - Enlighten Envoy API

Main API docs page:
[https://developer.enphase.com/docs](https://developer.enphase.com/docs)

The main API call to fetch solar panels generation data at 5 minute intervals for each individual microinverter.

## fetch_enlighten_data_get Google Cloud Function

[Google Cloud Functions](https://cloud.google.com/functions/docs)

`fetch_enlighten_data_get` is the main entry point to the API call that triggers the fetching of data from Enlighten API
and stores result JSON into Google storage bucket.  This can be manually called using curl:

```
curl -X POST --data "" "https://us-central1-$(gcloud config get-value project).cloudfunctions.net/fetch_enlighten_data_get" -H "Authorization: bearer $(gcloud auth print-identity-token)"
```

To deploy functions:

[gcloud functions](https://cloud.google.com/sdk/gcloud/reference/functions)

```
gcloud functions deploy fetch_enlighten_data_get --entry-point fetch_enlighten_data_get --runtime python37 --trigger-http --env-vars-file .secrets/.env.yaml
```

To delete functions:
```
gcloud functions delete fetch_enlighten_data_get
```

This function is scheduled using [gcloud scheduler jobs](https://cloud.google.com/sdk/gcloud/reference/scheduler/jobs) and was created using this command:
```
gcloud scheduler jobs create http fetch_enlighten_data_job --schedule="*/5 * * * *" --uri="https://us-central1-$(gcloud config get-value project).cloudfunctions.net/fetch_enlighten_data_get" --message-body="" --oidc-service-account-email=OIDC_SERVICE_ACCOUNT_EMAIL
```
and job details can be updated using this command:
```
gcloud scheduler jobs update http fetch_enlighten_data_job --schedule="*/5 * * * *" --uri="https://us-central1-$(gcloud config get-value project).cloudfunctions.net/fetch_enlighten_data_get" --message-body="" --oidc-service-account-email=OIDC_SERVICE_ACCOUNT_EMAIL
```
and delete:
```
gcloud scheduler jobs delete fetch_enlighten_data_job
```

### Common commands

View latest gcloud functions log
```
gcloud functions logs read
```

View bucket files
```
gsutil ls "gs://data-$(gcloud config get-value project)/enlighten"
```
