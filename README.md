# Home Energy

Hobby project to procure all home energy data from electricity meter, solar panels and home battery for presentation and analysis.

Unless one has the exact same home energy setup, this project will not be relevant.

## Getting Started

### Prerequisites

#### Google Cloud Platform
This project uses the following Google Cloud Platform features:

* [Cloud Functions](https://cloud.google.com/functions/docs) - [Python Runtime](https://cloud.google.com/functions/docs/concepts/python-runtime)
* [Cloud Scheduler](https://cloud.google.com/scheduler/docs)
* [Cloud Storage](https://cloud.google.com/storage/docs)
* [Cloud Firestore](https://cloud.google.com/firestore/docs) - [Native Mode](https://cloud.google.com/firestore/docs/firestore-or-datastore)

Getting Started on Google Cloud Platform is not easy as the ecosystem is huge with large number of [services](https://cloud.google.com/docs/overview/cloud-platform-services) offered.  See [Get started with Google Cloud](https://cloud.google.com/docs).

All Google Cloud Platform features in this project share the same [service account](https://cloud.google.com/iam/docs/creating-managing-service-accounts#creating_a_service_account) and this is not recommended, see [Enforce least privilege with recommendations](https://cloud.google.com/iam/docs/recommender-overview)

#### Python Development Environment

Refer to [Python Getting Started](https://www.python.org/about/gettingstarted/) and [Python Virtual Environments](https://www.python.org/dev/peps/pep-0405/).

Below commands will setup and install all dependencies of this project into its own virtual environment.

```bash
cd $PROJECT_DIR

python3 -m venv .venv

source .venv/bin/activate

pip install -r requirements.txt
```

#### Accessing Data APIs

Enlighten Developer account created and and API key setup.  Refer to their [Getting Started](https://developer.enphase.com/docs/quickstart.html) page.

[LEMS Battery Management](https://lems.panabattery.com/) account and API access.

## Running the tests

```bash
pytest
```

TODO - More tests required

## Build

TODO - Use Google Cloud Build?

## Functions

### on_storage_blob

This function runs on [storage triggers](https://cloud.google.com/functions/docs/calling/storage).  More specifically, blobs added to storage with following prefix namespaces are handled:

* nem12/in (env var = `$NEM12_STORAGE_PATH_IN`) - All NEM12 files are manually placed here.  All NEM12 files are merged and placed into nem12/merged folder.
* nem12/merged (env var = `$NEM12_STORAGE_PATH_MERGED`) - All merged NEM12 files grouped by one NMI per file are placed here.
* enlighten (env var = `$ENLIGHTEN_STORAGE_PATH_PREFIX`) - All solar panels data from Enlighten API are placed here one JSON file per day.
* lems (env var = `$LEMS_STORAGE_PATH_PREFIX`) - All LEMS battery data are placed here, one CSV file per day.

### fetch_enlighten_data - on_http_get_enlighten_data(request)

Fetch solar panels data, scheduled to run daily and can also be manually triggered using HTTP.

```bash
curl -X POST --data "" "https://us-central1-$(gcloud config get-value project).cloudfunctions.net/fetch_enlighten_data" -H "Authorization: bearer $(gcloud auth print-identity-token)"
```

### fetch_lems_data - on_http_get_lems_data(request)

Fetch battery data, scheduled to run daily and can also be manually triggered using HTTP.

```bash
curl -X POST --data "" "https://us-central1-$(gcloud config get-value project).cloudfunctions.net/fetch_lems_data" -H "Authorization: bearer $(gcloud auth print-identity-token)"
```

## Deployment

To deploy functions:

```bash
gcloud functions deploy fetch_enlighten_data --entry-point on_http_get_enlighten_data --runtime python37 --trigger-http --env-vars-file .secrets/.env.yaml

gcloud functions deploy fetch_lems_data --entry-point on_http_get_lems_data --runtime python37 --trigger-http --env-vars-file .secrets/.env.yaml

gcloud functions deploy on_storage_blob --entry-point on_storage_blob --runtime python37 --trigger-bucket $GCP_STORAGE_BUCKET_ID --env-vars-file .secrets/.env.yaml
```

To delete functions:

```bash
gcloud functions delete fetch_enlighten_data

gcloud functions delete fetch_lems_data

gcloud functions delete on_storage_blob
```

To schedule jobs and run functions:

```bash
gcloud scheduler jobs create http fetch_enlighten_data_job --schedule="0 2 * * *" --uri="https://us-central1-$(gcloud config get-value project).cloudfunctions.net/fetch_enlighten_data" --message-body="" --oidc-service-account-email=OIDC_SERVICE_ACCOUNT_EMAIL

gcloud scheduler jobs create http fetch_lems_data_job --schedule="0 2 * * *" --uri="https://us-central1-$(gcloud config get-value project).cloudfunctions.net/fetch_lems_data" --message-body="" --oidc-service-account-email=OIDC_SERVICE_ACCOUNT_EMAIL
```

To update scheduled jobs:

```bash
gcloud scheduler jobs update http fetch_enlighten_data_job --schedule="*/5 * * * *" --uri="https://us-central1-$(gcloud config get-value project).cloudfunctions.net/fetch_enlighten_data" --message-body="" --oidc-service-account-email=OIDC_SERVICE_ACCOUNT_EMAIL

gcloud scheduler jobs update http fetch_lems_data_job --schedule="*/5 * * * *" --uri="https://us-central1-$(gcloud config get-value project).cloudfunctions.net/fetch_lems_data" --message-body="" --oidc-service-account-email=OIDC_SERVICE_ACCOUNT_EMAIL
```

To delete scheduled jobs:

```bash
gcloud scheduler jobs delete fetch_enlighten_data_job

gcloud scheduler jobs delete fetch_lems_data_job
```

## Functions Framework

Handy for local testing rather than deploying functions to GCP, more info [here](https://cloud.google.com/functions/docs/functions-framework) and
[functions-framework-python](https://github.com/GoogleCloudPlatform/functions-framework-python) GitHub page.

Local testing of http triggered functions.

```bash
functions-framework --source main.py --target on_http_get_enlighten_data --signature-type http --debug
functions-framework --source main.py --target on_http_get_lems_data --signature-type http --debug
```

Local testing of event triggered functions.

```bash
functions-framework --source main.py --target on_storage_blob --signature-type event --debug
```

## Common GCP commands

View latest gcloud functions log

```bash
gcloud functions logs read
```

View bucket files

```bash
gsutil ls "gs://data-$(gcloud config get-value project)/nem12/in"

gsutil ls "gs://data-$(gcloud config get-value project)/nem12/merged"

gsutil ls "gs://data-$(gcloud config get-value project)/enlighten"

gsutil ls "gs://data-$(gcloud config get-value project)/lems"
```

## Related Docs

### Meter Data - United Energy Network

Australian Energy Market interval data can be manually downloaded from the network/distribution area you live in.
For this project, the meter data was obtained from United Energy's [Energy Easy Portal](https://energyeasy.ue.com.au/)
and is stored in [NEM12 format](https://www.aemo.com.au/consultations/current-and-closed-consultations/meter-data-file-format-specification-nem12-and-nem13/).

### Solar Panels Data - Enlighten Envoy API

My solar system uses Enphase solar panels with microinverters.  They provide an [API](https://developer.enphase.com/docs) to fetch energy generation data
from each panel.

Example request:

```bash
curl https://api.enphaseenergy.com/api/v2/systems/$ENLIGHTEN_SYSTEM_ID/stats?key=$ENLIGHTEN_API_KEY&user_id=$ENLIGHTEN_USER_ID&datetime_format=iso8601&
start_at=1428328800
```

### LEMS Battery Management

[Residential Storage Battery System](https://www.panasonic.com/au/support/product-archives/energy-solutions/residential-storage-battery-system/lj-sk84a.html)

Example request:
```bash
curl -X GET -u $LEMS_USER:$LEMS_PASSWORD https://lems.panabattery.com/api/Battery/$LEMS_BATTERY_ID/soc/data?MinDate=2020-01-01&Hours=24
```
