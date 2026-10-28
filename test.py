from google.cloud import bigquery

client = bigquery.Client(project="pricesz")

datasets = list(client.list_datasets())

for d in datasets:
    print(d.dataset_id)