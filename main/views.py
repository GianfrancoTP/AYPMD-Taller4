from django.shortcuts import render
from django.http import HttpResponse
import boto3
import time
import plotly.graph_objects as go
import plotly
import pandas as pd
import plotly.express as px
import random

f = open('credentials.txt','r')
aws_access_key_id= f.readline()[:-1]
aws_secret_access_key = f.readline()
aws_region = 'us-east-1'
athena = boto3.client('athena',
                      region_name=aws_region,
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)

def results_to_df(results):

    columns = [
        col['Label']
        for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']
    ]

    listed_results = []
    for res in results['ResultSet']['Rows'][1:]:
        values = []
        for field in res['Data']:
            try:
                values.append(list(field.values())[0]) 
            except:
                values.append(list(' '))

        listed_results.append(
            dict(zip(columns, values))
        )

    return listed_results
def randomCountry():
    countries = ["Melbourne","Berlin","Santiago","Tokyo","Chicago" ]
    value = random.randint(0,5)
    return countries[value-1]

# Create your views here.
def home(response):
    athena_job_query = athena.start_query_execution(QueryString='SELECT "bedrooms", AVG(NULLIF("price", 0) ) as "price" FROM "dataframe_parquet" GROUP BY "bedrooms" ORDER BY "bedrooms"',
                                                    QueryExecutionContext={'Database':'grupo6taller3'},
                                                    ResultConfiguration={'OutputLocation':'s3://aypmd-grupo6/Entrega3/athena/'})
    query_execution_id = athena_job_query['QueryExecutionId']
    athena_job_status_query = athena.get_query_execution(QueryExecutionId = query_execution_id)
    args = {'QueryExecutionId':query_execution_id,'MaxResults':1000}
    while(athena_job_status_query['QueryExecution']['Status']['State']!="SUCCEEDED"):
        time.sleep(1)
        athena_job_status_query = athena.get_query_execution(QueryExecutionId = query_execution_id)
        if(athena_job_status_query['QueryExecution']['Status']['State']=="FAILED"):
            return
    athena_response = athena.get_query_results(**args)
    df = pd.DataFrame(results_to_df(athena_response))
    df["price"] = pd.to_numeric(df["price"])
    df["bedrooms"] = pd.to_numeric(df["bedrooms"])
    df["bedrooms"] = df.multiply(50)
    df["country"] = df.apply(lambda x: randomCountry(), axis=1);
    fig = px.line(df, x="bedrooms", y="price", color='country')
    graph = plotly.io.to_html(fig)
    context = {'graph': graph}
    return render(response,"main/home.html",context)
    