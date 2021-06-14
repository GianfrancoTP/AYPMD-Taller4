from django.shortcuts import render
from django.http import HttpResponse
import boto3
import time
import plotly.graph_objects as go
import plotly
import pandas as pd
import plotly.express as px
import random
import numpy as np

months = ["jan", "feb","mar", "apr","may","jun","jul","aug","sep","oct","nov","dec"]

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
    value = random.randint(0,4)
    return countries[value]
def randomMonth():
    value = random.randint(0, 11)
    return months[value]
def athenaRequest(query):
    athena_job_query = athena.start_query_execution(QueryString=query,
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
    return athena.get_query_results(**args)
# Create your views here.
def home(response):
    
    #first graph (precio segun dormitorio y pais)
    athena_response = athenaRequest('SELECT "bedrooms", AVG(NULLIF("avg_price", 0) ) as price,"city" FROM "dataframe_parquet" WHERE "bedrooms"<=10 GROUP BY "bedrooms","city" ORDER BY "bedrooms"')
    df = pd.DataFrame(results_to_df(athena_response))
    df["price"] = pd.to_numeric(df["price"])
    df["bedrooms"] = pd.to_numeric(df["bedrooms"])
    #df["city"] = df.apply(lambda x: randomCountry(), axis=1);
    fig = px.line(df, x="bedrooms", y="price", color="city")
    graph1 = plotly.io.to_html(fig)
    
    #second graph (pie chart amount of homestays by country) despues hay que cambiar el query POR AHORA ES DUMMY
    athena_response = athenaRequest('SELECT COUNT("city") as total,"city","month" FROM "dataframe_parquet" GROUP BY "city","month"')
    df2 = pd.DataFrame(results_to_df(athena_response))
    fig = px.pie(df2, values='total', names='city', title='Homestays by city')
    graph2 = plotly.io.to_html(fig)
    

    #third graph (histogram) dummy query
    athena_response = athenaRequest('SELECT COUNT("city") as total,"city","month" FROM "dataframe_parquet" GROUP BY "city","month"')
    df3 = pd.DataFrame(results_to_df(athena_response))
    fig = px.histogram(df3, y="total", x="month", color="city", # can be `box`, `violin`
                             )#hover_data=df3.columns)
    graph3 = plotly.io.to_html(fig)
    

    #fourth graph
    athena_response = athenaRequest('SELECT "city","bedrooms","bathrooms", AVG(NULLIF("avg_price", 0) ) as price FROM "dataframe_parquet" GROUP BY "bedrooms","bathrooms","city"')
    df4 = pd.DataFrame(results_to_df(athena_response))
    df4["price"] = pd.to_numeric(df4["price"])
    df4["bedrooms"] = pd.to_numeric(df4["bedrooms"])
    df4["bedrooms"] = df4["bedrooms"]
    df4["bathrooms"] = pd.to_numeric(df4["bathrooms"])
    df4["bathrooms"] = df4["bathrooms"]
    df4 = df4[df4.bathrooms<30]
    df4 = df4[df4.bedrooms<30]
    fig = px.scatter_3d(df4, x='bathrooms', y='bedrooms', z='price',
                  color='city')
    graph4 = plotly.io.to_html(fig)
    
    
    #fifth graph
    athena_response = athenaRequest('SELECT AVG(NULLIF("avg_price", 0) ) as price,"city","month" FROM "dataframe_parquet" GROUP BY "city","month"')
    df3 = pd.DataFrame(results_to_df(athena_response))
    df3['month'] = pd.Categorical(df3['month'], categories=months, ordered=True)
    df3["price"] = pd.to_numeric(df3["price"])
    fig = go.Figure()
    subdf = df3.loc[df3['city'] == df3["city"].unique()[0]]
    subdf = subdf.sort_values(by="month")
    fig.add_trace(go.Scatter(x=months, y=subdf["price"], fill='tonexty',name=df3["city"].unique()[0])) # fill down to xaxis
    for city in df3["city"].unique()[1:]:
        subdf = df3.loc[df3['city'] == city]
        subdf = subdf.sort_values(by="month")
        fig.add_trace(go.Scatter(x=months, y=subdf["price"], fill='tonexty',name=city)) # fill to trace0 y
    graph5 = plotly.io.to_html(fig)
    
    
    context = {'graph1': graph1, 'graph2':graph2, 'graph3':graph3, 'graph4':graph4, 'graph5':graph5}
    return render(response,"main/home.html",context)
    