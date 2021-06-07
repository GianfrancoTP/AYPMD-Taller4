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
    athena_response = athenaRequest('SELECT "bedrooms", AVG(NULLIF("price", 0) ) as "price" FROM "dataframe_parquet" GROUP BY "bedrooms" ORDER BY "bedrooms"')
    df = pd.DataFrame(results_to_df(athena_response))
    df["price"] = pd.to_numeric(df["price"])
    df["bedrooms"] = pd.to_numeric(df["bedrooms"])
    df["bedrooms"] = df["bedrooms"]*50
    df["country"] = df.apply(lambda x: randomCountry(), axis=1);
    fig = px.line(df, x="bedrooms", y="price", color='country')
    graph1 = plotly.io.to_html(fig)
    
    #second graph (pie chart amount of homestays by country) despues hay que cambiar el query POR AHORA ES DUMMY
    athena_response = athenaRequest('SELECT COUNT("price") as price FROM "dataframe_parquet" GROUP BY "price"')
    df = pd.DataFrame(results_to_df(athena_response))
    df["price"] = pd.to_numeric(df["price"])
    df["country"] = df.apply(lambda x: randomCountry(), axis=1);
    df.loc[df['price'] < 1000, 'country'] = 'Other countries' # Represent only large countries
    fig = px.pie(df, values='price', names='country', title='Homestays by city')
    graph2 = plotly.io.to_html(fig)
    
    #third graph (histogram) dummy query
    df["month"] = df.apply(lambda x: randomMonth(), axis=1);
    fig = px.histogram(df, x="month", color="country", # can be `box`, `violin`
                             )#hover_data=df.columns)
    graph3 = plotly.io.to_html(fig)
    
    #fourth graph
    athena_response = athenaRequest('SELECT "bedrooms","bathrooms", AVG(NULLIF("price", 0) ) as "price" FROM "dataframe_parquet" GROUP BY "bedrooms","bathrooms"')
    df2 = pd.DataFrame(results_to_df(athena_response))
    df2["price"] = pd.to_numeric(df2["price"])
    df2["bedrooms"] = pd.to_numeric(df2["bedrooms"])
    df2["bedrooms"] = df2["bedrooms"]*50
    df2["bathrooms"] = pd.to_numeric(df2["bathrooms"])
    df2["bathrooms"] = df2["bathrooms"]*333
    df2 = df2[df2.bathrooms<30]
    df2 = df2[df2.bedrooms<30]
    df2["country"] = df2.apply(lambda x: randomCountry(), axis=1);
    fig = px.scatter_3d(df2, x='bathrooms', y='bedrooms', z='price',
                  color='country')
    graph4 = plotly.io.to_html(fig)
    
    #fifth graph
    #df = df.groupby(pd.cut(df["price"], np.arange(0, 1, 0.1))).sum()
    #fig = px.area(df, x="month", y="price", color="country")
    #graph5 = plotly.io.to_html(fig)
    df['month'] = pd.Categorical(df['month'], categories=months, ordered=True)
    fig = go.Figure()
    subdf = df.loc[df['country'] == df["country"].unique()[0]]
    subdf = subdf.sort_values(by="month")
    fig.add_trace(go.Scatter(x=months, y=subdf["price"], fill='tonexty',name=df["country"].unique()[0])) # fill down to xaxis
    for country in df["country"].unique()[1:]:
        subdf = df.loc[df['country'] == country]
        subdf = subdf.sort_values(by="month")
        fig.add_trace(go.Scatter(x=months, y=subdf["price"], fill='tonexty',name=country)) # fill to trace0 y
    graph5 = plotly.io.to_html(fig)
    
    
    context = {'graph1': graph1, 'graph2':graph2, 'graph3':graph3, 'graph4':graph4, 'graph5':graph5}
    return render(response,"main/home.html",context)
    