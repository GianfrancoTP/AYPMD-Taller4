from django.shortcuts import render
from django.http import HttpResponse
import boto3
import time
import plotly.graph_objects as go
import plotly

f = open('credentials.txt','r')
aws_access_key_id= f.readline()[:-1]
aws_secret_access_key = f.readline()
aws_region = 'us-east-1'
athena = boto3.client('athena',
                      region_name=aws_region,
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)
# Create your views here.


#extrae de athena
def home2Unused(response):
    athena_job_query = athena.start_query_execution(QueryString='SELECT "price" FROM "dataframe_parquet" limit 5',
                                                    QueryExecutionContext={'Database':'grupo6taller3'},
                                                    ResultConfiguration={'OutputLocation':'s3://aypmd-grupo6/Entrega3/athena/'})
    query_execution_id = athena_job_query['QueryExecutionId']
    athena_job_status_query = athena.get_query_execution(QueryExecutionId = query_execution_id)
    args = {'QueryExecutionId':query_execution_id,'MaxResults':1000}
    while(athena_job_status_query['QueryExecution']['Status']['State']!="SUCCEEDED"):
        time.sleep(1)
        athena_job_status_query = athena.get_query_execution(QueryExecutionId = query_execution_id)
        athena_response = athena.get_query_results(**args)
        
    return render(response, "main/home.html", {"athenaTest":athena_response})
    #return render(response, "main/home.html", {})

#muestra grafico
def home(response):
    #grafico
    fig = go.Figure(data=go.Bar(y=[2, 3, 1]))
    
    #ahora no necesita guardar el html en los archivos y lo pasa directamente
    #graph = fig.write_html('main/templates/main/first_figure.html',auto_open=False)
    graph = plotly.io.to_html(fig)
    context = {'graph': graph}
    return render(response,"main/home.html" , context)
    