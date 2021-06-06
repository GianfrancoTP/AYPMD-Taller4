from django.shortcuts import render
from django.http import HttpResponse
import boto3
import time

#crear un archivo llamado credentials.txt en la carpeta principal con las credenciales
f = open('credentials.txt','r')
aws_access_key_id= f.readline()[:-1]
aws_secret_access_key = f.readline()
aws_region = 'us-east-1'
#print(aws_access_key_id)
#print(aws_secret_access_key)

athena = boto3.client('athena',
                      region_name=aws_region,
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)



#print (athena_response) 

# Create your views here.

def home(response):
    athena_job_query = athena.start_query_execution(QueryString='SELECT "price" FROM "dataframe_parquet" limit 5',
                                                    QueryExecutionContext={'Database':'grupo6taller3'},
                                                    ResultConfiguration={'OutputLocation':'s3://aypmd-grupo6/Entrega3/athena/'})
    query_execution_id = athena_job_query['QueryExecutionId']
    athena_job_status_query = athena.get_query_execution(QueryExecutionId = query_execution_id)
    #print(athena_job_status_query['QueryExecution']['Status']['State'])
    args = {'QueryExecutionId':query_execution_id,'MaxResults':1000}
    while(athena_job_status_query['QueryExecution']['Status']['State']!="SUCCEEDED"):
        time.sleep(1)
        athena_job_status_query = athena.get_query_execution(QueryExecutionId = query_execution_id)
        athena_response = athena.get_query_results(**args)
        
    return render(response, "main/home.html", {"athenaTest":athena_response})
    #return render(response, "main/home.html", {})