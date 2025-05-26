import json
import pandas as pd
#Librería para el S3
import boto3
import urllib.parse
import matplotlib.pyplot as plt
from io import StringIO, BytesIO

#Creamos el cliente S3
s3 = boto3.client('s3')
#nuestro nombre de bucker
bucket_name = 'buckets-artifact-paco'

def lambda_handler(event, context):
    #Un bucket está compuesto por el nombre del propio bucket y la key que es 
    #el fichero que estamos subiendo
    #Tenemos la opción de leer el propio bucket s3 o podemos leer la URL
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    response = s3.get_object(Bucket=bucket, Key=key)
    print('Tenemos fichero!!!!')
    url = f"https://{bucket_name}.s3.us-east-1.amazonaws.com/{key}";
    df = pd.read_csv(url)
    print("leyendo file " + key)
    df_killer = df.groupby('killer')['death_no'].agg(['count'])
    df_primeros = df_killer.head(5)
    print(df_primeros)
    #capturamos los datos para el gráfico
    data = df_primeros.groupby('killer').sum()
    #generamos un nuevo gráfico limpio
    plt.cla()
    plt.clf()
    #creamos una imagen en memoria para el fichero
    img_data = BytesIO()
    #generamos el gráfico con los datos
    plt.pie(x=data["count"], labels=data.index)
    print("generando gráficos")
    #guardamos la imagen
    plt.savefig(img_data, format='png')
    img_data.seek(0)
    
    #el último paso es subir nuestra imagen a nuestro bucket
    #nombre de imagen
    image_name = key + ".png"
    bucket = boto3.resource('s3').Bucket(bucket_name)
    bucket.put_object(Body=img_data, ContentType='image/png', Key=image_name)
    print("Tenemos una imagen subida!!!!")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
