from zipfile import ZipFile
from xml.etree import ElementTree
import pickle

import keytree
from shapely.geometry import Point, shape, Polygon

import os

from elasticsearch import Elasticsearch
from elasticsearch.connection import create_ssl_context
import urllib3
import ssl
import logging
import json
import time
import sys
import pandas as pd
import numpy
from pyspark.sql import SparkSession

from pyspark.sql.functions import monotonically_increasing_id
import pyspark.sql.functions as f

import requests
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Pysparkexample").config("spark.some.config.option", "some-value").getOrCreate()




def parsin(x,codigo_df):

    mac = x[0]
    oui = x[1]
    latitude = x[2]
    longitude = x[3]
    lastdate = x[4]
    mfg = x[5]
    cfn = x[6]
    ssid = x[7]

    # Get in piclke file:
    POLYGON_URL = 'http://0.0.0.0:5050/get-polygon'
    # Phase 1. Get polygon data from wifi location
    loc_params = {
        'y': latitude,
        'x': longitude
    }
    response = requests.get(url=POLYGON_URL, params=loc_params)
    poly_data = response.json()

    if not poly_data:
        pass
    try:
            geocodigo_municipio = ''
            if 'text' in poly_data['extended_data']['schemadata']['simpledata'][0]:
                geocodigo_municipio = poly_data['extended_data']['schemadata']['simpledata'][0]['text']
            geocodigo_setor = ''
            if 'text' in poly_data['extended_data']['schemadata']['simpledata'][1]:
                geocodigo_setor = poly_data['extended_data']['schemadata']['simpledata'][1]['text']
            cidade = ''
            if 'text' in poly_data['extended_data']['schemadata']['simpledata'][2]:
                cidade = poly_data['extended_data']['schemadata']['simpledata'][2]['text']
            distrito = ''
            if 'text' in poly_data['extended_data']['schemadata']['simpledata'][3]:
                distrito = poly_data['extended_data']['schemadata']['simpledata'][3]['text']
            sub_distrito = ''
            if 'text' in poly_data['extended_data']['schemadata']['simpledata'][4]:
                sub_distrito = poly_data['extended_data']['schemadata']['simpledata'][4]['text']
            bairro = ''
            if 'text' in poly_data['extended_data']['schemadata']['simpledata'][5]:
                bairro = poly_data['extended_data']['schemadata']['simpledata'][5]['text']
    except:
        pass

    try:
        codigo_distrito = poly_data['extended_data']['schemadata']['simpledata'][1]['text'][:9]

        codigo_distrito = int(codigo_distrito)


        uf = codigo_df.iloc[0]['Nome_UF']
        mesoregiao_geografica = codigo_df.iloc[0]['Mesorregião Geográfica']
        nome_mesoregiao = codigo_df.iloc[0]['Nome_Mesorregião']
        microregiao_geografica = codigo_df.iloc[0]['Microrregião Geográfica']
        nome_microregiao = codigo_df.iloc[0]['Nome_Microrregião']
        codigo_municipio = codigo_df.iloc[0]['Município']
        codigo_municipio_completo = codigo_df.iloc[0]['Código Município Completo']
        nome_municipio = codigo_df.iloc[0]['Nome_Município']
        #codigo_distrito = codigo_df.iloc[0]['Distrito']
        codigo_distrito_completo = codigo_df.iloc[0]['Código de Distrito Completo']
        nome_distrito = codigo_df.iloc[0]['Nome_Distrito']

        codigo_distrito_completo = codigo_df.iloc[0]['Código de Distrito Completo']

        nome_distrito = codigo_df.iloc[0]['Nome_Distrito']
    #odigo_distrito = codigo_df.loc[codigo_df[codigo_df.columns[10]] == codigo_distrito , [codigo_df.columns[9]] ].iloc[0][0]

    #nome_distrito = codigo_df.loc[codigo_df[codigo_df.columns[10]] == codigo_distrito , [codigo_df.columns[9]] ].iloc[0][0]



        return([str(mac),str(oui),str(lastdate),str(mfg),str(cfn),str(ssid),str(geocodigo_municipio),str(geocodigo_setor),cidade,distrito,sub_distrito,str(uf),str(mesoregiao_geografica),str(nome_mesoregiao),str(microregiao_geografica),str(nome_microregiao),str(codigo_municipio),str(codigo_municipio_completo),str(nome_municipio),str(codigo_distrito),str(codigo_distrito_completo),str(nome_distrito)])


    except:
        return([])
        pass






if __name__ == '__main__':
    cSchema = StructType([StructField("mac", StringType()),StructField("oui", StringType()),StructField("lastdate", StringType()),StructField("mfg", StringType()),StructField("cfn", StringType()),StructField("ssid", StringType()),StructField("geocodigo_municipio", StringType()),StructField("geocodigo_setor", StringType()),StructField("cidade", StringType()),StructField("distrito", StringType()),StructField("sub_distrito", StringType()),StructField("uf", StringType()),StructField("mesoregiao_geografica", StringType()),StructField("nome_mesoregiao", StringType()),StructField("microregiao_geografica", StringType()),StructField("nome_microregiao", StringType()),StructField("codigo_municipio", StringType()),StructField("codigo_municipio_completo", StringType()),StructField("nome_municpio", StringType()),StructField("codigo_distrito", StringType()),StructField("codigo_distrito_completo", StringType()), StructField("nome_distrito", StringType()) ])



    codigo_df = pd.read_excel(os.path.join(os.path.dirname(__file__),  'RELATORIO_DTB_BRASIL_DISTRITO.xls'),sheet_name='DTB_2018_Distrito')
    # Prepare Logging
    logging.basicConfig(level=logging.ERROR)
    # Testar a conexao com o servidor:
    file_csv = 'brazil_wifi.csv'
    table = spark.read.csv(file_csv,header=True)

    table = table.withColumn('index',f.monotonically_increasing_id())
    part1 = table.filter('index > 1000000 and index < 2000000')
    #table = table.limit(1)
    #print((part1.count() ))
    rdd = parte1.rdd.map(list)
    print(time.time())
    rdd2 = rdd.map(lambda x: parsin(x,codigo_df)).collect()
    #print(rdd2)
    df = spark.createDataFrame(rdd2,schema=cSchema)
    df.write.csv('mycsv.csv')
    print("FINISH")

