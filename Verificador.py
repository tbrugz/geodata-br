'''
----------------- AUTOMATIZAR BUSCA DE ERRO DE MULTIPOLIGONOS ----------------------

Autor: Samuel  Amico
Data: 20/01/2020
Projeto: NetMapBR 
Objetivo: Encontrar Falsos Poligonos e detectar o nome das cidades com este problema 

------------------------------------------------------------------------------------

'''

import json
import glob

Lista_json = glob.glob('*.json')

# Para cada Estado descobrir cidades erradas:

for estado in Lista_json:
    numero_estado = estado.split('-')[1]

    # Lendo e carregando arquivo JSON em variavel data
    json_data = open(estado).read()
    data = json.loads(json_data)

    # Variaveis: contador de cidades e dicionario de cidades erradas
    cnt = 0
    cidades_erradas = {}

    # interar para cada cidade existente
    for json_cidades in data["features"]:
        cnt += 1
        # Vetor que contem as coordenadas, pode ser apenas uma LineString ou multiplos LinesStrings
        vector =(json_cidades["geometry"]["coordinates"])
        # Verifica se existe apenas um poligono
        if(len(vector) > 1):
            # Dicionario que recebe o nome da cidade e quantos poligos ele possui:
            cidade_errada = json_cidades["properties"]["name"]
            poligonos = len(vector)
            cidades_erradas[str(cidade_errada)] = poligonos

    print( (" Estado = {} , Cidades-Totais = {} , Cidades erradas e seus respectivos poligonos: {}").format(numero_estado,cnt,cidades_erradas) ) 


