#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#https://malum.com.br/wp/2019/07/09/avro/

#1
get_ipython().system('pip install avro-python3')
get_ipython().system('pip install requests')
get_ipython().system('pip install pandas')


# In[ ]:


#2
import requests
import pandas as pd


dataRequest = requests.get(url='http://api.tvmaze.com/singlesearch/shows?q=game-of-thrones&embed=episodes')
data_json = dataRequest.json()

from pandas.io.json import json_normalize

data_normalize = json_normalize(
                                    #Dados deserializados
                                    #Unserialized JSON objects
                                    data=data_json['_embedded']
    
                                    #Caminho do objeto aonde estão os dados
                                    #path of the data that you need
                                    ,record_path='episodes'
                                    #Prefixo das colunas. Recomendado usar para evitar ambiguidade de colunas
                                    # Columns prefix. Recommended to use to avoid ambiguous name columns                                    
                                    ,record_prefix='got_'
                                    )
#Listaremos apenas as colunas que precisamos
#Listing only the columns that we will need
data = data_normalize[['got_season','got_number','got_name']]


# In[ ]:


#3
#Usamos 3 aspas duplas para tudo virar string / We use triple doouble quotes to everything inside be string
schema = """{
  "type": "record",
  "name": "GOTLIST",
  "fields" : [
    {"name": "got_season", "type": "int", "doc": "Número da temporada / Season number"},             
    {"name": "got_number", "type": "int", "doc": "Número do episódio / Episode number"},             
    {"name": "got_name", "type": "string", "doc": "Título do episódio / Episode title"}             

  ]
}"""


# In[ ]:


#4
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import pandas as pd

schemaParse = avro.schema.Parse(schema)

#Aqui nós criaremos um arquivo com a lista de episódios com o schema para validação
#Here we create a file of GOT episodes list with the schema for validation
writer = DataFileWriter(open("got_malum.avro", "wb"), DatumWriter(), schemaParse)

# writer.append({"got_season": 1, "got_number": 1, "got_name": "Test"})
#Agora iremos inserir os dados da variavél data no arquivo got_malum.avro
#Now we will insert the data from the data variable in the got_malum.avro file

for index, rows in data.iterrows():

    writer.append({"got_season": rows['got_season'], "got_number": rows['got_number'], "got_name": rows['got_name']})


writer.close()


# In[ ]:


#Lendo o arquivo (deserializar) / reading the file (Deserialization)
reader = DataFileReader(open("got_malum.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()

