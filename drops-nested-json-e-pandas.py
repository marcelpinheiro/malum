#!/usr/bin/env python
# coding: utf-8

# In[1]:


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
print(data)

