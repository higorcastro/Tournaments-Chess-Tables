# -*- coding: utf-8 -*-
"""
Created on Tue Aug 31 20:17:23 2021

@author: HigorFPCastro
"""

#===========================================================================================
#0 ALGUNS PACOTES ÚTEIS
#===========================================================================================

#import numpy as np
#import requests
#import json
#from pandas.io.json import json_normalize
#from collections import defaultdict
#import swifter 
# import ssl


import urllib.request as urllib2
import pandas as pd
from functools import reduce
import time

start = time.time()


import os, ssl
if (not os.environ.get('PYTHONHTTPSVERIFY', '') and
getattr(ssl, '_create_unverified_context', None)):
    ssl._create_default_https_context = ssl._create_unverified_context


#===========================================================================================
#1º GERA UMA LISTA COM IDS DOS TORNEIOS CRIADOS POR UM USUÁRIO
#===========================================================================================

edicao_torneio='412_teste'


#acessa o site que gera os dados sobre os torneios criados por um usuário
torneios_01 = urllib2.urlopen("https://lichess.org/api/user/excaliburdeavalon/tournament/created").read()    
torneios01_ = pd.read_json(torneios_01, lines=True)
#torneios01 = pd.read_json('created_excaliburdeavalont_01_10_2021', lines=True)

#comando para remover linhas baseadas num intervalo dentro da coluna referente à data do torneio
torneios01=torneios01_.drop(torneios01_.index[(torneios01_['startsAt'] < 1639360800000)])

print(torneios01.tail(1))

'''
#SE MAIS DE UM USUARIO TIVER CRIADO OS TORNEIOS:
#acessa o site que gera os dados sobre os torneios criados por outro usuário
torneios_02 = urllib2.urlopen("https://lichess.org/api/user/higorfpcastro/tournament/created").read()    
torneios02 = pd.read_json(torneios_02, lines=True)
#torneios02 = pd.read_json('created_higorfpcastro21_01_2022', lines=True)
'''

#Etapa para utilizar apenas ids dos torneios de interesse:
    
df_01=pd.DataFrame(torneios01)

#caso nais de um usuário tenha criado os torneios, incluir este:
#df_02=pd.DataFrame(torneios02)

'''
#Deleta linhas que contém outros torneios criados pelo usuário A
df_criados01=df_01[(df_01.id != 'mKAPCW5P') & (df_01.id != '5H5fwukW') & (df_01.id != 'WiqXKTyr') & 
        (df_01.id != 'h0W3pZkB') & (df_01.id != 'gYs57D2T') & (df_01.id != '9DRwPmZk') &
        (df_01.id != 'cDWnXh2L') & (df_01.id != 'OQCREPcE') & (df_01.id != 'Xou72vBt')]
'''

#comando que selecionara apenas as linhas de interesse relativo aos torneios criados pelo usuário A
df_criados01=df_01[df_01.fullName.str.contains('TRENZINHO', case=False)]

'''
#SE MAIS DE UM USUARIO TIVER CRIADO OS TORNEIOS:
#comando que selecionara apenas as linhas de interesse relativo aos torneios criados pelo usuário B
df_criados01=df_01[df_01.fullName.str.contains('TRENZINHO', case=False)]
'''

'''
#SE MAIS DE UM USUARIO TIVER CRIADO OS TORNEIOS:
#Comando para unir dados de dois usuários (caso mais de um usuário tenha criado os torneios)   
#df_todos_criados=pd.concat([df_criados01, df_criados02])
'''

#Comando para ser usado caso apenas um usuáro seja o criador dos torneios
#df_todos_criados=df_criados01

#comando para ser usado quando não houver linhas relativas a torneios a serem excluídas
df_todos_criados=df_01


id_ = df_todos_criados['id']

print(id_)


#===========================================================================================
#2º GERA UMA LISTA COM LINKS DE ACESSO AOS RESULTADOS DOS TORNEIOS CRIADOS A PARTIR DOS IDS
#===========================================================================================

lista1 = []

#Comando para gerar links de acesso a cada resultado de um torneio especifio 
for i in id_:
    url="https://lichess.org/api/tournament/"+i+"/results?as=csv"
    lista1.append(url)
    df1=pd.DataFrame(lista1)
    
    #Segundo método para excluir os torneios criados que não são do SLT
    #df2=df1[:-44]

#===========================================================================================
#3º ACESSA OS RESULTADOS A PARTIR DOS LINKS CRIADOS
#===========================================================================================

colunas=['Rank','Title','username','Rating','Score','Performance']

dfs = []
urls=df1

for i in range(len(df1)):
    url_i=df1[0][i]
    for c in colunas:
        dfs1 = []
        df = pd.read_csv(url_i, sep=',', usecols=[2,3,4])
        dfs1.append(df)
    dfs.append(pd.concat(dfs1, axis=1))

Consolidado = reduce(lambda  left,right: pd.merge(left,right,on='Username', how='outer'),dfs)

#Salva tabela contendo dados de todas as participações em .csv e excel    
Consolidado.to_csv('Consolidado_'+edicao_torneio+'.csv', index=False)
Consolidado.to_excel('Consolidado_'+edicao_torneio+'.xlsx', index=False)

print(Consolidado)
    
#===========================================================================================
#4º EFETUA OS CÁLCULOS A PARTIR DOS RESULTADOS
#===========================================================================================
 
   
df3=pd.read_csv('Consolidado_'+edicao_torneio+'.csv',sep=',',header=None, skiprows=1)


#Realiza operacoes em colunas pares e impares isoladamente
df3 = df3.assign(Rating_Medio = df3[df3.columns[1::2]].mean(axis=1),
                           Pontos = df3[df3.columns[2::2]].sum(axis=1), 
               Participacoes = df3[df3.columns[1::2]].count(axis=1))

#Ordena os dados do maior para o menor valor baseado nas colunas selecionadas
df3 = df3.sort_values(['Pontos','Rating_Medio'],ascending=[False,False]).round()

df3.rename(columns={0: 'Username'}, inplace=True)


#d_parcial = ['Username','Rating_Medio','Pontos']
d_parcial_geral = ['Username','Rating_Medio','Pontos','Participacoes']


#Salva colunas especificas da tabela contendo a classificação geral dos pontos em excel
df3.loc[:, d_parcial_geral].to_csv('Resultado_Trenzinho_das_Onze_'+edicao_torneio+'.csv', sep='\t', index=False)
df3.loc[:, d_parcial_geral].to_excel('Resultado_Trenzinho_das_Onze_'+edicao_torneio+'.xlsx', index=False)

#===========================================================================================
#5º GERA TABELA DE VENCEDORES DE TODAS AS EDIÇÕES
#===========================================================================================

#arquivo que contém a classificação geral 
d_geral=pd.read_excel('Resultado_Trenzinho_das_Onze_'+edicao_torneio+'.xlsx')
d_geral.rename(columns = {'Username':'vencedores'}, inplace = True)


#seleciona apenas as colunas de interesse dentro do arquivo combinado
vencedores=df_todos_criados['winner'].str['name']
torneios=df_todos_criados['fullName']


df_vencedores=pd.concat([torneios,vencedores],axis=1)

#renomeia as colunas
df_vencedores.rename(columns = {'winner':'vencedores','fullName':'Torneio'}, inplace = True)

#mostra a frequencia de vitórias dos participantes
frequencia_ = df_vencedores['vencedores'].value_counts()

#transforma os indices em coluna
frequencia=frequencia_.reset_index()

#renomeia as colunas
frequencia = frequencia.rename(columns = {'index':'vencedores','vencedores':'vitorias'})

#Combina as informações referentes aos usuários em um único dataframe 
Resultado_vencedores_ = pd.merge(d_geral, frequencia)

#utiliza dois critérios para ordenar o resultado
Resultado_vencedores = Resultado_vencedores_.sort_values(['vitorias','Pontos'], ascending=False)

#organiza as colunas com uma determinada ordem
Resultado_vencedores = Resultado_vencedores[['vencedores','vitorias','Pontos','Rating_Medio','Participacoes']]

#salva o arquivo contendo a tabela dos vencedores em excel e .csv
Resultado_vencedores.to_excel('vencedores_Trenzinho_das_Onze_ate_'+edicao_torneio+'.xlsx', index=False)
Resultado_vencedores.to_csv('vencedores_Trenzinho_das_Onze_ate_'+edicao_torneio+'.csv', sep='\t', index=False)



#################################################################################################
#6º AGRUPA OS JOGADORES POR FAIXA DE RATING
#################################################################################################


import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas as pd
#import matplotlib.pyplot as plt
#import numpy as np


df=pd.read_csv('Resultado_Trenzinho_das_Onze_'+edicao_torneio+'.csv',sep='\t',header=None, skiprows=1)

#Comando para nomear as colunas
df.rename(columns={0: 'Username', 1: 'Rating_Medio', 2: 'Pontos',3: 'Participações'}, inplace=True)

#limites de ratings
a0=800; a=900;A=1000; B=1100;  C=1200; D=1300; E=1400; F=1500; G=1600; H=1700; 
I=1800; J=1900; K=2000; L=2100; M=2200; N=2300; O=2400; P=2500; Q=2600

#seleciona uma faixa de valores
df800=df.loc[(df['Rating_Medio'].astype(int) < a0)]
#df800.columns = pd.MultiIndex.from_product([['Ranking abaixo de 800'], df800.columns])

#seleciona uma faixa de valores
df900=df.loc[(df['Rating_Medio'].astype(int) < a) & (df['Rating_Medio'].astype(int) >=a0 )]
#df900.columns = pd.MultiIndex.from_product([['Ranking de 800 a 900'], df900.columns])

#seleciona uma faixa de valores
df1000=df.loc[(df['Rating_Medio'].astype(int) < A) & (df['Rating_Medio'].astype(int) >=a )]
#df1000.columns = pd.MultiIndex.from_product([['Ranking de 900 a 1000'], df1000.columns])

#seleciona uma faixa de valores
df1100=df.loc[(df['Rating_Medio'].astype(int) < B) & (df['Rating_Medio'].astype(int) >=A )]
#df1100.columns = pd.MultiIndex.from_product([['Ranking de 1000 a 1100'], df1100.columns])

df1200=df.loc[(df['Rating_Medio'].astype(int) < C) & (df['Rating_Medio'].astype(int) >=B )]
#df1200.columns = pd.MultiIndex.from_product([['Ranking de 1100 a 1200'], df1200.columns])

df1300=df.loc[(df['Rating_Medio'].astype(int) < D) & (df['Rating_Medio'].astype(int) >=C )]
#df1300.columns = pd.MultiIndex.from_product([['Ranking de 1200 a 1300'], df1300.columns])

df1400=df.loc[(df['Rating_Medio'].astype(int) < E) & (df['Rating_Medio'].astype(int) >=D )]
#df1400.columns = pd.MultiIndex.from_product([['Ranking de 1300 a 1400'], df1400.columns])

df1500=df.loc[(df['Rating_Medio'].astype(int) < F) & (df['Rating_Medio'].astype(int) >=E )]
#df1500.columns = pd.MultiIndex.from_product([['Ranking de 1400 a 1500'], df1500.columns])

df1600=df.loc[(df['Rating_Medio'].astype(int) < G) & (df['Rating_Medio'].astype(int) >=F )]
#df1600.columns = pd.MultiIndex.from_product([['Ranking de 1500 a 1600'], df1600.columns])

df1700=df.loc[(df['Rating_Medio'].astype(int) < H) & (df['Rating_Medio'].astype(int) >=G )]
#df1700.columns = pd.MultiIndex.from_product([['Ranking de 1600 a 1700'], df1700.columns])

df1800=df.loc[(df['Rating_Medio'].astype(int) < I) & (df['Rating_Medio'].astype(int) >=H )]
#df1800.columns = pd.MultiIndex.from_product([['Ranking de 1700 a 1800'], df1800.columns])

df1900=df.loc[(df['Rating_Medio'].astype(int) < J) & (df['Rating_Medio'].astype(int) >=I )]
#df1900.columns = pd.MultiIndex.from_product([['Ranking de 1800 a 1900'], df1900.columns])

df2000=df.loc[(df['Rating_Medio'].astype(int) < K) & (df['Rating_Medio'].astype(int) >=J )]
#df2000.columns = pd.MultiIndex.from_product([['Ranking de 1900 a 2000'], df2000.columns])

df2100=df.loc[(df['Rating_Medio'].astype(int) < L) & (df['Rating_Medio'].astype(int) >=K )]
#df2100.columns = pd.MultiIndex.from_product([['Ranking de 2000 a 2100'], df2100.columns])

df2200=df.loc[(df['Rating_Medio'].astype(int) < M) & (df['Rating_Medio'].astype(int) >=L )]
#df2200.columns = pd.MultiIndex.from_product([['Ranking de 2100 a 2200'], df2200.columns])

df2300=df.loc[(df['Rating_Medio'].astype(int) < N) & (df['Rating_Medio'].astype(int) >=M )]
#df2300.columns = pd.MultiIndex.from_product([['Ranking de 2200 a 2300'], df2300.columns])

df2400=df.loc[(df['Rating_Medio'].astype(int) < O) & (df['Rating_Medio'].astype(int) >=N )]
#df2400.columns = pd.MultiIndex.from_product([['Ranking de 2300 a 2400'], df2400.columns])

df2500=df.loc[(df['Rating_Medio'].astype(int) < P) & (df['Rating_Medio'].astype(int) >=O )]
#df2500.columns = pd.MultiIndex.from_product([['Ranking de 2400 a 2500'], df2500.columns])

df2600=df.loc[(df['Rating_Medio'].astype(int) < Q) & (df['Rating_Medio'].astype(int) >=P )]
#df2600.columns = pd.MultiIndex.from_product([['Ranking de 2500 a 2600'], df2600.columns])

df3000=df.loc[(df['Rating_Medio'].astype(int) >= Q) ]
#df2700.columns = pd.MultiIndex.from_product([['Ranking acima de 2600'], df2700.columns])


#################################################################################################################
###Metoto com todos os participantes em ordem de classificacao por rating
#df_total=pd.concat([df800,df900,df1000,df1100,df1200,df1300,df1400,df1500,df1600,df1700,df1800,
                    #df1900,df2000,df2100,df2200,df2300,df2400,df2500,df2600,df3000],ignore_index=False)
#comando para salvar o arquivo em formato .csv
#df_total.to_csv('Classificacao_Final_0000_a_130.csv', index=False)
#mostra o resultado
#print (df_total)               
#################################################################################################################

df_sub800=df800.iloc[:10]
df_sub900=df900.iloc[:5]
df_sub1000=df1000.iloc[:5]
df_sub1100=df1100.iloc[:5]
df_sub1200=df1200.iloc[:5]
df_sub1300=df1300.iloc[:5]
df_sub1400=df1400.iloc[:5]
df_sub1500=df1500.iloc[:5]
df_sub1600=df1600.iloc[:5]
df_sub1700=df1700.iloc[:5]
df_sub1800=df1800.iloc[:5]
df_sub1900=df1900.iloc[:5]
df_sub2000=df2000.iloc[:5]
df_sub2100=df2100.iloc[:5]
df_sub2200=df2200.iloc[:5]
df_sub2300=df2300.iloc[:5]
df_sub2400=df2400.iloc[:5]
df_sub2500=df2500.iloc[:5]
df_sub2600=df2600.iloc[:5]
df_sub3000=df3000.iloc[:5]

#Seleciona os 5 melhores por rating
df_categorias=pd.concat([df_sub800,df_sub900,df_sub1000,df_sub1100,df_sub1200,df_sub1300,df_sub1400,df_sub1500,
                         df_sub1600,df_sub1700,df_sub1800,df_sub1900,df_sub2000,df_sub2100,df_sub2200,df_sub2300,
                         df_sub2400,df_sub2500,df_sub2600,df_sub3000],ignore_index=True)



df_categorias.to_excel('Categorias_'+edicao_torneio+'.xlsx', index=True)

print(df_categorias)

print(df_vencedores)

print(Resultado_vencedores)

end = time.time()

print("The time of execution of above program is :", end-start)
#===========================================================================================
#===========================================================================================























