# -*- coding: utf-8 -*-
"""
Created on Fri Oct 27 10:39:39 2023

@author: L11057
"""

# Se necesita el blpapi instalado en la Bloomberg
# import blpapi
# https://www.bloomberg.com/professional/support/api-library/
# python -m pip install --index-url=https://bcms.bloomberg.com/pip/simple blpapi
# conda install -c conda-forge blpapi

from pybbg_k import Pybbg

session = Pybbg()
"""ultimo_precio = session.bdp(["EUR Curncy","JPY Curncy"], 
                           fld_list=["PX_BID","PX_ASK",  ])
historia_precio = session.bdh(["EUR Curncy","JPY Curncy"], 
                             fld_list=["PX_BID","PX_ASK",  ],
                            start_date="20230101")
"""

historia_prod_ind = session.bdh(["CHVAIOY Index","CHVAIOM Index"], 
                              fld_list=["PX_LAST"],
                              start_date="19900101").sort_index()
import pandas as pd
import numpy as np

rango_fechas = pd.date_range(historia_prod_ind.index[0] - pd.offsets.MonthEnd() * 12,
              historia_prod_ind.index[-1] ,
              freq="M")
historia_prod_ind = historia_prod_ind.reindex(rango_fechas)
historia_prod_ind["CHVAIOY Index"] = historia_prod_ind["CHVAIOY Index"].interpolate()
# yoy_interpolado = historia_prod_ind["CHVAIOY Index"].interpolate()

historia_prod_ind["produccion_china"] = np.nan

fecha_inicial = historia_prod_ind["CHVAIOM Index"].dropna().index[0] - pd.offsets.MonthEnd()
historia_prod_ind["produccion_china"] = historia_prod_ind["CHVAIOM Index"].div(100).add(1).cumprod().dropna().mul(100)
historia_prod_ind.produccion_china[fecha_inicial]=100

fechas_rellenar = historia_prod_ind.index[historia_prod_ind.produccion_china.isna()]
# fecha_rellenar = fechas_rellenar[-1]
for fecha_rellenar in fechas_rellenar[::-1]:
    fecha_referencia = fecha_rellenar+12*pd.offsets.MonthEnd()
    variacion_interanual = historia_prod_ind["CHVAIOY Index"][fecha_referencia]
    indice_fecha_rellenar = historia_prod_ind.produccion_china[fecha_referencia]/(1+variacion_interanual/100)
    historia_prod_ind["produccion_china"][fecha_rellenar]=indice_fecha_rellenar

df_construccion_sustituta = pd.DataFrame(historia_prod_ind["produccion_china"])

# import os
# os.chdir(r"C:/Users/l11876/BCRA")

tickers_indicadores = pd.read_excel('Tickers.xlsx', sheet_name='indicadores')
#tickers_indicadores=pd.read_excel(open('Tickers.xlsx')),sheet_name'Hoja1')

#fechas_rellenar = historia_prod_ind.index[historia_prod_ind.produccion_china.isna()]

tickers_indicadores.columns

lista_tickers = tickers_indicadores.TICKER.dropna()
df_datos = session.bdh(lista_tickers, 
            fld_list=["PX_LAST"],
            start_date="19890101",).sort_index()
# df_datos.sort_index(inplace=True)

indicadores_a_rellenar = tickers_indicadores.TICKER[tickers_indicadores["Completar trimestre hacia atras"] == 1]

columnas_a_rellenar = indicadores_a_rellenar

df_datos[columnas_a_rellenar] = df_datos[columnas_a_rellenar].fillna(method="bfill", limit=2)

indices_sustitutos = tickers_indicadores["Construccion sustituta"].dropna()

df_datos[indices_sustitutos] = df_construccion_sustituta[indices_sustitutos]

df_datos.index = df_datos.index.date

import seaborn as sns
from string import ascii_letters
from sklearn.preprocessing import StandardScaler

# for i in range(0, len(tickers_indicadores)):
#     if tickers_indicadores.loc[i, "TICKER"] == np.nan:
#         tickers_indicadores.loc[i, 'TICKER'] = tickers_indicadores.loc[i, "Construccion sustituta"]
tickers_indicadores.TICKER[7] = tickers_indicadores["Construccion sustituta"][7]

import matplotlib.pyplot as plt
import matplotlib.cm as cm

countries = tickers_indicadores.PAIS.unique()
for country in countries:
    print(country)
    print(tickers_indicadores.TICKER[tickers_indicadores.PAIS==country])
    df_tmp = df_datos[tickers_indicadores.TICKER[tickers_indicadores.PAIS==country]].tail(12)

    # print(df_tmp.shape)
    ss = StandardScaler().fit(df_datos[tickers_indicadores.TICKER[tickers_indicadores.PAIS==country]])
    df_tmp.loc[:,:] = ss.transform(df_tmp)
    
    sns.heatmap(df_tmp.T)


valores = pd.DataFrame(df_datos[['GDP CYOY Index', 'CPI YOY Index']]).tail(24)
valores = valores.dropna()
sns.heatmap(valores.T, cmap = "Blues")


plt.figure(figsize=(10, 10))
plt.pcolormesh(valores, cmap=cm.Reds)
plt.show()




rs = np.random.RandomState(33)
d = pd.DataFrame(data=rs.normal(size=(100, 26)),
                 columns=list(ascii_letters[26:]))

sns.heatmap(d)


sns.heatmap(df_datos)

df_datos.to_excel("df_datos.xlsx")
tickers_indicadores.to_excel("tickers_indicadores.xlsx")

df_datos.to_pickle("df_datos.pkl")
