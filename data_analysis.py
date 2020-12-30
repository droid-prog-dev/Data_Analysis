import pandas as pd
import numpy as np
import pickle
import seaborn as sns
from sklearn.preprocessing import MinMaxScaler, RobustScaler, PowerTransformer, StandardScaler
from statsmodels.tsa.stattools import adfuller
from statsmodels.graphics.tsaplots import plot_pacf, plot_acf
from statsmodels.tsa.stattools import pacf
import statsmodels.api as sm
import plotly_express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

st.write("""
# Data Analysis for Surgimedical
	""")

st.write("""
## Loading data
""")


pickle_in = open("facturas.pickle","rb")
df_fact = pickle.load(pickle_in)
df_fact['fecha'] = df_fact['fecha'].apply(lambda x: x[0:2]+'-'+x[3:5]+'-'+x[-4:])
df_fact['fecha']=pd.to_datetime(df_fact['fecha'], format='%d-%m-%Y')
df_fact.set_index('fecha', inplace=True)

df_fact['year'] = df_fact.index.year
df_fact['month'] = df_fact.index.month
df_fact['weekday'] = df_fact.index.weekday
df_fact['day'] = df_fact.index.day
df_fact['quarter'] = df_fact.index.quarter

sutures = df_fact[df_fact['codigo'].str.startswith('SN')]
#sutures.head()
