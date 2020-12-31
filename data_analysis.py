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

st.title("Data Analysis for data analysis")

st.subheader("Loading data:")

sutures = pickle.load(open('dfsutures.pkl','rb'))
st.dataframe(sutures)
#df_fact = pickle.load(open('facturas.pickle','rb'))
#df_fact['fecha'] = df_fact['fecha'].apply(lambda x: x[0:2]+'-'+x[3:5]+'-'+x[-4:])
#df_fact['fecha']=pd.to_datetime(df_fact['fecha'], format='%d-%m-%Y')
#df_fact.set_index('fecha', inplace=True)

#df_fact['year'] = df_fact.index.year
#df_fact['month'] = df_fact.index.month
#df_fact['weekday'] = df_fact.index.weekday
#df_fact['day'] = df_fact.index.day
#df_fact['quarter'] = df_fact.index.quarter

#sutures = df_fact[df_fact['codigo'].str.startswith('SN')]
#sutures.head()
daily_sutures = sutures['cantidad'].resample('D').sum()
df_daily_sutures = pd.DataFrame(daily_sutures, columns=['cantidad'])
weekly_sutures = sutures['cantidad'].resample('W').sum()
df_weekly_sutures = pd.DataFrame(weekly_sutures, columns=['cantidad'])
monthly_sutures = sutures['cantidad'].resample('M').sum()
df_monthly_sutures = pd.DataFrame(monthly_sutures, columns=['cantidad'])


product = sutures[sutures['codigo']==p.value]
product_daily = product['cantidad'].resample('D').sum()
product_daily = pd.DataFrame(product_daily, columns=['cantidad'])
product_weekly = product['cantidad'].resample('W').sum()
product_weekly = pd.DataFrame(product_weekly, columns=['cantidad'])
product_monthly = product['cantidad'].resample('M').sum()
product_monthly = pd.DataFrame(product_monthly, columns=['cantidad'])


lista_prod = np.unique(sutures['codigo'].loc[(sutures['codigo'].str[:2]=='SN') & (sutures['codigo'].str[-1:]!='U')])
st.sidebar.header("Suture Codes:")
option = st.sidebar.selectbox("Code",lista_prod)
st.write('Product:', option)

fig = make_subplots(rows=1, cols=1)

fig.add_trace(go.Scatter(x=df_monthly_sutures.index, y=np.squeeze(df_monthly_sutures.values),
                    mode='lines+markers',
                    name='lines+markers'))
fig.add_trace(go.Bar(x=df_monthly_sutures.index, y=np.squeeze(df_monthly_sutures.values),name='Bar mode'))
fig.update_layout(height=600, width=900, title_text=f"Total Sutures Monthly Output")

st.write(fig)

