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


fig = make_subplots(rows=1, cols=1)

fig.add_trace(go.Scatter(x=df_monthly_sutures.index, y=np.squeeze(df_monthly_sutures.values),
                    mode='lines+markers',
                    name='lines+markers'))
fig.add_trace(go.Bar(x=df_monthly_sutures.index, y=np.squeeze(df_monthly_sutures.values),name='Bar mode'))
fig.update_layout(height=600, width=900, title_text=f"Total Sutures Monthly Output")

st.write(fig)

st.write("Total Monthly Output")

x = sutures.groupby(["year","month"])["cantidad"].sum()
df_wide = x.unstack()
df_wide = df_wide.fillna(value=0)
st.dataframe(df_wide)

f = sutures.groupby(["month","year"])["cantidad"].sum().unstack()
fig1 = px.box(data_frame=f,color_discrete_sequence=px.colors.qualitative.Set2)
st.write(fig1)

plt.figure(figsize=(14, 6))
fig2 = make_subplots(rows=1, cols=1)
f1 = sutures.groupby(["year"])["cantidad"].sum()

fig2.add_trace(go.Scatter(x=f1.index, y=np.squeeze(f1.values),
                    mode='lines+markers',
                    name='Line values'))
fig2.add_trace(go.Bar(x=f1.index, y=np.squeeze(f1.values),name='Bar Values'))
fig2.update_layout(height=600, width=900, title_text=f"Sutures Yearly Output")
st.write(fig2)

lista_prod = np.unique(sutures['codigo'].loc[(sutures['codigo'].str[:2]=='SN') & (sutures['codigo'].str[-1:]!='U')])
st.sidebar.header("Suture Codes:")
option = st.sidebar.selectbox("Code",lista_prod)
st.write('Product:', option)

product = sutures[sutures['codigo']==option]
product_daily = product['cantidad'].resample('D').sum()
product_daily = pd.DataFrame(product_daily, columns=['cantidad'])
product_weekly = product['cantidad'].resample('W').sum()
product_weekly = pd.DataFrame(product_weekly, columns=['cantidad'])
product_monthly = product['cantidad'].resample('M').sum()
product_monthly = pd.DataFrame(product_monthly, columns=['cantidad'])

st.subheader("Product Analysis:")
fig3 = make_subplots(rows=1, cols=1)

fig3.add_trace(go.Scatter(x=product_monthly.index, y=np.squeeze(product_monthly.values),
                    mode='lines+markers',
                    name='lines+markers'))
fig3.add_trace(go.Bar(x=product_monthly.index, y=np.squeeze(product_monthly.values),name='Bar mode'))
fig3.update_layout(height=600, width=900, title_text=f"Monthly Output for : {option}")
st.write(fig3)

x = product.groupby(["year","month"])["cantidad"].sum()
df_wide = x.unstack()
df_wide = df_wide.fillna(value=0)
st.dataframe(df_wide)

f2 = product.groupby(["month","year"])["cantidad"].sum().unstack()
fig4 = px.box(data_frame=f2)
st.write(fig4)

st.subheader("Time Serie components")
decomposition_monthly = sm.tsa.seasonal_decompose(product_monthly, model='additive')
fig5 = decomposition_monthly.plot()
st.write(fig5)
