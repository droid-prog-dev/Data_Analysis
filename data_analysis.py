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

st.subheader("Stock de Productos:")

stock_surgim = pickle.load(open("./data/surgim_stock.pkl","rb"))
stock_lenova = pickle.load(open("./data/lenova_product.pkl","rb"))
stock_surgim = stock_surgim[['codigo','producto','stock','costoun']]
stock_lenova = stock_lenova[['code','product','stock','costoun']]
titles = ['codigo','producto','stock','costoun']
stock_lenova.columns = titles

df_stock = stock_surgim.merge(stock_lenova, how='outer', on=['codigo','producto'], suffixes=("_s","_l"))
df_stock.fillna(0, inplace=True)
df_stock.sort_values(by=['producto'], ascending=False, inplace=True)
df_stock['total'] = df_stock['stock_s'] + df_stock['stock_l']
df_stock['S/.'] = df_stock['stock_s']*df_stock['costoun_s'] + df_stock['stock_l']*df_stock['costoun_l']
df_stock['S/.'] = df_stock['S/.'].round(2)


df_stock_sutures = df_stock.loc[df_stock['codigo'].str.startswith('SN')]
df_stock_sutures.sort_values(by=['codigo'], ascending=False, inplace=True)

st.dataframe(df_stock)
st.write(f"Total stock value S/.{round(np.sum(df_stock['S/.']),2)}")

st.subheader("Stock de suturas:")

st.dataframe(df_stock_sutures)
st.write(f"Total stock value S/.{round(np.sum(df_stock_sutures['S/.']),2)}")


st.subheader("Salida de suturas:")
sutures = pickle.load(open('./data/dfsutures.pkl','rb'))

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
fig1 = px.violin(data_frame=f, box=True, points='all',
				title='Total Monthly Output by Year', width=850, height=550,
				color_discrete_sequence=px.colors.qualitative.Plotly,
				)
st.write(fig1)

fig2 = make_subplots(rows=1, cols=1)
f1 = sutures.groupby(["year"])["cantidad"].sum()

fig2.add_trace(go.Scatter(x=f1.index, y=np.squeeze(f1.values),
                    mode='lines+markers',
                    name='Line values'))
fig2.add_trace(go.Bar(x=f1.index, y=np.squeeze(f1.values),name='Bar Values'))
fig2.update_layout(height=500, width=800, title_text=f"Sutures Yearly Output")
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
fig4 = px.violin(data_frame=f2, box=True, points='all',
				title=f'Total Monthly Output for:{option}', width=850, height=550,
				color_discrete_sequence=px.colors.qualitative.Plotly,
				)
st.write(fig4)

## Time Series Analysis:
st.markdown("Time Serie components")
decomposition_monthly = sm.tsa.seasonal_decompose(product_monthly, model='additive')
fig5 = decomposition_monthly.plot()
st.write(fig5)

## Analysis for Clients:
st.subheader("Venta x Cliente:")
df_cliente = pickle.load(open('./data/dfclientes.pkl','rb'))
lista_cliente = np.unique(df_cliente['cliente'])

st.sidebar.header("Cliente:")
option1 = st.sidebar.selectbox("Cliente:",lista_cliente)
st.write('Cliente:', option1)

vtaxcli = pickle.load(open('./data/dfvtaxcliente.pkl','rb'))
selection = vtaxcli.loc[vtaxcli['cliente'] == option1]

st.dataframe(selection)

fig6 = make_subplots(rows=1, cols=1)
fig6.add_trace(go.Scatter(x=selection.columns[1:], y=np.squeeze(selection.iloc[:,1:].values),
                    mode='lines+markers', name='Ventas S/.'))
fig6.add_trace(go.Bar(x=selection.columns[1:], y=np.squeeze(selection.iloc[:,1:].values),name='Bar mode'))
fig6.update_layout(height=600, width=900, title_text=f"Monthly Sales S/:")
st.write(fig6)

