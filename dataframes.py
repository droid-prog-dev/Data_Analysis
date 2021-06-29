import pymysql
import pandas as pd
import numpy as np
import pickle
from github import Github
from datetime import date

today = date.today()
today = today.strftime("%d/%m/%Y")

def connectdb1():
     conn = pymysql.connect(host='127.0.0.1', port=3306, user='admin',password='lenova19',db='lenovaeirl')
     return conn
try:

	conn=connectdb1()
	sql = "select date as Fecha, n_invoice as nfact, client.rs as Razon_Social, round((quantity*priceun), 2) importe, round((costoun * quantity), 2) costo, round(((priceun - costoun)*quantity),2) utilidad from invoice join client on invoice.idclient = client.idclient order by idinvoice"
	    
	df=pd.read_sql(sql,con=conn)
	conn.close()

	pickle_out = open("Y:/Lenova/Lenova_data/lenova_facturas.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()


	pickle_out = open("C:/Anaconda36/py36/Lenova_data/lenova_facturas.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()

	## GITHUB UPDATE:
	# g = Github("33393d9a0a861e173aad41489a66a5b2a63ad2e6")
	# repo = g.get_repo("droid-prog-dev/Data_Analysis")
	# contents = repo.get_contents("lenova_facturas.pkl", ref="main")  # Retrieve old file to get its SHA and path
	# repo.update_file("C:/Anaconda36/py36/Lenova_data/lenova_facturas.pkl",f"updated:{today}","updating", contents.sha, branch="main")

	print(f"Se genero el dataframe de lista de facturas LENOVA")

	conn=connectdb1()
	sql = "select invoice.date, product.code, quantity, priceun, invoice.costoun from invoice join product on invoice.idproduct=product.idproduct order by idinvoice"
	df=pd.read_sql(sql,con=conn)
	conn.close()

	pickle_out = open("Y:/Lenova/Lenova_data/lenova_factxprod.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()


	pickle_out = open("C:/Anaconda36/py36/Lenova_data/lenova_factxprod.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()
	print(f"Se genero el dataframe de facturas-producto LENOVA")

	conn=connectdb1()
	sql = "select * from invoice"
	    
	df=pd.read_sql(sql,con=conn)
	conn.close()


	pickle_out = open("Y:/Lenova/Lenova_data/lenova_invoice.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()

	pickle_out = open("C:/Anaconda36/py36/Lenova_data/lenova_invoice.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()
	print(f"Se genero el dataframe de facturas LENOVA")


	conn=connectdb1()
	sql = "select * from product"
	    
	df=pd.read_sql(sql,con=conn)
	conn.close()


	pickle_out = open("Y:/Lenova/Lenova_data/lenova_product.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()

	pickle_out = open("C:/Anaconda36/py36/Lenova_data/lenova_product.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()

	pickle_in = open("C:/Anaconda36/py36/Lenova_data/lenova_factxprod.pkl","rb")
	df_factprod = pickle.load(pickle_in)

	df_fact = df_factprod.loc[df_factprod['code'].str[:2]=='SN']
	df_fact['date'] = df_fact['date'].apply(lambda x: x[0:2]+'-'+x[3:5]+'-'+x[-4:])
	df_fact['date']=pd.to_datetime(df_fact['date'], format='%d-%m-%Y')
	df_fact.set_index('date', inplace=True)
	df_fact['month'] = df_fact.index.month
	df_fact['year'] = df_fact.index.year
	df_fact['total'] = df_fact['quantity'] * df_fact['priceun']

	df = df_fact.groupby(['year','month']).agg({'quantity': 'sum',
                                  'priceun':['max','mean','min'],
                                  'costoun': 'mean',
                                  'total': 'sum'})
	df.columns = df.columns.map('-'.join).str.strip('-')
	format_dict = {'quantity-sum':'{:,.0f}','priceun-mean':'{:,.2f}',
               'priceun-max':'{:,.2f}',
               'priceun-min':'{:,.2f}',
               'costoun-mean':'{:,.2f}', 'total-sum':'{:,.2f}'}
	df.style.format(format_dict).highlight_max(axis=0)

	df.to_pickle('Y:/Lenova/Lenova_data/df_lenova_factxprod.pkl')
	df.to_pickle('C:/Anaconda36/py36/Lenova_data/df_lenova_factxprod.pkl')





	## GITHUB UPDATE:
	# g = Github("33393d9a0a861e173aad41489a66a5b2a63ad2e6")
	# repo = g.get_repo("droid-prog-dev/Data_Analysis")
	# contents = repo.get_contents("lenova_product.pkl", ref="main")  # Retrieve old file to get its SHA and path
	# repo.update_file("C:/Anaconda36/py36/Lenova_data/lenova_product.pkl",f"updated{today}","updating", contents.sha, branch="main")

	print(f"Se genero el dataframe de productos y stock LENOVA")

	
	conn=connectdb1()
	sql = "select * from client"
	    
	df=pd.read_sql(sql,con=conn)
	conn.close()


	pickle_out = open("Y:/Lenova/Lenova_data/lenova_clientes.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()

	pickle_out = open("C:/Anaconda36/py36/Lenova_data/lenova_clientes.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()

	# g = Github("33393d9a0a861e173aad41489a66a5b2a63ad2e6")
	# repo = g.get_repo("droid-prog-dev/Data_Analysis")
	# contents = repo.get_contents("lenova_clientes.pkl", ref="main")  # Retrieve old file to get its SHA and path
	# repo.update_file("C:/Anaconda36/py36/Lenova_data/lenova_clientes.pkl",f"updated:{today}","updating", contents.sha, branch="main")

	print(f"Se genero el dataframe de clientes LENOVA")


	conn=connectdb1()
	sql = "select fecha, numdoc, tipoingreso, rucprov, tipodocprov, docprov, product.code as codigo, cantidad, ir.costoun, importe  from ir inner join product on product.idproduct = ir.idproduct order by idir"
	    
	df=pd.read_sql(sql,con=conn)
	conn.close()


	pickle_out = open("Y:/Lenova/Lenova_data/lenova_compras.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()


	pickle_out = open("C:/Anaconda36/py36/Lenova_data/lenova_compras.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()

	# g = Github("33393d9a0a861e173aad41489a66a5b2a63ad2e6")
	# repo = g.get_repo("droid-prog-dev/Data_Analysis")
	# contents = repo.get_contents("lenova_compras.pkl", ref="main")  # Retrieve old file to get its SHA and path
	# repo.update_file("C:/Anaconda36/py36/Lenova_data/lenova_compras.pkl",f"updated:{today}","updating", contents.sha, branch="main")

	print(f"Se genero el dataframe de compras LENOVA")


	conn=connectdb1()
	sql = "select cxc.n_invoice as nfact, client.rs cliente, cxc.importe importe, (select date from cobranza where idcobranza=(select max(idcobranza) from cobranza where n_invoice=cxc.n_invoice)) as fecha_pago, invoice.date as fecha_emision from cxc inner join invoice on invoice.n_invoice = cxc.n_invoice inner join client on invoice.idclient = client.idclient group by cxc.n_invoice order by cxc.idcxc"
	df=pd.read_sql(sql,con=conn)
	conn.close()


	pickle_out = open("Y:/Lenova/Lenova_data/lenova_creditos.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()


	pickle_out = open("C:/Anaconda36/py36/Lenova_data/lenova_creditos.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()

	# g = Github("33393d9a0a861e173aad41489a66a5b2a63ad2e6")
	# repo = g.get_repo("droid-prog-dev/Data_Analysis")
	# contents = repo.get_contents("lenova_creditos.pkl", ref="main")  # Retrieve old file to get its SHA and path
	# repo.update_file("C:/Anaconda36/py36/Lenova_data/lenova_creditos.pkl",f"updated:{today}","updating", contents.sha, branch="main")

	print(f"Se genero el dataframe de creditos LENOVA")

except Exception as e:
	print(e)

## SURGIMEDICAL DATA:

def connectdb():
     conn = pymysql.connect(host='127.0.0.1', port=3306, user='root',password='lenova14',db='lenova')
     return conn


try:
	conn=connectdb()
	sql = "select fecha, numfact, codigo, umed, cantidad, precioun, round((cantidad*precioun),2) as total, clientes.razonsocial as cliente, pago from facturas inner join clientes on facturas.ruccliente = clientes.ruccliente order by idFactura desc"
    
	df=pd.read_sql(sql,con=conn)
	conn.close()

	pickle_out = open("Y:/Surgim/Surgim_Data_Analysis/surgim_facturas.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()

	pickle_out = open("C:/Anaconda36/py36/Surgim_Data_Analysis/surgim_facturas.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()

	## Lenova data
	data1 = pickle.load(open("C:/Anaconda36/py36/Lenova_data/lenova_invoice.pkl","rb"))
	data1 = data1[['date','idproduct','quantity','priceun','costoun']]
	data2 = pickle.load(open("C:/Anaconda36/py36/Lenova_data/lenova_product.pkl","rb"))
	data2 = data2[['idproduct','code']]
	
	fact_lenova = pd.merge(data1, data2, on='idproduct',how='left')
	fact_lenova.drop(['idproduct','priceun','costoun'], axis=1, inplace=True)
	columns = ['fecha','cantidad','codigo']
	fact_lenova.columns = columns

	fact_lenova['fecha'] = fact_lenova['fecha'].apply(lambda x: x[0:2]+'-'+x[3:5]+'-'+x[-4:])
	fact_lenova['fecha']=pd.to_datetime(fact_lenova['fecha'], format='%d-%m-%Y')
	fact_lenova.set_index('fecha', inplace=True)

	fact_lenova['year'] = fact_lenova.index.year
	fact_lenova['month'] = fact_lenova.index.month
	fact_lenova['weekday'] = fact_lenova.index.weekday
	fact_lenova['day'] = fact_lenova.index.day
	fact_lenova['quarter'] = fact_lenova.index.quarter

	sutures1 = fact_lenova[fact_lenova['codigo'].str.startswith('SN')]
	sutures1 = sutures1.loc['2020-12-01':]

	## Surgimedical data
	df_fact = pickle.load(open("C:/Anaconda36/py36/Surgim_Data_Analysis/surgim_facturas.pkl","rb"))
	df_fact['fecha'] = df_fact['fecha'].apply(lambda x: x[0:2]+'-'+x[3:5]+'-'+x[-4:])
	df_fact['fecha']=pd.to_datetime(df_fact['fecha'], format='%d-%m-%Y')
	df_fact.set_index('fecha', inplace=True)

	df_fact['year'] = df_fact.index.year
	df_fact['month'] = df_fact.index.month
	df_fact['weekday'] = df_fact.index.weekday
	df_fact['day'] = df_fact.index.day
	df_fact['quarter'] = df_fact.index.quarter

	sutures2 = df_fact[df_fact['codigo'].str.startswith('SN')]
	sutures2 = sutures2.drop(['numfact','umed','precioun','total','cliente','pago'],axis=1)
	sutures2 = sutures2.loc[:'2020-11-30']

	## 2014 data
	sutures2014 = pd.read_excel('./Ventas_suturas_Ene-Set2014.xls')
	sutures2014['fecha'] = pd.to_datetime(sutures2014['fecha'], format='%d-%m-%Y')
	sutures2014.set_index('fecha', inplace=True)
	sutures3 = sutures2014.drop(['umed','precio','nfact','cliente'],axis=1)
	sutures3['year']=sutures3.index.year
	sutures3['month']=sutures3.index.month
	sutures3['weekday']=sutures3.index.weekday
	sutures3['day']=sutures3.index.day
	sutures3['quarter']=sutures3.index.quarter

	## total sutures
	tot_sutures = pd.concat([sutures1,sutures2,sutures3],axis=0)
	tot_sutures.sort_index(axis=0, ascending=True, inplace=True)

	tot_sutures.to_pickle('./dfsutures.pkl')
	pickle_out = open("Y:/Surgim/Surgim_Data_Analysis/dfsutures.pkl","wb")
	pickle.dump(tot_sutures, pickle_out)
	pickle_out.close()

	## GITHUB UPDATE:
	
	#g = Github("33393d9a0a861e173aad41489a66a5b2a63ad2e6")
	#repo = g.get_repo("droid-prog-dev/Data_Analysis")
	#contents = repo.get_contents("./dfsutures.pkl", ref="main")  # Retrieve old file to get its SHA and path
	#repo.update_file("dfsutures.pkl",f"updated:{today}","updating", contents.sha, branch="main") 

	print(f"Se genero el dataframe TOTAL de Suturas Lenova y Surgimedical")


	conn=connectdb()
	sql = "select concat(substring(fecha,7,4),'-',substring(fecha,4,2)) as periodo,  concat(substring(fecha,7,4),'-',substring(fecha,4,2),'-',substring(fecha,1,2)) as fecha_, fecha,numfact, giro, cliente, importe, utilidad from listafact order by periodo desc,giro desc"
	df=pd.read_sql(sql,con=conn)
	conn.close()
	
	df[['importe','utilidad']] =df[['importe','utilidad']].apply(lambda x: round(x,2))
	df['Rentab(%)'] = ((df['utilidad'] / df['importe'])*100).apply(lambda x: round(x,2))


	pickle_out = open("Y:/Surgim/Surgim_Data_Analysis/surgim_listafact.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()

	pickle_out = open("C:/Anaconda36/py36/Surgim_Data_Analysis/surgim_listafact.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()


	#g = Github("33393d9a0a861e173aad41489a66a5b2a63ad2e6")
	#repo = g.get_repo("droid-prog-dev/Data_Analysis")
	#contents = repo.get_contents("listafact.pkl", ref="main")  # Retrieve old file to get its SHA and path
	#repo.update_file("C:/Anaconda36/py36/Surgim_Data_Analysis/listafact.pkl",f"updated:{today}","updating", contents.sha, branch="main")

	print(f"Se genero el dataframe de Listafact Surgimedical")

	conn=connectdb()
	sql = "select ruccliente as ruc, razonsocial as cliente, dircliente as direccion, contacto, tel, giro from clientes order by ClienteId desc"
	df=pd.read_sql(sql,con=conn)
	conn.close()
	
	pickle_out = open("Y:/Surgim/Surgim_Data_Analysis/surgim_clientes.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()

	pickle_out = open("C:/Anaconda36/py36/Surgim_Data_Analysis/surgim_clientes.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()

	#g = Github("33393d9a0a861e173aad41489a66a5b2a63ad2e6")
	# repo = g.get_repo("droid-prog-dev/Data_Analysis")
	# contents = repo.get_contents("clientes.pkl", ref="main")  # Retrieve old file to get its SHA and path
	# repo.update_file("C:/Anaconda36/py36/Surgim_Data_Analysis/clientes.pkl",f"updated:{today}","updating", contents.sha, branch="main")


	clientes_surgim = pickle.load(open("C:/Anaconda36/py36/Surgim_Data_Analysis/surgim_clientes.pkl","rb"))
	clientes_lenova = pickle.load(open("C:/Anaconda36/py36/Lenova_data/lenova_clientes.pkl","rb"))

	clientes_surgim = clientes_surgim[['ruc','cliente','contacto','giro']]
	clientes_lenova = clientes_lenova[['ruc','rs','contact','market']]
	columns = ['ruc','cliente','contacto','giro']
	clientes_lenova.columns = columns
	
	clientes = pd.concat([clientes_surgim, clientes_lenova]).drop_duplicates()
	clientes.sort_values(by=['cliente'], inplace=True, ascending=True)
	clientes.fillna("",inplace=True)

	clientes.to_pickle('./dfclientes.pkl')
	pickle_out = open("Y:/Surgim/Surgim_Data_Analysis/dfclientes.pkl","wb")
	pickle.dump(clientes, pickle_out)
	pickle_out.close()

	print(f"Se genero el dataframe de CLIENTES")

	#DATAFRAME DE FACTURAS SURGIMEDICAL DESDE OCT-2014 HASTA 02 OCT 2020
	fact_surgim = pickle.load(open("C:/Anaconda36/py36/Surgim_Data_Analysis/surgim_listafact.pkl","rb"))
	fact_lenova = pickle.load(open("C:/Anaconda36/py36/Lenova_data/lenova_facturas.pkl","rb"))

	fact_surgim = fact_surgim[['fecha_','periodo','numfact','cliente','giro','importe','utilidad']]
	fact_surgim['costo'] = fact_surgim['importe'] - fact_surgim['utilidad']
	columns = ['fecha','periodo','nfact','cliente','giro','importe','utilidad','costo']
	fact_surgim.columns = columns
	fact_surgim['fecha'] = pd.to_datetime(fact_surgim['fecha'], format='%Y-%m-%d')
	fact_surgim.set_index('fecha', inplace=True)
	fact_surgim = fact_surgim.loc[:'2020-10-02']

	#DATAFRAME DE FACTURAS LENOVA

	fact_lenova['periodo'] = fact_lenova['Fecha'].apply(lambda x: x[-4:] +'-' + x[3:5])
	fact_lenova['Fecha'] = fact_lenova['Fecha'].apply(lambda x: x[0:2]+'-'+x[3:5]+'-'+x[-4:])
	columns = ['fecha','nfact','cliente','importe','utilidad','costo','periodo']
	fact_lenova.columns = columns
	fact_lenova = pd.merge(fact_lenova, clientes, on='cliente',how='left')
	fact_lenova = fact_lenova[['fecha','periodo','nfact','cliente','giro','importe','utilidad','costo']]
	fact_lenova['fecha'] = pd.to_datetime(fact_lenova['fecha'], format='%d-%m-%Y')
	fact_lenova = fact_lenova.groupby(['nfact','fecha','periodo','cliente','giro']).sum().reset_index()
	fact_lenova.set_index('fecha', inplace=True)
	fact_lenova.sort_index(axis=0, ascending=True, inplace=True)
	
	fact_total = pd.concat([fact_surgim, fact_lenova], axis=0, ignore_index=False)
	fact_total.sort_values(by=['fecha'], axis=0, ascending=True, inplace=True)

	vta_cliente = fact_total.pivot_table(values='importe',index=['cliente'],columns=['periodo'], fill_value=0.0, aggfunc=np.sum).reset_index()

	vta_cliente.to_pickle('./dfvtaxcliente.pkl')
	vta_cliente.to_pickle("Y:/Surgim/Surgim_Data_Analysis/dfvtaxcliente.pkl")
	#pickle_out = open("Y:/Surgim/Surgim_Data_Analysis/dfvtaxcliente.pkl","wb")
	#pickle.dump(vta_cliente, pickle_out)
	#pickle_out.close()

	# df_listafact = pickle.load(open("C:/Anaconda36/py36/Surgim_Data_Analysis/surgim_listafact.pkl","rb"))
	
	# df_listafact_pivot = df_listafact.pivot_table(values='importe',index=['giro','cliente'],columns=['periodo'], fill_value='', aggfunc=np.sum)
	# df_listafact_pivot = df_listafact_pivot.reset_index()

	# pickle_out = open("Y:/Surgim/Surgim_Data_Analysis/listafact_pivot.pkl","wb")
	# pickle.dump(df_listafact_pivot, pickle_out)
	# pickle_out.close()

	# pickle_out = open("C:/Anaconda36/py36/Surgim_Data_Analysis/listafact_pivot.pkl","wb")
	# pickle.dump(df_listafact_pivot, pickle_out)
	# pickle_out.close()

	print(f"Se genero el dataframe de FACTURAS X CLIENTE: SURGIMEDICAL + LENOVA")


	
	conn=connectdb()
	sql = "select fecha, codprod as codigo, umedida as umed, cantidad, costoun, proveedores.razonsoc as proveedor, docprov from inforrecep inner join proveedores on inforrecep.rucprov = proveedores.rucprov order by idInforRecep desc"
	df=pd.read_sql(sql,con=conn)
	conn.close()

	pickle_out = open("Y:/Surgim/Surgim_Data_Analysis/surgim_ir.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()

	pickle_out = open("C:/Anaconda36/py36/Surgim_Data_Analysis/surgim_ir.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()

	# g = Github("33393d9a0a861e173aad41489a66a5b2a63ad2e6")
	# repo = g.get_repo("droid-prog-dev/Data_Analysis")
	# contents = repo.get_contents("ir.pkl", ref="main")  # Retrieve old file to get its SHA and path
	# repo.update_file("C:/Anaconda36/py36/Surgim_Data_Analysis/ir.pkl",f"updated:{today}","updating", contents.sha, branch="main")

	print(f"Se genero el dataframe de Informes de recepcion Surgimedical")


	conn=connectdb()
	sql = "select codprod as codigo, descrip as producto, lote, umed, marca, stock, costoun, vigencia from producto"
	df=pd.read_sql(sql,con=conn)
	conn.close()

	pickle_out = open("Y:/Surgim/Surgim_Data_Analysis/surgim_producto.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()


	pickle_out = open("C:/Anaconda36/py36/Surgim_Data_Analysis/surgim_producto.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()

	# g = Github("33393d9a0a861e173aad41489a66a5b2a63ad2e6")
	# repo = g.get_repo("droid-prog-dev/Data_Analysis")
	# contents = repo.get_contents("producto.pkl", ref="main")  # Retrieve old file to get its SHA and path
	# repo.update_file("C:/Anaconda36/py36/Surgim_Data_Analysis/productos.pkl",f"updated:{today}","updating", contents.sha, branch="main")

	print(f"Se genero el dataframe de Productos Surgimedical")

	conn=connectdb()
	sql = "select codprod as codigo, descrip as producto, lote, umed, marca, stock, costoun, round((costoun/1.18),2) costounsigv, round((stock*costoun/1.18),2) as costototal  from producto where vigencia='V' and stock > 0"
	df=pd.read_sql(sql,con=conn)
	conn.close()


	pickle_out = open("Y:/Surgim/Surgim_Data_Analysis/surgim_stock.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()


	pickle_out = open("C:/Anaconda36/py36/Surgim_Data_Analysis/surgim_stock.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()

	## GITHUB UPDATE:
	# g = Github("33393d9a0a861e173aad41489a66a5b2a63ad2e6")
	# repo = g.get_repo("droid-prog-dev/Data_Analysis")
	# contents = repo.get_contents("stock_surgim.pkl", ref="main")  # Retrieve old file to get its SHA and path
	# repo.update_file("stock_surgim.pkl",f"updated{today}","updating", contents.sha, branch="main")
	
	print(f"Se genero el dataframe de Stock Surgimedical")

	conn=connectdb()
	sql = "select factxcob.nfact as nfact, factxcob.cliente as cliente , factxcob.importe as importe, (select fecha from cobranza where idcob=(select max(idcob) from cobranza where nfact=factxcob.nfact)) as fecha_pago, factxcob.fecha as fecha_emision from factxcob order by id desc"
	df=pd.read_sql(sql,con=conn)
	conn.close()
		
	pickle_out = open("Y:/Surgim/Surgim_Data_Analysis/surgim_creditos.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()

	pickle_out = open("C:/Anaconda36/py36/Surgim_Data_Analysis/surgim_creditos.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()

	# g = Github("33393d9a0a861e173aad41489a66a5b2a63ad2e6")
	# repo = g.get_repo("droid-prog-dev/Data_Analysis")
	# contents = repo.get_contents("creditos.pkl", ref="main")  # Retrieve old file to get its SHA and path
	# repo.update_file("C:/Anaconda36/py36/Surgim_Data_Analysis/creditos.pkl",f"updated:{today}","updating", contents.sha, branch="main")

	print(f"Se genero el dataframe de Creditos Surgimedical")


	conn=connectdb()
	sql = "select nfact,cliente,fecha,importe from factxcob where estado = 'p' order by id desc"
	df=pd.read_sql(sql,con=conn)
	conn.close()
	
	pickle_out = open("Y:/Surgim/Surgim_Data_Analysis/surgim_cxc.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()

	pickle_out = open("C:/Anaconda36/py36/Surgim_Data_Analysis/surgim_cxc.pkl","wb")
	pickle.dump(df, pickle_out)
	pickle_out.close()

	# g = Github("33393d9a0a861e173aad41489a66a5b2a63ad2e6")
	# repo = g.get_repo("droid-prog-dev/Data_Analysis")
	# contents = repo.get_contents("cxc.pkl", ref="main")  # Retrieve old file to get its SHA and path
	# repo.update_file("C:/Anaconda36/py36/Surgim_Data_Analysis/cxc.pkl",f"updated:{today}","updating", contents.sha, branch="main")

	print(f"Se genero el dataframe de Cuentas por Cobrar Surgimedical")

except Exception as e:
	print(e)