from dateutil import parser
import pandas as pd
from datetime import datetime
from pymongo import *
from pprint import pprint
import matplotlib.pyplot as plt
import numpy as np



def getOrders():
    
    ''' retorna un DataFrame con las ordenes  a partir del 01-12-2022'''
    iso = parser.parse("2022-12-28T00:00:00-03:00")
    uri=('mongodb://user@45.55.142.191:4706/password')
    client = MongoClient(uri)

    mydatabase = client.deliveratedb
    mydatabase.list_collection_names()
    collection = mydatabase["orders"].find({'created_date': { '$gt':iso}                                   
                                     },{'created_date':1,'shift':1,'city':1,'shop_name':1,'shift':1})
    list_col = list(collection)
    df=pd.DataFrame(list_col)
    return df
    
def getOrdersV2(start_date, end_date, shop):
    
    ''' retorna un DataFrame con las ordenes '''
    iso = parser.parse(start_date)

    
     
    iso2 = parser.parse(end_date)

    td = pd.Timedelta(3, "h")

   
        
    uri=('mongodb://user@45.55.142.191:4706/password')
    client = MongoClient(uri)

    mydatabase = client.deliveratedb
    mydatabase.list_collection_names()
    if (shop=='todos'):
        collection = mydatabase["orders"].find({ '$and': [{'created_date': { '$gt':(iso+td),'$lt':(iso2+td)}                               
                                     }]},{'created_date':1,'shift':1,'delay_start_time':1, 'delay_end_time':1,'start_kitchen_delay':1, 'withdrawn_delay':1,
                                          'confirmed_delay':1,'order_on_kitchen_ready_time':1,
                                          'kitchen_ready_minutes':1,'dboy_sent_date':1,
                                          'sent_minutes':1,'shop_on_hold_time':1,
                                          'waiting_minutes_shop':1, 'withdrawn_date':1, 'withdrawn_minutes':1,
                                          'waiting_minutes_consumer':1, 'confirmed_date':1, 'final_deliver_time':1,
                                          'consumer_on_hold_time':1,'shop_name':1,'consumer_canceled_date':1,
                                          'city':1,'order_number':1,'notes':1,'scheduled_datetime':1})
    else:    
        collection = mydatabase["orders"].find({ '$and': [{'created_date': { '$gt':(iso+td),'$lt':(iso2+td)}, 'shop_name' : shop                                
                                     }]},{'created_date':1,'shift':1,'delay_start_time':1, 'delay_end_time':1,'start_kitchen_delay':1, 'withdrawn_delay':1,
                                          'confirmed_delay':1,'order_on_kitchen_ready_time':1,
                                          'kitchen_ready_minutes':1,'dboy_sent_date':1,
                                          'sent_minutes':1,'shop_on_hold_time':1,
                                          'waiting_minutes_shop':1, 'withdrawn_date':1, 'withdrawn_minutes':1,
                                          'waiting_minutes_consumer':1, 'confirmed_date':1, 'final_deliver_time':1,
                                          'consumer_on_hold_time':1,'shop_name':1,'consumer_canceled_date':1,
                                          'city':1,'order_number':1,'notes':1,'scheduled_datetime':1})
    list_col = list(collection)
    df=pd.DataFrame(list_col)
    return df                                 


def getShops():
    
    ''' retorna un dataframe con los clientes'''
  
    uri=('mongodb://user@45.55.142.191:4706/password')
    client = MongoClient(uri)
    start_date = '2022-08-13'
    iso = parser.parse(start_date)
    mydatabase = client.deliveratedb
    mydatabase.list_collection_names()
    #collection = mydatabase["orders"].dinstinct('shop_name' ,{'created_date': { '$gte':iso}})
    collection = mydatabase["orders"].find({'created_date': { '$gte':iso}                             
                                     }).distinct('shop_name')
    list_col = list(collection)
    df=pd.DataFrame(list_col)
    return df

def corregirHoras(df):
    td = pd.Timedelta(-3, "h")
    td1 = pd.Timedelta(-0.01, "h")
    df["created_date"]=df["created_date"]+td
    df['order_on_kitchen_ready_time']=df['order_on_kitchen_ready_time']+td
    df['dboy_sent_date']=df['dboy_sent_date']+td
    df['withdrawn_date']=df['withdrawn_date']+td
    try:
        df['shop_on_hold_time']=df['shop_on_hold_time']+td
    except KeyError:
        df['shop_on_hold_time']=df['withdrawn_date']+td1
    if ('consumer_on_hold_time' in df):
        df['consumer_on_hold_time']=df['consumer_on_hold_time']+td
    df['confirmed_date']=df['confirmed_date']+td
    return df

def editOrders(df):
    '''redondea,corrige zona horaria y crea nuevos campos útiles'''
    
        
    #redondeo
    df['delay_start_time'] = df['delay_start_time'].astype(float).round()
    df['delay_end_time'] = df['delay_end_time'].astype(float).round()
    df['start_kitchen_delay'] = df['start_kitchen_delay'].astype(float).round()
    df['withdrawn_delay'] = df['withdrawn_delay'].astype(float).round()
    df['confirmed_delay'] = df['confirmed_delay'].astype(float).round()
    df['kitchen_ready_minutes'] = df['kitchen_ready_minutes'].astype(float).round()
    df['sent_minutes'] = df['sent_minutes'].astype(float).round()
    df['waiting_minutes_shop'] = df['waiting_minutes_shop'].astype(float).round()
    df['withdrawn_minutes'] = df['withdrawn_minutes'].astype(float).round()
    df['waiting_minutes_consumer'] = df['waiting_minutes_consumer'].astype(float).round()
    df['final_deliver_time'] = df['final_deliver_time'].astype(float).round()

        
    return df

def createReportFields(df1):

    if ('consumer_canceled_date' not in df1):
        df1['consumer_canceled_date']=None

    df1['created_time'] = pd.to_datetime(df1['created_date']).dt.time
    df1['hour'] = pd.to_datetime(df1['created_date']).dt.hour
    df1['weekday']=df1['created_date'].dt.day_name()
    df1['assigned_minutes']=df1['sent_minutes']-df1['kitchen_ready_minutes']
    df1['real_withdrawn'] = df1['withdrawn_minutes']-df1['waiting_minutes_shop']
    df1['real_end'] = df1['final_deliver_time']+df1['kitchen_ready_minutes']+df1['waiting_minutes_shop']
    #df1['real_end'] = df1['real_end'].round()
    
    df1['fecha'] = pd.to_datetime(df1['created_date']).dt.date
    #df1['Fecha'] = df1['Fecha'].astype("datetime64")
    df1['num_pedido'] = df1['order_number']
    df1['demora_cocina'] = df1['start_kitchen_delay'].round()
    df1['deli_i'] = df1['delay_start_time'].round()
    df1['deli_f'] = df1['delay_end_time'].round()
    df1['creado']  = pd.to_datetime(df1['created_date']).dt.time
    df1['creado'] =df1['creado'].apply(lambda x: x.replace(microsecond=0))
    df1['creado'] = df1['creado'].map(lambda x: str(x)[:-3])
    #df1['creado'] = df1['creado'].astype("datetime64")
    df1['listo'] = pd.to_datetime(df1['order_on_kitchen_ready_time']).dt.time
    df1['listo'] =df1['listo'].apply(lambda x: x.replace(microsecond=0))
    df1['listo'] = df1['listo'].map(lambda x: str(x)[:-3])
   # df1['listo'] = df1['listo'].astype("datetime64")
    df1['asignado'] = pd.to_datetime(df1['dboy_sent_date']).dt.time
    df1['asignado'] =df1['asignado'].apply(lambda x: x.replace(microsecond=0))
    df1['asignado'] = df1['asignado'].map(lambda x: str(x)[:-3])
    #df1['asignado'] = df1['asignado'].astype("datetime64")
    df1['espera_comercio'] = df1['waiting_minutes_shop'].round()
    df1['retirado'] = pd.to_datetime(df1['withdrawn_date']).dt.time
    df1['retirado'] =df1['retirado'].apply(lambda x: x.replace(microsecond=0))
    df1['retirado'] = df1['retirado'].map(lambda x: str(x)[:-3])
    #df1['retirado'] = df1['retirado'].astype("datetime64")
    df1['espera_consumidor'] = df1['waiting_minutes_consumer'].round()
    df1['entregado'] = pd.to_datetime(df1['confirmed_date']).dt.time
    df1['entregado'] =df1['entregado'].apply(lambda x: x.replace(microsecond=0))
    df1['entregado'] = df1['entregado'].map(lambda x: str(x)[:-3])
    #df1['entregado'] = df1['entregado'].astype("datetime64")
    df1['demora_de_retiro'] = (df1['real_withdrawn']-df1['kitchen_ready_minutes']).round() 
    df1['fail_retirado']=df1['real_withdrawn']>(df1['delay_start_time']+5)
    df1['fail_entrega']=df1['real_end']>(df1['delay_end_time']+5)
    
    df1['fail_propio']=((df1['real_end']>(df1['delay_end_time']+5)) & 
                        (df1['kitchen_ready_minutes']<df1['start_kitchen_delay']+5)
           &(df1['waiting_minutes_shop']<=5)& ((~ df1['creado'].str.contains('10:')))&
            ((~df1['creado'].str.contains('09:'))))
    
    df1['fail_shop']=((df1['real_end']>(df1['deli_f']+5)) &
                      (df1['kitchen_ready_minutes']>df1['start_kitchen_delay']+5)
           &(df1['waiting_minutes_shop']>5)& ((~ df1['creado'].str.contains('10:')))&
            ((~df1['creado'].str.contains('09:'))))
    
    df1['tiempo_listo']=df1['kitchen_ready_minutes'].round()
    df1['tiempo_final']=df1['real_end'].round()
    df1['asignado_a_retirado']= df1['withdrawn_minutes']-df1['sent_minutes']
    df1['listo_a_asignado']= df1['assigned_minutes']
    df1['retirado_a_entregado']=df1['final_deliver_time'] -(df1['withdrawn_minutes'] -df1['waiting_minutes_shop']
                                                           - df1['kitchen_ready_minutes'])
    
    
    df1['entre_listo_y_retirado'] = df1['withdrawn_minutes']-df1['waiting_minutes_shop']-df1['kitchen_ready_minutes']
    df1['diff']=df1['real_end']-df1['delay_end_time']   
    df1['quali'] = df1['diff'].apply(getCategory)
    
    return df1

def groupByShift(df,*args):
    #Agrupa por turnos (y promedia) los campos pasados como parametros###
    dic={}
    for n in args:
        dic[n]='mean'
    df = df.groupby(['created_date','shift']).agg(dic)
    return df
def groupByHour(df):
    #Agrupa por hora (y suma) la cantidad de pedidos###
    df = df.groupby('hour')['created_date'].count()
   
    return df

def getShop(df,shop):
    df1=df.loc[df['shop_name'] == shop]
    return df1
#dfb=getShop(df,'borziburguers')

def getHour(df,time):
    df1=df.loc[df['created_time'] == time]
    return df1
def getShift(df,shift):
    if shift=='Todos':
        print(shift[0])
        return df
    turno=shift[0]
    print(turno)
    df1=df.loc[df['shift'] == turno]
    return df1

def getCity(df,city):
    df1=df.loc[df['city'] == city]
    return df1
def getReport(df):
    df1=df[['fecha','num_pedido','shop_name','deli_i','deli_f','tiempo_final','entre_listo_y_retirado',
            'demora_cocina','creado','listo','asignado','espera_comercio',
            'retirado','espera_consumidor','entregado','notes','shift','consumer_canceled_date','created_date']]
    return df1

def getWeek(df,date):
    td = pd.Timedelta(7, "d")
    date_time_str = date
    date_time_obj = datetime.strptime(date_time_str, '%Y-%m-%d')
    date_time_object = date_time_obj.date()
    mask = (df['fecha'] < date_time_object) & (df['fecha']>= (date_time_object - td))
    df1 = pd.DataFrame(df[mask])
    return df1
def getDateInterval(df,date1,date2):
    date_time_str = date1
    date_time_obj = datetime.strptime(date_time_str, '%Y-%m-%d')
    date_time_object = date_time_obj.date()
    
    date_time_str2 = date2
    date_time_obj2 = datetime.strptime(date_time_str2, '%Y-%m-%d')
    date_time_object2 = date_time_obj2.date()
    mask =  ((pd.to_datetime(df['created_date']).dt.date) >= (date_time_object)) & ((pd.to_datetime(df['created_date']).dt.date) < date_time_object2)
    df1 = pd.DataFrame(df[mask])
    return df1

def getDate(df,date):
    date_time_str = date
    date_time_obj = datetime.strptime(date_time_str, '%Y-%m-%d')
    date_time_object = date_time_obj.date()
    mask = df['fecha'] == date_time_object
    df1 = pd.DataFrame(df[mask])
    return df1

def getFailsReport(df):
    '''retorna dataframe con TODOS los pedidos entrgados dspues de la hora que dió el sistema inicialmente'''
    df1=getReport(df)
    mask = ((df1['tiempo_final']>(df1['deli_f']+5)) & (df1['tiempo_listo']<df1['demora_cocina']+5)
           &(df1['espera_comercio']<=5)& ((~ df1['creado'].str.contains('10:')))&
            ((~df1['creado'].str.contains('09:'))))
    df2 = pd.DataFrame(df1[mask])
    return df2  

def getCategory(n):
    cat = ''
    if   (n>(-5) and n<5) : cat = 'justo'
    elif (n>=5 and n<15) : cat = '+5 +15'
    elif (n>=15 and n<30) : cat = '+15 +30'
    elif (n>=30 and n<45) : cat = '+30 +45'
    elif (n>=45) : cat = '+45'
    elif (n>=(-15) and n<=(-5)) : cat = '-15 -5'
    elif (n>=(-30) and n<(-15)) : cat = '-30 -15'  
    elif (n>(-45) and n<(-30)) : cat = '-45 -30'    
    elif (n<=(-45)): cat = '-45'
    return cat      

def getMoreThan60(df):
    
    mask = df['tiempo_final'] > 60
    df1 = pd.DataFrame(df[mask])
    return df1      