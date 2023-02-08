from datetime import date, datetime
import json
import dash
import dash_auth
import dash_bootstrap_components as dbc

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dash_table, dcc, html
from dash.dependencies import Input, Output, State
from dateutil import parser
from pymongo import *

from funciones2 import *
from funciones2 import getOrdersV2, getShops







VALID_USERNAME_PASSWORD_PAIRS = {
    'user': 'pass'
   
}


td = pd.Timedelta(-1, "d")
fecha_final=date.today()
fecha_inicial= date.today()+td



dff= getShops()
options= dff[0].tolist()
options.append('todos')
options=np.sort(options)

#DF vacío para enviar a la tabla en caso de elegir un local q no tiró pedidos

dataCero = [['Narnia pa',0,0,0,0,0,0,0,0,0,0,0,0,0,'-','-',0,0,0]]
dfCero = pd.DataFrame(dataCero, columns=['fecha', 'num_pedido','deli_i','deli_f','tiempo_final','entre_listo_y_retirado','demora_cocina','creado','listo','asignado','espera_comercio','retirado','espera_consumidor','entregado','notes','shift','consumer_canceled_date','created_date','demora_total'])


#Define cards
card1 = dbc.Card([
    dbc.CardBody([
        html.H2("Entregados", className="card-text",id="card_text1"),
        html.H3("Card title", className="card-title",id="card_num_1"),
        html.H5("Card title", className="card-title",id="card_num_1_porcentaje")
        
    ])
],
    style={'display': 'inline-block',
           'width': '20%',
           'text-align': 'center',
           'color':'white',
           'background-color': 'rgba(37, 150, 190)'},
    outline=True)

card2 = dbc.Card([
    dbc.CardBody([
        html.H2("Cancelados", className="card-text",id="card_text2"),
        html.H3("Card title", className="card-title",id="card_num_2"),
        html.H5("Card title", className="card-text",id="card_num_2_porcentaje")
        
        ]
     )],
    style={'display': 'inline-block',
           'width': '20%',
           'text-align': 'center',
           'color':'white',
           'background-color': 'rgba(37, 150, 190)'},
    outline=True)

card3 = dbc.Card([
    dbc.CardBody([
        html.H2("Tiempo promedio", className="card-text",id="card_text3"),
        html.H3("Card title", className="card-title",id="card_num_3"),
        html.H5("mins", className="card-title")
        
        ]
     )],
    style={'display': 'inline-block',
           'width': '20%',
           'text-align': 'center',
           'color':'white',
           'background-color': 'rgba(37, 150, 190)'},
    outline=True)

card4 = dbc.Card([
    dbc.CardBody([
        html.H2("Mas de 60 mins", className="card-text",id="card_text4"),
        html.H3("Card title", className="card-title",id="card_num_4"),
        html.H5("Card title", className="card-title",id="card_num_4_porcentaje")
        
        ]
     )],
    style={'display': 'inline-block',
           'width': '20%',
           'text-align': 'center',
           'color':'white',
           'background-color': 'rgba(37, 150, 190)'},
    outline=True)

card5 = dbc.Card([
    dbc.CardBody([
        html.H2("Demorados cocina", className="card-text",id="card_text5"),
        html.H3("Card title", className="card-title",id="card_num_5"),
        html.H5("Card title", className="card-title",id="card_num_5_minutos")
        
        ]
     )],
    style={'display': 'inline-block',
           'width': '20%',
           'text-align': 'center',
           'color':'white',
           'background-color': 'rgba(37, 150, 190)'},
    outline=True)


app = dash.Dash(__name__,external_stylesheets=[dbc.themes.BOOTSTRAP])
auth = dash_auth.BasicAuth(
    app,
    VALID_USERNAME_PASSWORD_PAIRS
)

server = app.server

app.layout = html.Div([
    
    html.Div([
        html.Div([
            html.Div([
            html.Label('Cliente'), 
            dcc.Dropdown(options,'todos',id='client-selection')
        ],className="mini")
        ]),
        html.Div([
            html.Label('Fecha'),   
            dcc.DatePickerRange(
                id='date-picker-range',
                start_date = fecha_inicial,
                display_format='Y-MM-DD',
                end_date= fecha_final, )
        ],className="fecha")
    ],className="filtros"),
    html.Div([
        html.Div([
            card1,card2,card3,card4,card5],className="card-container"),
        #html.Button("descargar CSV", id="btn_csv"),
        #dcc.Download(id="download-dataframe-csv"),    
        html.Div([
            dash_table.DataTable(
                id = 'update-table', 
                style_cell={'textAlign': 'center',
                            'backgroundColor':'#242424',
                            'color':'#b154c1'
                },
                style_header={
                        
                        'fontWeight': 'bold'
                },
                style_as_list_view=False,
                style_data_conditional=[{
                    'if': {'row_index': 'odd'},
                    'backgroundColor': '#3d3d3d',},
            {
                    'if': {
                    'filter_query': '{shift} = M',
                    'column_id': 'num_pedido'
                     },
                     #'backgroundColor': 'yellow',
                     'color': 'yellow'
                 },
                 {
                    'if': {
                    'filter_query': '{shift} = T',
                    'column_id': 'num_pedido'
                     },
                    #'backgroundColor': 'orange',
                    'color': 'orange'
                 },

                 {
                    'if': {
                    'filter_query': '{shift} = N',
                    'column_id': 'num_pedido'
                 },
                    #'backgroundColor': 'tomato',
                    'color': 'tomato'
                 }],
             ),      
    
        ]),
    ]),
 html.Div([dcc.Store(id='df_stored')]),], className="outer")





@app.callback([
    Output('card_num_1', 'children'),
    Output('card_num_1_porcentaje', 'children'),
    Output('card_num_2', 'children'),
    Output('card_num_2_porcentaje', 'children'),
    Output('card_num_3', 'children'),
    
    Output('card_num_4', 'children'),
    Output('card_num_4_porcentaje', 'children'),
    Output('card_num_5', 'children'),
    Output('card_num_5_minutos', 'children'),
    

    Output('update-table', 'data'),
    Output('update-table', 'columns')
    #,
    #Output('df_stored', 'data')
    ],
    Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date'),
    Input('client-selection', 'value'))
def update_output(start_date, end_date, value):

    df=getOrdersV2(start_date, end_date, value)
    
    #Chequeo que haya levantado pedidos
    if ('created_date' in df):
        df=corregirHoras(df)
        
        

        df=editOrders(df)
        
        df=createReportFields(df)
        
    

        try:
            df=getReport(df)
        except KeyError("['consumer_canceled_date'] not in index"):
            print('*'*40+'EXCEPTTNUEVO')
            df = getReport(df)
        except KeyError('consumer_canceled_date'): 
            print('*'*40+'EXCEPTT')
            df['consumer_canceled_date']=None
            df = getReport(df)
    #except KeyError('consumer_on_hold_time'): 
    #    df['consumer_on_hold_time']=None
    #    df = getReport(df)    

        entregados=len(df.index) - df.consumer_canceled_date.count()
        
        porcentaje_entregados= str(round((entregados/len(df.index))*100,1)) + '%'
        if 'consumer_canceled_date' in df:
            cancelados= df.consumer_canceled_date.count()  
        else:
            cancelados=0     
        porcentaje_cancelados = str(round((cancelados/len(df.index))*100,1)) + '%'

        t_prom=round(df.tiempo_final.mean(),1) 
        
        df60=getMoreThan60(df)
    
        menos60=len(df60.index)
        menos60_porcentaje = str(round((menos60/len(df.index))*100,1)) + '%'
        try:
            demorados_cocina=df.apply(lambda x: x['espera_comercio'] >=6, axis=1).sum()
            df['demora_total']=df[['espera_comercio']].sum(axis=1).where(df['espera_comercio'] >= 6, 0)
            minutos_a_cobrar=str(round(df['demora_total'].sum())-(demorados_cocina*5)) + ' minutos a cobrar'
        except KeyError:
            demorados_cocina=0
            minutos='0 minutos'
    


        data1 = df.to_dict('records')
        columns1=[{"name": i, "id": i} for i in df.columns]
    #options1=df.shop_name.unique()
    #options_sort=np.sort(options1)
   
    #df3=getReport(getDateInterval(dff,start_date,end_date))
    #dfstored=df.to_json(date_format='iso', orient='split')
    else:
        entregados=0
        porcentaje_entregados=0
        cancelados=0
        porcentaje_cancelados=0
        t_prom=0
        menos60=0
        menos60_porcentaje=0
        demorados_cocina=0
        minutos_a_cobrar=0
        df=dfCero
        data1 = df.to_dict('records')
        columns1=[{"name": i, "id": i} for i in df.columns]

    return (entregados,porcentaje_entregados,cancelados,porcentaje_cancelados,t_prom,menos60,
            menos60_porcentaje,demorados_cocina,minutos_a_cobrar,data1,columns1)



if __name__ == '__main__':
    app.run_server(debug=True)