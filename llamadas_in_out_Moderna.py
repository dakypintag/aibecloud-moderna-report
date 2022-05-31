import pandas as pd
import numpy as np
import chardet

def run():
    df = pd.read_csv('LLAMADAS_IN_OUT_MA.report-ABRIL.csv', sep=';', encoding = 'ISO-8859-1')
    # FECHA-CORTA
    df['FECHA'] = pd.to_datetime(df['AÑO-MES-DÍA DE  LL.FECHA'], format = '%Y-%m-%d')
    df['FECHA-CORTA'] = df['FECHA'].dt.strftime('%d/%m/%Y')
    # print(df['FECHA-CORTA']) 

    # DIA
    df['DIA'] = df['FECHA'].dt.day
    # print(df['DIA'])

    # HORA
    df['HORA'] = pd.to_datetime(df['HORA1'], format = '%H:%M:%S').dt.hour
    # print(df['HORA']) 

    # MINUTO
    df['MINUTO'] = pd.to_datetime(df['HORA1'], format = '%H:%M:%S').dt.minute
    #print(df['MINUTO']) 

    # DURACION-SEG
    df['DURACION'] = df['DURACION'].str.replace(',','.')
    df['DURACION'] = df['DURACION'].astype(float)*60
    df['DURACION-SEG'] =  df['DURACION'].round(2)
    # print(df['DURACION-SEG'].head(5))

    # COLA
    conditions2 = [
        (df['VDN'] == 1020) & (df['DDI'] != 5454),
        (df['VDN'] == 1030),
        (df['VDN'] == 1040),
        (df['VDN'] != '')
    ]

    results2 = ['200-300','YAYAYA','CAYAMBE','']
    df['COLA'] = np.select(conditions2, results2)
    # print(df['COLA'])

    # DIRECCION
    conditions1 = [
        (df['DDI'] == 5454),
        (df['DDI'] != 5454)
    ]
    results1 = ['OUTBOUND','INBOUND']
    df['DIRECCION'] = np.select(conditions1, results1)
    #print(df['DIRECCION'].head(25))

    # STATUS
    conditions = [
        (df['WHYCALL'] == 'UT'),    # 1
        (df['WHYCALL'] == 'CI'),    # 2
        (df['WHYCALL'] == 'CO'),    # 3
        (df['WHYCALL'] == 'NE'),    # 4
        (df['WHYCALL'] == 'NL'),    # 5
        (df['WHYCALL'] == 'NC'),    # 6
        (df['WHYCALL'] == 'CA'),    # 7
        (df['WHYCALL'] == 'OT') & (df['DURACION-SEG'] > 32),    # 8
        (df['WHYCALL'] == 'SC'),    # 9
        (df['WHYCALL'] == 'NT'),    # 10
        (df['WHYCALL'] == 'TE'),    # 11
        (df['WHYCALL'] == 'XX')    # 12
    ]
    results = [
        'EFECTIVA',    # 1
        'EFECTIVA',    # 2
        'EFECTIVA',    # 3
        'EFECTIVA',    # 4
        'NO EFECTIVA',    # 5
        'NO EFECTIVA',    # 6
        'NO EFECTIVA',    # 7
        'EFECTIVA',    # 8
        'NO EFECTIVA',    # 9
        'NO EFECTIVA',    # 10
        'NO EFECTIVA',    # 11
        'EFECTIVA'    # 12
    ]
    df['STATUS'] = np.select(conditions,results)

    # PREFIJO
    conditions3 = [
        (df['DDI'] == 5454),
        (df['DDI'] != 5454)
    ]
    results3 = ['33','']
    df['PREFIJO'] = np.select(conditions3, results3)

    dfIn = df[df.DIRECCION == 'INBOUND']
    dfIn['DIA-NOMBRE'] = df['FECHA'].dt.day_name()
    # print(dfIn['DIA-NOMBRE'].tail(15))
    tablaInHoraDia = pd.pivot_table(data = dfIn, index=['HORA'], values= 'DIRECCION', columns =['DIA','DIA-NOMBRE'], aggfunc= 'count', margins= True, margins_name= 'Por dia')
    print(tablaInHoraDia)

    # print(dfIn.head(30))

    df2 = df
    # FECHA = FECHA-CORTA
    df2['FECHA'] = df['FECHA-CORTA']
    # # CALLID = CODUNICO
    df2['CALLID'] = df['CODUNICO']
    # TIPO = DIRECCION
    df2['TIPO'] = df['DIRECCION']
    # NUMERO = TELEFONO
    df2['NUMERO'] = df['TELEFONO']

    df2.to_excel('REPORTE_LLAMADAS_IN_OUT_AIBECLOUD_git.xlsx', index = False) # index = False  -> allow skip index column
    
    # LLAMADA-POR-MARCA = COLA . Cambiar MODERNA_200 por 200-300
    df2['LLAMADA-POR-MARCA'] = df['COLA']
    # # CODIGO = (vacio)  . Columna con datos vacios
    df2['CODIGO'] = ''
    # # DETALLE = CLASIFICACION   . PARA REGISTROS DE AIBECLOUD
    df2['DETALLE'] = df['CLASIFICACION']
    # # DETALLE = DETALLE   . PARA REGISTROS DE REPORTE DE LLAMADAS CELULAR
    conditions5 = [
        (df2['CLASIFICACION'] == 'CLIENTE'),
        (df2['CLASIFICACION'] == 'CONSUMIDOR'),
        (df2['CLASIFICACION'] == 'CANDIDATO'),
        (df2['CLASIFICACION'] == 'INFORMACION'),
        (df2['CLASIFICACION'] != 'SIN AUDIO') & (df2['DURACION-SEG']<13),  # & (df2['ESTADO_CASO'] == '') & (df2['DDI'] == '')
        (df2['CLASIFICACION'] == 'SIN AUDIO'),
        df2['CLASIFICACION'].isnull()
    ]
    results5 = [df2['ASUNTOCLIENTE'],df2['ASUNTO'],df2['ASUNTOCANDIDATO'],df2['OBSERVACIONES'],'NO HABLA',df2['CLASIFICACION'], '']
    df2['DETALLE'] = np.select(conditions5, results5)

    # INTERACCION = DEVOLUCION . Condicion: si llamada es OUTBOUND, INTERACCION = DEVOLUCION
    conditions4 = [
        (df2['DDI'] == 5454),
        (df2['TIPO'] == 'INBOUND')  & ((df2['CLASIFICACION'] == 'CLIENTE') | (df2['CLASIFICACION'] == 'CONSUMIDOR') | (df2['CLASIFICACION'] == 'CANDIDATO')), # & (df2['CLASIFICACION'] != 'INFORMACION')
        (df2['TIPO'] == 'INBOUND') & (df2['CLASIFICACION'] == 'INFORMACION'),
        (df2['TIPO'] == 'INBOUND') & (df2['CLASIFICACION'] == 'SIN AUDIO') ,
        (df2['TIPO'] == 'INBOUND') & (df2['DETALLE'] == 'NO HABLA'), # & (df2['DURACION-SEG'] > 50)
        (df2['TIPO'] == 'INBOUND') & (df2['CLASIFICACION'].isnull())
        # (df2['DDI'] != 5454)
    ]

    results4 = ['DEVOLUCION','INGRESO','INFORMACION','INFORMACION','INFORMACION','INFORMACION']
    df2['INTERACCION'] = np.select(conditions4, results4)

    # # CONTACTO = (vacio) . PARA REGISTROS DE AIBECLOUD
    df2['CONTACTO'] = ''
    # # CONTACTO = CONTACTO . PARA REGISTROS DE REPORTE DE LLAMADAS CELULAR
    # # STATUS = STATUS   . PARA REGISTROS DE AIBECLOUD
    # # STATUS = STATUS   . PARA REGISTROS DE REPORTE DE LLAMADAS CELULAR
    # # INTERACCION-POR = ORIGEN

    df2['INTERACCION-POR'] = df['ORIGEN'] 
    df3=df2[['FECHA','CALLID','TIPO','NUMERO','INTERACCION', 'LLAMADA-POR-MARCA','CODIGO','DETALLE','CLASIFICACION','DURACION-SEG','CONTACTO','STATUS','INTERACCION-POR']]
    # print(df3.head(10))
    b1 = ['7YZPLOVQ2','27NIL7EK3C','M2FX81KHYO','7X4HM7GBP','7X4HM7GBP','27HSDEHGTL','M2Z55FHQXM','27I1EXS26G','27I1GKA64F','7X7SK3OMZ','M31P6Z9HNX']
    df4 = df3[df3['CALLID'].isin(b1)]
    # print(df4.sort_values(by=['DURACION-SEG']))
    
    # print(df2[['FECHA','CALLID','TIPO','NUMERO','INTERACCION', 'LLAMADA-POR-MARCA','CODIGO','DETALLE','CLASIFICACION','CONTACTO','STATUS','INTERACCION-POR']].head(20))
    df2[['FECHA','CALLID','TIPO','NUMERO','INTERACCION', 'LLAMADA-POR-MARCA','CODIGO','DETALLE','CONTACTO','STATUS','INTERACCION-POR']].to_excel('tabla-interacciones4.xlsx', index = False)



    # df_final = df[['FECHA-CORTA','DIA','HORA','MINUTO','DURACION-SEG','COLA','DIRECCION','STATUS','PREFIJO']]
    # df_final.to_excel('aibe.xlsx', index=False)
    # print(df[['FECHA-CORTA','DIA','HORA','MINUTO','DURACION-SEG','COLA','DIRECCION','STATUS','PREFIJO']][12:35])
    # df.to_excel('REPORTE_LLAMADAS_IN_OUT_AIBECLOUD_git.xlsx', index = False) # index = False  -> allow skip index column
if __name__ == '__main__':
    run()


    # df['STATUS'] = ['EFECTIVA' if x == 'UT' else '' for x in df['WHYCALL']]
    # df['STATUS'] = ['EFECTIVA' if x == 'CI' else '' for x in df['WHYCALL']]    # df['STATUS'] = ['EFECTIVA' if x == 'CO' else '' for x in df['WHYCALL']]
    # df['STATUS'] = ['EFECTIVA' if x == 'NE' else '' for x in df['WHYCALL']]
    # df['STATUS'] = ['NO EFECTIVA' if x == 'NL' else '' for x in df['WHYCALL']]
    # df['STATUS'] = ['NO EFECTIVA' if x == 'NC' else '' for x in df['WHYCALL']]
    # df['STATUS'] = ['NO EFECTIVA' if x == 'CA' else '' for x in df['WHYCALL']]
    # df['STATUS'] = ['NO EFECTIVA' if x == 'OT' else '' for x in df['WHYCALL']]
    # df['STATUS'] = ['NO EFECTIVA' if x == 'SC' else '' for x in df['WHYCALL']]
    # df['STATUS'] = ['NO EFECTIVA' if x == 'NT' else '' for x in df['WHYCALL']]
    # df['STATUS'] = ['NO EFECTIVA' if x == 'TE' else '' for x in df['WHYCALL']]