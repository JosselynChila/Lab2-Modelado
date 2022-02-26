from datetime import date
from datetime import timedelta
import pandas as pd
import sqlalchemy

def extraer_database(path):

    motorDB = sqlalchemy.create_engine(path)
    conectarDB = motorDB.connect()

    return motorDB, conectarDB


def dfFecha():
    desde = date(2005, 1, 5)
    hasta = date(2020, 1, 25)
    rango = []
  
    dias_totales = (hasta - desde).days
    for days in range(dias_totales + 1): 
        fecha = desde + timedelta(days=days)
        rango.append(fecha)
    
    df_fecha = pd.DataFrame(rango,columns=['Rango de fechas'])

    return df_fecha


def fechas():
    date = pd.date_range('2005-01-01','2020-12-31',freq='D').to_series()
    fecha = pd.to_datetime(date) 
    day = fecha.dt.day
    month = fecha.dt.month
    year = fecha.dt.year
    quarter = fecha.dt.quarter
    week = fecha.dt.week
    weekday = fecha.dt.weekday
    day_name = fecha.dt.day_name()
    month_name = fecha.dt.month_name()
    
    df = pd.DataFrame({'Date':list(fecha),'Day':list(day),'Month':list(month),'Year':list(year),'Quarter':list(quarter),'Week':list(week),'Weekday':list(weekday),'NameDay':list(day_name),'NameMonth':list(month_name)})
    return df

def cargar_a_sql(datos, connectar, tabla_sqlite):
    
    # Procesamiento de completar los valores faltantes
    datos.to_sql(tabla_sqlite, connectar, if_exists='append', index=False)
    connectar.close()
    termino = print("carga terminada")
    return termino

if __name__ == '__main__':
    path = "sqlite:///DW_Sale_Music.db"

    # Extracción
    extraer = fechas()

    # carga de los datos
    extraerNueva = extraer_database(path)
    datos = extraer
    conectarNuevo = extraerNueva[1]
    tabla_sqlite = "dim_time"
    cargar_a_sql(datos, conectarNuevo, tabla_sqlite)
    print(extraer)