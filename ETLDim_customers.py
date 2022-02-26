import pandas as pd
import sqlalchemy

def extraer_DB(path):

    DB = sqlalchemy.create_engine(path)
    conectarDB = DB.connect()

    return DB, conectarDB

def extraernueva_DB(path):
    
    DBnueva = sqlalchemy.create_engine(path)
    conectarDBNueva = DBnueva.connect()

    return DBnueva, conectarDBNueva

def extraer_tabla(conectarDB):

    query = '''SELECT * FROM customers;'''
    result = conectarDB.execute(query)

    df = pd.DataFrame(result.fetchall())
    df.columns = result.keys()

    return df

def cargarSql(df, conectar, tabla):

    df.to_sql(tabla, conectar, if_exists='replace', index=False)
    conectar.close()
    msg = print("La carga se ha terminado!!")
    return msg

if __name__ == '__main__':
    path = "sqlite:///chinook.db"
    path2 = "sqlite:///DW_Sale_Music.db"

    extraerBD = extraer_DB(path)
    engine = extraerBD[0]
    extraer = extraer_tabla(engine)
    extraerNueva = extraernueva_DB(path2)
    df = extraer
    conectarNuevo = extraerNueva[1]
    tabla = "dim_customer"
    cargarSql(df, conectarNuevo, tabla)
    print(extraer)