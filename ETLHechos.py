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

    query = '''SELECT invoice_items.InvoiceLineId AS InvoiceId,
                customers.CustomerId AS CustomerID,
                employees.EmployeeId AS EmployeeId,
                invoices.InvoiceId AS TimeID,
                invoices.InvoiceId AS LocationID,
                tracks.TrackId AS TrackID,
                playlists.PlaylistId AS PlaylistID,
                artists.ArtistId AS ArtistID,
                albums.AlbumId AS AlbumID,
                invoices.Total AS Total
            FROM employees 
                INNER JOIN customers  ON customers.SupportRepId = employees.EmployeeId
                INNER JOIN invoices  ON invoices.CustomerId = customers.CustomerId
                INNER JOIN invoice_items  ON invoice_items.InvoiceId = invoices.InvoiceId
                INNER JOIN tracks  ON tracks.TrackId = invoice_items.TrackId
                INNER JOIN playlist_track ON playlist_track.TrackId = tracks.TrackId
                INNER JOIN playlists ON playlists.PlaylistId = playlist_track.PlaylistId
                INNER JOIN albums ON albums.AlbumId = tracks.AlbumId
                INNER JOIN artists ON artists.ArtistId = albums.ArtistId
                GROUP BY invoice_items.InvoiceLineId'''
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
    tabla = "Fact_Sales"
    cargarSql(df, conectarNuevo, tabla)
    print(extraer)

    
   