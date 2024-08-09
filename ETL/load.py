from sqlalchemy import create_engine
import pandas as pd

def FactLoad(df):
    server = 'DESKTOP-9BB8IGK'
    database = 'Estate Market DWH'
    trusted_connection = 'yes'
    driver = 'ODBC Driver 17 for SQL Server'

    conn_str = f'mssql+pyodbc://{server}/{database}?trusted_connection={trusted_connection}&driver={driver}'
    engine = create_engine(conn_str)

    df.to_sql(name="fac_estate_offers_snpt", con=engine, if_exists="append", index=False)
    print('Data succesfully loaded to the database')