# This script reads gdb file of AIS downloaded from Marine Cadastre website. Preprocesses data and exports to database server.
# @author: Sanjeev Bhurtyal


# %%Libraries
import pandas as pd
import sys
import numpy as np
import os
import io
import csv
import psycopg2
from sqlalchemy import create_engine
import calendar
import zipfile

#%% Functions to move cleaned data to database
def psql_insert_copy(table, conn, keys, data_iter):
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = io.StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)

#%% For 2015 to 2021
fl = "./FromMarineCadastre/"  # AIS Data Folder Location(fl)
yr=sys.argv[1]
yr = int(yr)
yr = str(20) + str(yr)
yr = int(yr)
print('Year: ', yr)
for mo in (['0'+str(i) for i in range(1, 10)] + [str(i) for i in range(10,13)]): #+ [str(i) for i in range(10,13)]
    df_mo=pd.DataFrame() #Monthly DataFrame
    days = (calendar.monthcalendar(yr, int(mo)))
    days = [item for sublist in days for item in sublist]
    days = [i for i in days if i != 0]
    days = ['0'+str(i) for i in range(1,10)]+[str(i) for i in range(10,days[-1]+1)]
    for dy in days:
        pth = fl+str(yr)+'/'+'AIS_'+str(yr)+'_'+mo+'_'+str(dy)+'.zip'
        file_name = 'AIS_'+str(yr)+'_'+mo+'_'+str(dy)+'.csv'
        print('******************'+file_name+'******************', flush=True)
        zf = zipfile.ZipFile(pth)
        df = pd.read_csv(zf.open(file_name), on_bad_lines='skip')
        #Clean Data
        # slt = 0  # Speed less than threshold (Remove records with speed less than or equal to this threshold)
        irc = ['VesselName', 'IMO', 'CallSign','Status']  # Irrelevant Columns
        irvt = [0, 36, 37, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 1012, 1013, 1014, 1015, 1019] # Irrelevant Vessel Type

        # df = df[df['SOG'] > slt] # Remove records with 0 speed

        df = df.drop(columns=[col for col in df if col in irc])# # Remove irrelevant columns

        df = df[~df.VesselType.isin(irvt)]

        df = df.dropna(subset=['MMSI','VesselType']) # Remove records with null MMSI

        df = df.drop_duplicates(keep='first') # Remove duplicates

        df = df.set_index('BaseDateTime').groupby('MMSI').resample("5T").first()
        df = df.dropna(subset=['MMSI'])
        df = df.drop('MMSI', axis=1)
        df = df.reset_index()

        if 'BaseDateTime' not in df:
            df['BaseDateTime'] = 0
            df["BaseDateTime"] = pd.to_datetime(df['BaseDateTime'])

        df = df.reindex(
            columns=['MMSI', 'BaseDateTime', 'LAT', 'LON', 'SOG','COG','Heading', 'VesselType', 'Length', 'Width', 'Draft','Cargo']) #To make sure that dataframe are in same order.
        df_mo = pd.concat([df, df_mo], ignore_index=True)
    df_mo["VesselType"] = df_mo["VesselType"].astype(int)
    df_mo["MMSI"] = df_mo["MMSI"].astype(np.int64)
    df_mo["BaseDateTime"] = pd.to_datetime(df_mo['BaseDateTime'])
    df_mo.info()
    print('Transferring data for year: ', yr, 'month: ', mo , flush=True)

    engine = create_engine('postgresql://username:password@server/database') #Replace username, password, server, and database
    conn = engine.raw_connection()
    cur = conn.cursor()

    table_name = 'ais_' + str(yr) + '_' + str(mo)  # InsertTableNameHere
    drop_table = '''drop table if exists wcs.''' + table_name
    cur.execute(drop_table)
    conn.commit()


    df_mo.to_sql(table_name, engine, method=psql_insert_copy, index=False, schema='wcs', chunksize=20000)

