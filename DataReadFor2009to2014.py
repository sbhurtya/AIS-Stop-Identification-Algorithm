#This script reads gdb file of AIS downloaded from Marine Cadastre website..
# @author: Sanjeev Bhurtyal

# %%Libraries
import sys
import pandas as pd
import numpy as np
import io
import os
from sqlalchemy import create_engine
import psycopg2
import geopandas as gpd
import datetime
import csv
import pandas as pd
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


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


# %%
# Data from 2009 to 2014 have Zones and are in .gdb format
fl = "./FromMarineCadastre/"  # AIS Data Folder Location(fl)
yr=sys.argv[1] #This reads argument from the terminal. Helpful while running the code for multiple years in HPC.
if yr == '9':
    yr = str(0) + str(9)
yr = str(20) + str(yr)
print('Year: ', yr)
yr = int(yr)
mo = os.listdir(fl+str(yr)) #List of folders within year folder
for mon in mo:
    df_mo = pd.DataFrame()  # Monthly DataFrame
    pth = fl+str(yr)+'/'+mon    #Month Path
    zone_file_name_zip = [] # list to store zip files name
    for file in os.listdir(pth):
        if file.endswith('.zip'):
            zone_file_name_zip.append(file)
    zone_file_name = [x.split('.')[0] for x in zone_file_name_zip]
    for zfn in zone_file_name:
        # Read Layers
        if zfn != 'Zone2_2010_06': #Zone 2 for 2010 June is empty
            print('**********************************'+zfn+'**********************************', flush=True)
            if yr in [2009,2010,2014]:
                if zfn == 'Zone18_2009_05': #Some Files have different name
                    fpth = pth+'/'+zfn+'.zip'+'!'+'May_Zone_18'+'.gdb' #File Path
                elif zfn == 'Zone7_2010_11':
                    fpth = pth+'/'+zfn+'.zip'+'!'+'ZOne7_2010_11'+'.gdb' #File Path
                else:
                    fpth = pth + '/' + zfn + '.zip' + '!' + zfn + '.gdb'  # File Path
            else:
                fpth = pth + '/' + zfn + '.gdb.zip' + '!' + zfn + '.gdb'  # File Path
            print(datetime.datetime.now(), flush=True)

            if yr in [2009,2010,2011,2012]:
                broadcast = gpd.read_file(fpth, driver = 'FileGDB', layer='Broadcast', ignore_fields = ['ROT', 'Status', 'ReceiverType', 'ReceiverID'])
                voyage = gpd.read_file(fpth, driver = 'FileGDB', layer='Voyage')
                vessel = gpd.read_file(fpth, driver = 'FileGDB', layer='Vessel')
            else:
                broadcast = gpd.read_file(fpth, driver='FileGDB', layer=zfn+'_Broadcast', ignore_fields=['ROT', 'Status', 'ReceiverType', 'ReceiverID'])
                voyage = gpd.read_file(fpth, driver='FileGDB', layer=zfn+'_Voyage')
                vessel = gpd.read_file(fpth, driver='FileGDB', layer=zfn+'_Vessel')

            # Clean Data
            # slt = 0  # Speed less than threshold (Remove records with speed less than or equal to this threshold)
            irvt = [0, 36, 37, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 1012, 1013, 1014, 1015,
                    1019]  # Irrelevant Vessel Type

            # broadcast = broadcast[broadcast['SOG'] > slt]
            broadcast = broadcast.dropna(subset=['MMSI'])


            voyage = voyage[['VoyageID', 'Cargo', 'Draught']]
            vessel = vessel[['MMSI', 'VesselType','Length','Width']]
            vessel = vessel[~vessel.VesselType.isin(irvt)]
            vessel = vessel.dropna(subset=['VesselType'])

            broadcast = pd.merge(broadcast, voyage, on=['VoyageID'], how='left')
            broadcast = broadcast.drop(['VoyageID'], axis=1)
            broadcast = broadcast.rename(columns = {'Draught':'Draft'})
            broadcast['Draft'] = broadcast['Draft']/10

            broadcast = pd.merge(broadcast, vessel, on=['MMSI'], how='inner')

            broadcast = broadcast.drop_duplicates(keep='first')  # Remove duplicates

            broadcast = broadcast.set_index('BaseDateTime').groupby('MMSI').resample("5T").first()
            broadcast = broadcast.dropna(subset=['MMSI'])
            broadcast = broadcast.drop('MMSI', axis=1)
            broadcast = broadcast.reset_index()

            if 'BaseDateTime' not in broadcast:
                broadcast['BaseDateTime'] = 0
                broadcast["BaseDateTime"] = pd.to_datetime(broadcast['BaseDateTime'])

            broadcast['LON'] = broadcast['geometry'].x
            broadcast['LAT'] = broadcast['geometry'].y
            broadcast = broadcast.drop(['geometry'], axis=1)
            broadcast['VesselType'] = broadcast['VesselType'].astype(int)
            broadcast = broadcast[broadcast['BaseDateTime'].dt.strftime('%Y') == str(yr)]
            broadcast = broadcast.reindex(columns=['MMSI', 'BaseDateTime', 'LAT', 'LON', 'SOG','COG','Heading', 'VesselType', 'Length', 'Width', 'Draft','Cargo'])  # To make sure that dataframe are in same order.

            broadcast.head()
            broadcast.dtypes

            df_mo = pd.concat([broadcast, df_mo], ignore_index=True)

    mo = mon[:2]
    print('Transferring data for year: ', yr, 'month: ', mo, flush=True)
    engine = create_engine('postgresql://username:password@server/database') #Replace username, password, server, and database
    conn = engine.raw_connection()
    cur = conn.cursor()

    table_name = 'ais_' + str(yr) + '_' + str(mo)  # InsertTableNameHere
    drop_table = '''drop table if exists wcs.''' + table_name
    cur.execute(drop_table)
    conn.commit()

    df_mo.to_sql(table_name, engine, method=psql_insert_copy, index=False, schema='wcs', chunksize=20000)

