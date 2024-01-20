# Librarires
import numpy as np
import sys
import os
import pandas as pd
import os
import csv
import datetime
import time
import sqlalchemy as db
import psycopg2
import io
from sqlalchemy.sql import select, text
from sqlalchemy import create_engine
from math import *
import multiprocessing
from sklearn.cluster import DBSCAN
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

def dbscan(v, data_pings):
    v_df = data_pings[data_pings['mmsi'] == v]
    stop_id_list = v_df.stop_area_id.unique()
    area_df = pd.DataFrame()
    area_df_list = []
    for s in stop_id_list:
        s_v_df_backup = v_df[v_df['stop_area_id'] == s]
        s_v_df = s_v_df_backup.copy()
        coords = np.array(s_v_df[['lat', 'lon']])

        eps = 0.00001
        min_sample = 4
        db = DBSCAN(eps=eps, min_samples=min_sample, metric='haversine', algorithm='ball_tree').fit(np.radians(coords))
        labels = db.labels_

        unique_labels, counts = np.unique(labels, return_counts=True)
        max_cluster_label = unique_labels[np.argmax(counts)]
        most_concentrated_points = coords[labels == max_cluster_label]

        #If max_cluster_label is not -1, then
        if max_cluster_label != -1:
            # Convert the lat_lon_pairs list to a set for efficient filtering
            lat_lon_set = set(map(tuple, most_concentrated_points))

            # Create a boolean mask for matching rows
            mask = s_v_df.apply(lambda row: tuple(row[['lat', 'lon']]) in lat_lon_set, axis=1)

            # Filter the DataFrame based on the mask
            s_v_df = s_v_df[mask]

            #Get lat and lon from middle of the dataframe
            mid_lat = s_v_df['lat'].iloc[int(len(s_v_df)/2)]
            mid_lon = s_v_df['lon'].iloc[int(len(s_v_df)/2)]

            #Keep middle point
            s_v_df = s_v_df.groupby(['mmsi', 'stop_area_id']) \
                .agg(lat=('lat', 'last'), lon=('lon', 'last'), start_time=('basedatetime', 'min'),
                     end_time=('basedatetime', 'max'), vesseltype=('vesseltype', 'first'), draft=('draft', 'first'),
                     cargo=('cargo', 'first'), length=('length', 'first'), width=('width', 'first')).reset_index()

            s_v_df['lat'] = mid_lat
            s_v_df['lon'] = mid_lon

            #Append to area_df_list
            area_df_list.append(s_v_df)
    if len(area_df_list) > 0:
        area_df = pd.concat(area_df_list)
    return area_df

if __name__ == '__main__':
    alg_start = time.time()
    yr = sys.argv[1]
    if yr == '9':
        yr = str(0) + str(9)
    yr = str(20) + str(yr)
    print('Year: ', yr, datetime.datetime.now(), flush=True)
    yr = int(yr)
    mo_list = ['01','02','03','04','05','06','07','08','09','10','11','12']
    for mo in mo_list:

        def read_sql_inmem_uncompressed(query, db_engine):
            copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
                query=query, head="HEADER"
            )
            conn = db_engine.raw_connection()
            cur = conn.cursor()
            store = io.StringIO()
            cur.copy_expert(copy_sql, store)
            store.seek(0)
            df = pd.read_csv(store)
            return df

        table_name = 'ais_' + str(yr) + '_' + mo
        table_name = "\"wcs\"" + '.' + table_name + '_stop_pings'

        print(table_name, datetime.datetime.now(), flush=True)
        query = "select \"mmsi\", \"basedatetime\", \"lat\", \"lon\", \"sog\", \"cog\", \"heading\", \"vesseltype\", \"draft\", \"cargo\", \"length\", \"width\", \"stop_area_id\" from " + table_name
        #+ " where mmsi IN (367625810, 366982340) and extract(day from wcs.ais_2019_01_stop_pings.\"basedatetime\") IN (18)"

        print(query)
        engine = create_engine('postgresql://username:password@server/database') #Replace username, password, server, and database
        print('Reading started at ', datetime.datetime.now(), flush=True)
        df = read_sql_inmem_uncompressed(query, engine)
        print('Reading completed at ', datetime.datetime.now(), flush=True)
        print('Records read: ', len(df), flush=True)

        df['basedatetime'] = pd.to_datetime(df['basedatetime'])
        df.columns = map(str.lower, df.columns)

        stop_pings = df.copy()

        print('DBSCAN started at ', datetime.datetime.now(), flush=True)
        vessel_list = stop_pings.mmsi.unique()

        print(f"The number of CPU cores used: {num_processes}")
        with multiprocessing.Pool(processes=(multiprocessing.cpu_count() - 2)) as pool:
            result_dfs = pool.starmap(dbscan, [(v, stop_pings) for v in vessel_list])

        stop_area = pd.concat(result_dfs, ignore_index=True)

        #Exporting to database
        def psql_insert_copy(table, conn, keys, data_iter):
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


        print('Records after DBSCAN: ', len(stop_area), flush=True)
        stop_area_table_name = 'ais_' + str(yr) + '_' + str(mo) + '_stop_area'
        engine = create_engine('postgresql://username:password@server/database') #Replace username, password, server, and database
        print('Transferring started for ', stop_area_table_name, datetime.datetime.now(), flush=True)
        stop_area.to_sql(stop_area_table_name, engine, method=psql_insert_copy, index=False, schema='wcs', chunksize=20000, if_exists = 'replace')
        print('Transferring completed for ', stop_area_table_name, datetime.datetime.now(), flush=True)
        print('Transferring completed for ', yr, mo, datetime.datetime.now(), flush=True)
    print('Algorithm completed for ', yr, datetime.datetime.now(), flush=True)
