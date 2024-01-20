# Script to identify stop pings for AIS vessels.
# The script is based on following papers:
# "Exploring AIS data for intelligent maritime routes extraction" - Yan et al. (2020)
# "Extracting ship stopping information from AIS data" - Yan et al. (2022)
# "Inland waterway network mapping of AIS data for freight transportation planning" - Asborno et al. (2022)

# Author: Sanjeev Bhurtyal
# Libraries
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
from geopy.distance import great_circle
from math import *
import multiprocessing
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


# Function to identify stop areas
def stop(ves):
    # Create a copy of the dataframe for the vessel of interest
    pings = a[a['mmsi'] == ves].copy()
    iter_stop = 1
    iter_count = 0
    while iter_stop != 0:
        start = time.time()
        # Stop Area Determination (sad)
        # Calculate average lat and lon for each stop area. Extract the start and stop time for each stop area
        sad = pings.groupby(['mmsi', 'stop_area_id'])\
            .agg(avg_lat = ('lat','mean'),avg_lon=('lon','mean'), first_time = ('basedatetime','min'),last_time=('basedatetime','max')).reset_index()

        # Shift the stop area id, average lat, average lon, and last time by 1 row to get the previous stop area id,
        # average lat, average lon, and last time
        sad[['prev_stop_area_id', 'prev_avg_lat', 'prev_avg_lon','prev_last_time']] = sad.sort_values(by=['stop_area_id'], ascending=True) \
            .groupby(['mmsi'])['stop_area_id', 'avg_lat', 'avg_lon', 'last_time'].shift(1)

        # Shift again
        sad[['prev_stop_area_id_2', 'prev_avg_lat_2', 'prev_avg_lon_2', 'prev_last_time_2']] = \
        sad.sort_values(by=['stop_area_id'], ascending=True) \
            .groupby(['mmsi'])['prev_stop_area_id', 'prev_avg_lat', 'prev_avg_lon', 'prev_last_time'].shift(1)

        # Fill the missing values with the current stop area id, average lat, average lon, and last time
        sad.prev_stop_area_id.fillna(sad.stop_area_id, inplace=True)
        sad.prev_avg_lat.fillna(sad.avg_lat, inplace=True)
        sad.prev_avg_lon.fillna(sad.avg_lon, inplace=True)
        sad.prev_last_time.fillna(sad.last_time, inplace=True)


        time_threshold = 60 # In Minutes, "Exploring AIS data for intelligent maritime routes extraction"
        distance_threshold = 2  #In Kilometers, "Exploring AIS data for intelligent maritime routes extraction"
        # Time difference between the current stop area and the previous stop area. Takes value of 1 if the
        # time difference is less than the threshold
        sad['merge_stop_area_time'] = sad.apply(lambda row: stop_time(row.first_time, row.prev_last_time, time_threshold), axis=1)
        # Distance between the current stop area and the previous stop area. Takes value of 1 if the
        # distance is less than the threshold
        sad['merge_stop_area_distance'] = sad.apply(
            lambda row: stop_distance(row.avg_lat, row.avg_lon, row.prev_avg_lat, row.prev_avg_lon, distance_threshold), axis=1)

        # Merge the current stop area with the previous stop area if the time difference and distance are less than the
        # threshold
        sad['merge_to'] = sad.apply(lambda x: x.prev_stop_area_id if (x.merge_stop_area_time == 1) & (x.merge_stop_area_distance == 1) else x.stop_area_id,axis=1)
        sad['merge_to'] = sad.apply(lambda x: x.merge_to if x.stop_area_id==x.merge_to else x.merge_to, axis=1)
        # This step might not be necessary
        sad['test'] = sad.apply(lambda x:0 if x.stop_area_id==x.merge_to else 1,axis=1)

        # If all the stop areas are merged, then the iteration stops
        if sad['stop_area_id'].equals(sad['merge_to']) == True:
            iter_stop = 0
        # If there are stop areas that are not merged, then the iteration continues
        if iter_stop !=0:
            sad = sad[['mmsi','stop_area_id','merge_to']]
            pings = pd.merge(pings, sad, left_on=['mmsi','stop_area_id'], right_on=['mmsi','stop_area_id'], how='left')
            pings['stop_area_id'] = pings.apply(lambda x: x.stop_area_id if x.stop_area_id==x.merge_to else x.merge_to,axis=1)
            pings.drop(columns=['merge_to'],inplace=True)
        iter_count = iter_count+1

    # Threshold for number of pings in a stop area
    pings_threshold = 4
    # Count number of pings in each stop area for each vessel
    pings_count = pings.groupby(["mmsi", "stop_area_id"]).agg(pings_count=('mmsi', 'count')).reset_index()
    pings = pd.merge(pings, pings_count, left_on=["mmsi", "stop_area_id"], right_on=["mmsi", "stop_area_id"], how='left')
    # Keep records where the number of pings meets the threshold
    pings = pings[pings['pings_count'] >= pings_threshold]
    pings.drop(columns=['pings_count'], inplace=True)

    # Rank the stop areas for each vessel
    pings['stop_area_id_1'] = pings.groupby(['mmsi'])['stop_area_id'].rank(method='dense', ascending=True)
    pings.drop(columns=['stop_area_id'], inplace=True)
    pings.rename(columns={'stop_area_id_1': 'stop_area_id'}, inplace=True)
    # print('Completed in Iteration Number: ', iter_count, 'for vessel: ', ves, 'in ', time.time() - start, flush=True)
    return pings


if __name__ == '__main__':
    # Define functions
    # Calculate distance between two points in kilometers
    def gc(lat1, lon1, lat2, lon2):
        return great_circle((lat1, lon1), (lat2, lon2)).kilometers

    #Checking if the speed is within the threshold
    def stop_speed(speed):
        speed_threshold = 1
        if speed <= speed_threshold:
            return 1
        else:
            return 0

    #Checking if the time difference between two pings is within the threshold
    def stop_time(t1, t2, time_threshold):
        time_diff = (t1 - t2)
        time_diff = time_diff.total_seconds()
        time_diff = int(time_diff / 60)
        if time_diff < time_threshold:
            return 1
        else:
            return 0

    #Checking if the distance between two pings is within the threshold
    def stop_distance(lat1, lon1, lat2, lon2, distance_threshold):
        distance = gc(lat1, lon1, lat2, lon2)
        if distance < distance_threshold:
            return 1
        else:
            return 0

    #If speed, time and distance threshold meets, th ping is a candidate for a stop area
    def candidate(sp, ti, di):
        if sp == 1 & ti == 1 & di == 1:
            return 1
        else:
            return 0

    #Assigning stop area id to the candidate
    def stop_area_id(can, prev_can):
        global r
        # If the ping is not a candidate, return None
        if can == 0:
            return None
        # If the ping is a candidate and the previous ping is not a candidate, assign a new stop area id
        elif (can == 1) & (prev_can == 0):
            r = r + 1
            return r
        # If the ping is a candidate and the previous ping is also a candidate, assign the same stop area id
        elif (can == 1) & (prev_can == 1):
            return r

    alg_start = time.time()
    #Year parameter is passed using command line argument
    yr = sys.argv[1]
    if yr == '9':
        yr = str(0) + str(9)
    yr = str(20) + str(yr)
    print('Year: ', yr, datetime.datetime.now(), flush=True)
    yr = int(yr)
    #List of months
    mo_list = ['01','02','03','04','05','06','07','08','09','10','11','12']
    #Iterate through each month and read the data from Postgres
    for mo in mo_list:
        table_name = 'ais_' + str(yr) + '_' + mo
        table_name = "\"wcs\"" + '.' + table_name


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
        table_name = "\"wcs\"" + '.' + table_name

        print(table_name, datetime.datetime.now(), flush=True)
        query = "select \"MMSI\", \"BaseDateTime\", \"LAT\", \"LON\", \"SOG\", \"COG\", \"Heading\", \"VesselType\", \"Draft\", \"Cargo\", \"Length\", \"Width\" from " + table_name
        # + " where extract(day from ais_2016_01.\"BaseDateTime\") IN (16)

        engine = create_engine('postgresql://username:password@server/database') #Replace username, password, server, and database
        # print('Reading started at ', datetime.datetime.now(), flush=True)
        df = read_sql_inmem_uncompressed(query, engine)
        # print('Reading completed at ', datetime.datetime.now(), flush=True)

        # Convert the data type of BaseDateTime to datetime
        df['BaseDateTime'] = pd.to_datetime(df['BaseDateTime'])
        #Convert the data type of MMSI to string
        df.columns = map(str.lower, df.columns)

        # Create a copy of the dataframe
        a = df.copy()
        # Step 1: Candidate Stop Point Identification Sort the dataframe by MMSI and BaseDateTime in ascending order
        # and create a new column with previous time, lat and lon values using shift function
        a[['prev_time', 'prev_lat', 'prev_lon']] = a.sort_values(by=['basedatetime'], ascending=True) \
            .groupby(['mmsi'])['basedatetime', 'lat', 'lon'].shift(1)

        # Fill the first row of the dataframe with the same values as the first row
        a.prev_time.fillna(a.basedatetime, inplace=True)
        a.prev_lat.fillna(a.lat, inplace=True)
        a.prev_lon.fillna(a.lon, inplace=True)

        # Thresholds for speed, time and distance
        stop_threshold = 1  # In knots, "Extracting ship stopping information from AIS data"
        time_threshold = 30  # In Minutes, "Exploring AIS data for intelligent maritime routes extraction"
        distance_threshold = 2  # Distance threshold in kilometers "Exploring AIS data for intelligent maritime routes extraction"

        # Check if pings are within the threshold for speed, time and distance. If they are, assign 1 to the new columns
        a['stop_speed'] = a.apply(lambda x: 1 if x.sog <= stop_threshold else 0, axis=1)
        a['stop_time'] = a.apply(lambda row: stop_time(row.basedatetime, row.prev_time, time_threshold), axis=1)
        a['stop_distance'] = a.apply(
            lambda row: stop_distance(row.lat, row.lon, row.prev_lat, row.prev_lon, distance_threshold), axis=1)

        # Identifying candidate stop pings. Candidate stop pings are the ones that meet the threshold for speed,
        # time and distance
        a['candidate'] = a.apply(lambda x: 1 if (x.stop_speed == 1) & (x.stop_time == 1) & (x.stop_distance == 1) else 0,
                                 axis=1)
        # Create a new column with previous candidate value using shift function
        a['prev_candidate'] = a.sort_values(by=['basedatetime'], ascending=True) \
            .groupby(['mmsi'])['candidate'].shift(1)
        # Fill the first row of the prev_candidate with the same values as the candidate column
        a.prev_candidate.fillna(a.candidate, inplace=True)

        # Assigning candidate stop area ID
        r = 0  # Start stop area id from 0
        # Sort by MMSI and BaseDateTime in ascending order
        a = a.sort_values(['mmsi', 'basedatetime'], ascending=[True, True])
        # Create a new column with stop area id using the stop_area_id function
        a['stop_area_id'] = a.apply(lambda x: stop_area_id(x.candidate, x.prev_candidate), axis=1)
        # Drop the rows with null values in stop_area_id column. This remove non-candidate stop pings
        a = a.dropna(subset=['stop_area_id'])
        # Calculate the rank of 'stop_area_id' within each 'mmsi'
        a['stop_area_id'] = a.groupby(['mmsi'])['stop_area_id'].rank(method='dense', ascending=True)

        # Irrelevant columns
        irc = ['prev_time', 'prev_lat', 'prev_lon', 'stop_speed', 'stop_time', 'stop_distance', 'candidate',
               'prev_candidate']

        # Drop the irrelevant columns
        a.drop(columns=irc, inplace=True)

        # Step 2: True Stop Area Determination
        # List of unique MMSI
        vessel_list = a.mmsi.unique()
        print('Stop Identification started at ', datetime.datetime.now(), flush=True)

        # Using multiprocessing to process all the vessels at the same time
        pool = multiprocessing.Pool()
        pool = multiprocessing.Pool(processes=(multiprocessing.cpu_count() - 1))
        # Pass vessel_list to the stop function
        results = pool.map(stop, vessel_list)
        pool.close()
        pool.join()

        # Concatenate the results for all the vessels
        stop_pings = pd.concat(results, ignore_index=True)

        # Create stop_area dataframe for each stop area. This dataframe includes the average lat, lon, start time,
        # end time, vessel type, draft, cargo, length and width for each stop area
        stop_area = stop_pings.groupby(['mmsi', 'stop_area_id'])\
            .agg(lat = ('lat','mean'),lon=('lon','mean'), start_time = ('basedatetime','min'),end_time=('basedatetime','max'), vesseltype = ('vesseltype','first'),draft=('draft','first'),cargo=('cargo','first'), length=('length','first'), width=('width','first')).reset_index()
        #Drop any duplicate rows
        stop_area = stop_area.drop_duplicates()

        # Function to export dataframes to database
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

        # Name for stop_pings and stop_area dataframes
        stop_pings_table_name = 'ais_' + str(yr) + '_' + str(mo) + '_stop_pings'
        stop_area_table_name = 'ais_' + str(yr) + '_' + str(mo) + '_stop_area'
        # print('Transferring started for ', stop_pings_table_name, datetime.datetime.now(), flush=True)
        engine = create_engine('postgresql://username:password@server/database') #Replace username, password, server, and database
        # Export stop_pings dataframes to database
        stop_pings.to_sql(stop_pings_table_name, engine, method=psql_insert_copy, index=False, schema='wcs', chunksize=20000)
        # print('Transferring completed for ', stop_pings_table_name, datetime.datetime.now(), flush=True)

        # print('Transferring started for ', stop_area_table_name, datetime.datetime.now(), flush=True)
        # Export stop_pings dataframes to database
        #stop_area.to_sql(stop_area_table_name, engine, method=psql_insert_copy, index=False, schema='wcs', chunksize=20000)
        # print('Transferring completed for ', stop_area_table_name, datetime.datetime.now(), flush=True)
        print('Transferring completed for ', yr, mo, datetime.datetime.now(), flush=True)
    print('Algorithm completed for ', yr, datetime.datetime.now(), flush=True)
