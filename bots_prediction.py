
# coding: utf-8

# In[1]:


import pandas as pd
import pymysql
import datetime as dt
import numpy as np
import sklearn
from sqlalchemy import create_engine, text
from sklearn import linear_model
from statsmodels.tsa.arima_model import ARIMA
from sklearn.metrics import mean_squared_error
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.arima_model import ARMAResults
from statsmodels.tsa.stattools import acf, pacf
import time
import datetime
from datetime import date, timedelta
from dfply import *
import yagmail
import pygsheets
import json
from pandas.io.json import json_normalize
from io import StringIO
import boto3
import pyathena


# In[2]:


import boto3
from json import loads
from sqlalchemy import create_engine
import s3fs
import psycopg2
import pymysql
import pandas as pd

def get_db_creds():
    secret_name = "analytics/production-redshift-user"
    region_name = "ap-southeast-1"

    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    creds = loads(get_secret_value_response['SecretString'])

    return creds['username'], creds['password']

def get_s3_client():
    session = boto3.session.Session()
    client = session.client(service_name='sts')
    creds = client.assume_role(RoleArn='arn:aws:iam::813886543639:role/AnalyticsMasterLegacy',
            RoleSessionName='analytics-automation')

    s3Sess = boto3.session.Session(aws_access_key_id=creds['Credentials']['AccessKeyId'],
            aws_secret_access_key=creds['Credentials']['SecretAccessKey'],
            aws_session_token=creds['Credentials']['SessionToken'])

    return s3Sess.client(service_name='s3', region_name='ap-south-1')

username, password = get_db_creds()

s3 = get_s3_client()
#resp['Body'].read()

#Query for bookings data
def get_athena_connection():
    return pyathena.connect(role_arn='arn:aws:iam::813886543639:role/AnalyticsMasterLegacy',
                            role_session_name='analytics-automation-athena',
                            s3_staging_dir='s3://reporting.shuttl.internal/query-results/analytics/python-jdbc/',
                            region_name='ap-south-1',
                            schema_name='raw',
                            work_group='analytics')


# In[3]:


def get_client(service):
    session = boto3.session.Session()
    client = session.client(service_name='sts')
    creds = client.assume_role(RoleArn='arn:aws:iam::813886543639:role/AnalyticsMasterLegacy',
                               RoleSessionName='analytics-automation')
    sess = boto3.session.Session(aws_access_key_id=creds['Credentials']['AccessKeyId'],
                                   aws_secret_access_key=creds['Credentials']['SecretAccessKey'],
                                   aws_session_token=creds['Credentials']['SessionToken'])
    return sess.client(service_name=service, region_name='ap-south-1')

def run_query(client, query, database):
    response = client.start_query_execution(
                        QueryString=query,
                        QueryExecutionContext={'Database': database},
                        WorkGroup='analytics'
                        )
    return response

s3_client = get_client('s3')
athena_client = get_client('athena')


# In[4]:


def capacity(x):
    if x<=20:
        return "<=20"
    else:
        return ">20"

def distance(x):
    if x<=2000:
        return "0-2 KM"
    elif ((x>2000)&(x<=5000)):
        return "2-5 KM"
    elif ((x>5000)&(x<=10000)):
        return "5-10 KM"
    elif ((x>10000)&(x<=20000)):
        return "10-20 KM"
    else:
        return ">20 KM"

def hour_bucket(x):
    if x<=6.5:
        return "<6:30 slot"
    elif (x>6.5) & (x<=7) :
        return "6:30-7 slot"
    elif (x>7) & (x<=7.5) :
        return "7-7:30 slot"
    elif (x>7.5) & (x<=8) :
        return "7:30-8 slot"
    elif (x>8) & (x<=8.5) :
        return "8-8:30 slot"
    elif (x>8.5) & (x<=9) :
        return "8:30-9 slot"
    elif (x>9) & (x<=10) :
        return "9-10 slot"
    elif (x>10) & (x<=11) :
        return "10-11 slot"
    elif (x>16) & (x<=16.5) :
        return "16-16:30 slot"
    elif (x>16.5) & (x<=17) :
        return "16:30-17 slot"
    elif (x>17) & (x<=17.5) :
        return "17-17:30 slot"
    elif (x>17.5) & (x<=18) :
        return "17:30-18 slot"
    elif (x>18) & (x<=18.5) :
        return "18-18:30 slot"
    elif (x>18.5) & (x<=19) :
        return "18:30-19 slot"
    elif (x>19) & (x<=20) :
        return "19-20 slot"
    elif (x>20) & (x<=21) :
        return "20-21 slot"
    else:
        return "rest"

def applyZero(x):
    if x<=9:
        return "0"
    else:
        return ""

def date_reshaping(x):
    x=datetime.datetime.strptime(x , "%Y_%W_0%w")
    return x


# In[5]:


con_dict = {'user':'ac1386IU', 'password':'ac123456', 'host':'172.31.14.189', 'port':3306}


# In[61]:


#Setting Dates
no_of_days=61
date_today = (pd.to_datetime(datetime.date.today()))
weekday = date_today.strftime("%A")

if weekday=='Thursday':
    targetDate = date_today + datetime.timedelta(days=4)
    targetDate=pd.to_datetime(targetDate)
    endDate=date_today
    startDate=(endDate - datetime.timedelta(days=no_of_days))
    targetDate1 = date_today - datetime.timedelta(days=1)
    targetDate1=pd.to_datetime(targetDate1)
else:
    targetDate = date_today + datetime.timedelta(days=2)
    targetDate=pd.to_datetime(targetDate)
    endDate=date_today
    startDate=(endDate - datetime.timedelta(days=no_of_days))
    targetDate1 = date_today - datetime.timedelta(days=1)
    targetDate1=pd.to_datetime(targetDate1)
    
print('targetDate', targetDate)
print('targetDate1', targetDate1)
print('End Date',endDate)


# In[62]:


delta = endDate - startDate   
# timedelta

date_index = pd.DataFrame()
temp_df = pd.DataFrame(index = {0}, columns = ['date'])

for i in range(delta.days):
    temp_df['date_of_travel'] = (startDate + timedelta(i))
    date_index = date_index.append(temp_df)    

date_index['date_of_travel'] = pd.to_datetime(date_index.date_of_travel)
date_index['day_name'] = date_index.date_of_travel.dt.weekday_name
date_index = (date_index >>
              mask(~X.day_name.isin(['Saturday', 'Sunday'])) >>
              select(X.date_of_travel) >>
              mutate(prediction = -1))


# In[63]:


date_index.shape


# In[8]:


query="""
Select 
    it.id as trip_id,    
    it.route_id as old_route_id,
    rsm.landmark_id as old_location_id,
    rsm.position,
    v.seats,
    date(from_unixtime(it.slot_time)) as date_of_travel,
    time(from_unixtime(it.slot_time)) as slot_time,
    time(from_unixtime(th.time)) as pick_up_time
from shuttl.itinerary_trip it
left join shuttl.vehicles as v 
    on v.id=it.vehicle_id
left join RMS.ROUTE_STOP_MAPPINGS as rsm
    on it.route_id=rsm.route_id
left join analytics.trip_history th
    on it.id=th.trip_id and th.location_id=rsm.landmark_id
left join RMS.ROUTES as r
    on r.id=it.route_id
where
    it.route_id>0
    and time > 0
    and it.valid = 1
    and weekday(from_unixtime(it.slot_time)) <= 4
    and not exists (select route_id from RMS.EXEMPTED_ROUTES as er where deleted=0 and er.route_id=it.route_id)
    and it.slot_time between unix_timestamp('{0}') and unix_timestamp('{1}')
    and not exists (Select 1 from RMS.LINE as l where l.id=r.line_id and l.line_name in ('Null','Freego','B2B Route','B2B'))
    and exists (Select 1 from RMS.LINE as l left join RMS.ZONE as z on z.id=l.zone_id where l.id=r.line_id)
    and rsm.deleted=0
;
"""
conn = pymysql.connect(**con_dict)
trips_landmark_reach_time = pd.read_sql_query(query.format(startDate,endDate), conn)
conn.close()


# In[9]:


user_db_string='postgresql://'+username+':'+password+'@analytics.cubs1zinxavu.ap-south-1.redshift.amazonaws.com:5439/reporting'
engine = create_engine(user_db_string)
conn = engine.connect()

query="""
Select 
    distinct
    rr.route_number as old_route_id,
    rr.route_id
from rms_core_route_revision as rr
;
"""
old_new_route_mapping = pd.read_sql_query(query, conn)
conn.close()


# In[10]:



user_db_string='postgresql://'+username+':'+password+'@analytics.cubs1zinxavu.ap-south-1.redshift.amazonaws.com:5439/reporting'
engine = create_engine(user_db_string)
conn = engine.connect()

query="""
Select 
    distinct
    old_id as old_location_id,
    id as location_id
from rms_core_stop as s
;
"""
old_new_stop_mapping= pd.read_sql_query(query, conn)
conn.close()


# In[11]:


trips_landmark_reach_time=trips_landmark_reach_time.merge(old_new_route_mapping,how='inner',on=['old_route_id']).merge(old_new_stop_mapping,how='left',on=['old_location_id'])


# In[12]:



user_db_string='postgresql://'+username+':'+password+'@analytics.cubs1zinxavu.ap-south-1.redshift.amazonaws.com:5439/reporting'
engine = create_engine(user_db_string)
conn = engine.connect()

query="""
with
trip as
(select 
     trip_id,
     route_id,
     line_id,
     date_of_travel,
     slot_time,
     start_time
 from 
    (select 
         t.id as trip_id,
         substring(replace((regexp_substr(t.tags,'rid[^,]*')),'rid_',''),1,36) as route_id,
         substring(replace((regexp_substr(t.tags,'lineid[^,]*')),'lineid_',''),1,36) as line_id,
         trunc(t.start_time + interval '5:30 Hours') as date_of_travel,
         to_char(t.start_time + interval '5:30 Hours', 'HH24:MI:SS') as slot_time,
         t.start_time,
         t.state,
         t.type,
         row_number()over(partition by t.id order by t.audited_at desc) rn
    from tms_trip_audit t
    where 
        (t.start_time + interval '5:30 Hours') between  timestamp '{0}' and timestamp '{1}') a 
 where a.rn =1
 and state = 'COMPLETED'
 and type in ('AUTOMATIC', 'MANUAL')),
trip_vehicle_allocation as
(select 
     trip_id,
     allocation_id
 from 
    (select 
         trip_id,
         allocation_id,
         row_number()over(partition by trip_id order by updated_at desc) rn
    from tms_trip_vehicle_allocation_audit) a 
 where a.rn =1),
active_revision as
(select 
     route_id,
     revision_id
 from 
    (select 
         route_id,
         id as revision_id,
         row_number()over(partition by route_id order by created_at desc) rn
    from rms_core_route_revision) a 
 where a.rn =1)

Select 
    t.trip_id,
    t.route_id,
    t.line_id,
    v.available_seats as seats,
    t.date_of_travel,
    t.slot_time,
    t.start_time,
    wp.stop_id as location_id,
    wp.order as position
from trip as t
left join trip_vehicle_allocation tva on t.trip_id = tva.trip_id
left join fleet_vehicle_allocations va on tva.allocation_id = va.id
left join fleet_vehicle v on va.vehicle_id = v.id
left join active_revision as ar on ar.route_id = t.route_id
left join rms_core_way_point as wp on wp.route_revision_id = ar.revision_id
left join b2c_rms_line as l on l.id = t.line_id
left join b2c_rms_zone as z on z.id = l.zone_id
where
    wp.type <> 'VIA';

"""
trips_landmark_reach_time_new = pd.read_sql_query(query.format(startDate,endDate), conn)
conn.close()
trips_landmark_reach_time_new['date_of_travel'] = pd.to_datetime(trips_landmark_reach_time_new['date_of_travel'])


# In[13]:


list = ['tms_trip_audit', 'tms_trip_track_history', 'rms_core_way_point', 'rms_core_route_revision', 'b2c_rms_line', 'b2c_rms_zone', 'fleet_vehicle', 'fleet_route_slot_vehicle_mapping_audit']

conn = get_athena_connection()
cursor = conn.cursor()
for i in range(len(list)):
    key = 'msck repair table ' + list[i] 
    cursor.execute(key)
    print(key)
conn.close()    


# In[14]:


query='''
with trip as
(select
    trip_id
from
   (select
        t.id as trip_id,
        state,
        type,
        row_number() over (partition by t.id order by t.audited_at desc) rn
   from tms_trip_audit t
   where
       (t.start_time + interval '5' hour + interval '30' minute) between timestamp '{0}' and timestamp '{1}') a
where a.rn =1
and state = 'COMPLETED'
and type in ('AUTOMATIC', 'MANUAL'))

select trip_id,
       json_extract(way_point,'$.stop_id') stop_id,
        json_extract(way_point,'$.departed_at') departed_at,
         json_extract(way_point,'$.estimated_departure_time') estimated_departure_time
from (
 
 SELECT trip_id,
         json_extract_scalar(tracking_details,'$.updated_at') AS updated_at,   
         tracking_details
from
(select
      tth.trip_id,
      tracking_details,
      row_number() over (partition by t.trip_id order by tth.created_at desc) rn
 from tms_trip_track_history tth
 join trip t on (tth.trip_id=t.trip_id)
) a
where a.rn = 1) t 
cross join unnest (cast(json_extract(tracking_details,'$.way_point_info') AS ARRAY<JSON>)) u(way_point) ;
'''


# In[15]:


res = run_query(athena_client, query.format(startDate, endDate), 'raw')
ex = athena_client.get_query_execution(QueryExecutionId=res['QueryExecutionId'])
while ex['QueryExecution']['Status']['State'] == 'RUNNING':
  ex = athena_client.get_query_execution(QueryExecutionId=res['QueryExecutionId'])
  print(ex['QueryExecution']['Status']['State'])  
  time.sleep(5)
(bucket, path) = ex['QueryExecution']['ResultConfiguration']['OutputLocation'].replace('s3://', '').split('/', 1)
s3 = get_s3_client()
s3.download_file(bucket, Key=path, Filename='trip_history.csv')
trip_track_history = pd.read_csv('trip_history.csv')


# In[16]:


trip_history = trip_track_history[['trip_id', 'stop_id', 'departed_at']].rename(columns = {'stop_id' : 'location_id', 'departed_at' : 'time'})
trip_history['location_id'] = trip_history.location_id.str.replace('"', '')
trip_history['time'] = trip_history.time.str.replace('"', '')
trip_history['time'] = pd.to_datetime(trip_history['time']) + dt.timedelta(seconds = 19800)
trip_history['pick_up_time']= trip_history['time']-pd.to_datetime(trip_history['time'].dt.date)
trip_history = trip_history.sort_values(['trip_id', 'time'])
trip_history = trip_history.sort_values(['trip_id', 'time']).drop_duplicates(subset=['trip_id','time'], keep='last')


# In[17]:


trips_landmark_reach_time_new = trips_landmark_reach_time_new.merge(trip_history, how='left', on=['trip_id','location_id'])
trips_landmark_reach_time_new = trips_landmark_reach_time_new.drop(['line_id','start_time','time'],axis=1)


# In[18]:


trips_landmark_reach_time=trips_landmark_reach_time.append(trips_landmark_reach_time_new)


# In[19]:


trips_landmark_reach_time.slot_time=pd.to_timedelta(trips_landmark_reach_time.slot_time)
trips_landmark_reach_time.pick_up_time=pd.to_timedelta(trips_landmark_reach_time.pick_up_time)
trips_landmark_reach_time.loc[trips_landmark_reach_time.pick_up_time =='05:30:00','pick_up_time']=None
trips_landmark_reach_time.date_of_travel=pd.to_datetime(trips_landmark_reach_time.date_of_travel)


# In[20]:


conn = get_athena_connection()
query="""
with
active_revision as
(select 
     route_id,
     route_revision_id
 from 
    (select 
         route_id,
         id as route_revision_id,
         row_number()over(partition by route_id order by created_at desc) rn
    from rms_core_route_revision) a 
 where a.rn =1)

select 
    ar.route_revision_id,
    ar.route_id,
    wp.stop_id as location_id,
    wp."order" as position
from active_revision ar
left join rms_core_way_point wp on wp.route_revision_id=ar.route_revision_id
where wp.stop_id is not null
;
"""
route_design = pd.read_sql_query(query, conn)
conn.close()


# In[21]:


route_design = route_design.sort_values(['route_id','position'])


# In[22]:


conn = get_athena_connection()
query="""
with
active_revision as
(select 
     route_id,
     route_number,
     line_id
 from 
    (select 
         route_id,
         route_number,
         row_number()over(partition by route_id order by created_at desc) rn,
         substring(replace((regexp_extract(tags,'lineid[^,]*')),'lineid_',''),1,36) as line_id
    from rms_core_route_revision) a 
 where a.rn =1)

Select
       ar.route_id,
       ar.route_number,
       l.name as line_name,
       z.name as zone_name
from active_revision ar
left join b2c_rms_line as l on l.id =  ar.line_id
left join b2c_rms_zone as z on z.id=l.zone_id
;
"""
route_zone_map = pd.read_sql_query(query, conn)
conn.close()


# In[23]:


conn = get_athena_connection()
query="""
with
route_slot_vehicle_mapping as
(select  
     route_id,
     time_in_secs,
     weekday,
     vehicle_id
 from 
    (select 
         route_id,
         slot_time as time_in_secs,
         weekday,
         vehicle_id,
         row_number()over(partition by route_id, slot_time, weekday order by updated_at desc) rn
    from fleet_route_slot_vehicle_mapping_audit
    where
        is_deleted = false) a 
 where a.rn =1)

select
    route_id,
    time_in_secs,
    weekday,
    vehicle_id,
    available_seats as seats
from route_slot_vehicle_mapping vm
left join fleet_vehicle v on vm.vehicle_id = v.id
;
"""
slot_vehicle_map = pd.read_sql_query(query, conn)
conn.close()


# In[24]:


slot_vehicle_map = slot_vehicle_map[slot_vehicle_map.weekday==targetDate.weekday_name.upper()]
slot_vehicle_map['time_in_secs'] = round((slot_vehicle_map['time_in_secs']/100).astype(int)*3600+((slot_vehicle_map['time_in_secs']/100)-(slot_vehicle_map['time_in_secs']/100).astype(int))*6000,0)
slot_vehicle_map = slot_vehicle_map[['route_id', 'time_in_secs', 'seats']]


# In[25]:


s3 = get_s3_client()
key = 'reports/route-distance/' + targetDate1.strftime('%Y/%m/%d') + '.csv'
s3.download_file('analytics-reports.shuttl.internal', Key=key,Filename='route_points.csv')
route_points=pd.read_csv('route_points.csv')


# In[26]:


distance_df=route_points[['route_id','stop_id','distance_in_metres']].rename(columns={'stop_id':'location_id','distance_in_metres':'distance_in_meters'})
distance_df = distance_df[distance_df['route_id'].notnull()]


# In[27]:


s3 = get_s3_client()
key = 'reports/route-stop-timings/' + targetDate1.strftime('%Y/%m/%d') + '.csv'
s3.download_file('analytics-reports.shuttl.internal', Key = key,Filename='route_slot_schedule.csv')
route_slot_schedule_new=pd.read_csv('route_slot_schedule.csv')
route_slot_schedule_new['day_of_week'] = route_slot_schedule_new.day_of_week.str.replace('"', '')
route_slot_schedule_new['stop_id'] = route_slot_schedule_new.stop_id.str.replace('"', '')


# In[28]:


route_revisions = "','".join(map(str,route_slot_schedule_new['route_revision_id'].unique()))


# In[29]:


conn = get_athena_connection()
query="""
select
    id as route_revision_id,
    route_id
from rms_core_route_revision rr
where
    rr.id in ('{0}')
;
"""
route_revision_map = pd.read_sql_query(query.format(route_revisions), conn)
conn.close()


# In[30]:


route_slot_schedule = route_slot_schedule_new[route_slot_schedule_new.day_of_week==targetDate.weekday_name.upper()].rename(columns={'stop_id':'location_id'})


# In[31]:


route_slot_schedule = route_slot_schedule.merge(route_revision_map, on = 'route_revision_id')

route_slot_schedule = route_design.merge(route_slot_schedule,how='left',on=['route_id','location_id'])

route_slot_schedule['previous_position']=route_slot_schedule['position']-1

route_slot_schedule=route_slot_schedule[route_slot_schedule.slot_time.notnull()]

route_slot_schedule['time_in_secs']=round((route_slot_schedule['slot_time']/100).astype(int)*3600+((route_slot_schedule['slot_time']/100)-(route_slot_schedule['slot_time']/100).astype(int))*6000, 0)

route_slot_schedule=route_slot_schedule[route_slot_schedule.position==0][['route_id','position','previous_position','location_id','time_in_secs']]


# In[32]:


route_slot_schedule = route_slot_schedule.merge(route_zone_map[['route_id']], how = 'inner', on = 'route_id')


# In[33]:


route_design=route_design.merge(distance_df,how='left',on=['route_id','location_id'])

route_design=route_design.sort_values(['route_id','position']).reset_index(drop=True)

route_design[['previous_position','previous_location_id','previous_distance_in_meters']]=route_design.groupby(['route_id']).agg({'position':'shift','location_id':'shift','distance_in_meters':'shift'})

route_design['distance_in_meters_fpp']=route_design['distance_in_meters']-route_design['previous_distance_in_meters']

route_design = route_design.fillna(0)


# In[34]:


trips_landmark_reach_time=trips_landmark_reach_time.sort_values(['trip_id','position'])

trips_landmark_reach_time=trips_landmark_reach_time.merge(distance_df, how='left',on=['route_id','location_id'])

trips_landmark_reach_time[['previous_position','previous_location_id','time_at_previous_location','previous_distance_in_meters']]=trips_landmark_reach_time.groupby(['trip_id']).agg({'position':'shift','location_id':'shift','pick_up_time':'shift','distance_in_meters':'shift'})

trips_landmark_reach_time['time_taken_from_previous_position']=(trips_landmark_reach_time.pick_up_time-trips_landmark_reach_time.time_at_previous_location).dt.total_seconds()

trips_landmark_reach_time['hourOfDay'] = trips_landmark_reach_time.slot_time.dt.total_seconds()

trips_landmark_reach_time['hourOfDay'] = round(round(trips_landmark_reach_time.hourOfDay, 0)/3600,2)

trips_landmark_reach_time['time_taken_from_previous_position'] = trips_landmark_reach_time['time_taken_from_previous_position'].apply(lambda x: x if x > 0  else None)

trips_landmark_reach_time['distance_in_meters_fpp']=trips_landmark_reach_time['distance_in_meters']-trips_landmark_reach_time['previous_distance_in_meters']

trips_landmark_reach_time=trips_landmark_reach_time[((trips_landmark_reach_time.previous_position.notnull())&(trips_landmark_reach_time.time_taken_from_previous_position.notnull()))].reset_index(drop=True)


# In[35]:



trips_landmark_reach_time['time_buckets'] = trips_landmark_reach_time['hourOfDay'].apply(lambda x: hour_bucket(x))

trips_landmark_reach_time['capacity'] = trips_landmark_reach_time['seats'].apply(lambda x: capacity(x))

trips_landmark_reach_time['distance_in_meters_fpp'] = trips_landmark_reach_time['distance_in_meters_fpp'].fillna(500)

trips_landmark_reach_time['distance_in_meters_fpp']=(round(trips_landmark_reach_time['distance_in_meters_fpp']/100,0)*100).astype(int)

trips_landmark_reach_time['previous_location_id']=trips_landmark_reach_time['previous_location_id']

trips_landmark_reach_time['unique_tuple']= trips_landmark_reach_time.previous_location_id.map(str) + "_" + trips_landmark_reach_time.location_id.map(str) + "_" + trips_landmark_reach_time.capacity.map(str) + "_" +  trips_landmark_reach_time.time_buckets.map(str)


# In[36]:


tuple_mapping = trips_landmark_reach_time.drop_duplicates('unique_tuple')[['unique_tuple','previous_location_id', 'location_id', 'capacity', 'time_buckets']].reset_index(drop = True)


# In[37]:


trips_landmark_reach_time=trips_landmark_reach_time[(trips_landmark_reach_time.date_of_travel>=startDate)&(trips_landmark_reach_time.date_of_travel<targetDate)]


# In[38]:




quantile=trips_landmark_reach_time.groupby(['unique_tuple','date_of_travel']).time_taken_from_previous_position.describe().reset_index().rename(columns={'25%':'q_25','75%':'q_75'})[['unique_tuple','date_of_travel','q_25','q_75']]

trips_landmark_reach_time=trips_landmark_reach_time.merge(quantile,how='left',on=['unique_tuple','date_of_travel'])

trips_landmark_reach_time=trips_landmark_reach_time[(trips_landmark_reach_time.time_taken_from_previous_position>=(trips_landmark_reach_time.q_25-(1.5*(trips_landmark_reach_time.q_75-trips_landmark_reach_time.q_25)))) & 
                        (trips_landmark_reach_time.time_taken_from_previous_position<=(trips_landmark_reach_time.q_75+(1.5*(trips_landmark_reach_time.q_75-trips_landmark_reach_time.q_25))))]


# In[39]:


input_data=trips_landmark_reach_time.groupby(['unique_tuple','date_of_travel']).time_taken_from_previous_position.mean().reset_index()

input_data['prediction']=-1

unique_tuple_df=input_data[['unique_tuple','prediction']].drop_duplicates().reset_index(drop=True)

date_index=pd.merge(unique_tuple_df, date_index, how='outer', on=['prediction']).reset_index(drop=True).drop(columns=['prediction'],axis=1)

input_data=date_index.merge(input_data,how='left',on=['unique_tuple','date_of_travel'])


# In[40]:



quantile=input_data.groupby(['unique_tuple']).time_taken_from_previous_position.describe().reset_index().rename(columns={'25%':'q_25','75%':'q_75'})[['unique_tuple','q_25','q_75']]

input_data=input_data.merge(quantile,how='left',on=['unique_tuple'])

input_data.loc[(input_data.time_taken_from_previous_position<(input_data.q_25-(1.5*(input_data.q_75-input_data.q_25)))) & 
                        (input_data.time_taken_from_previous_position>(input_data.q_75+(1.5*(input_data.q_75-input_data.q_25)))),'time_taken_from_previous_position']=None

input_data=input_data.merge((input_data.groupby(['unique_tuple']).time_taken_from_previous_position.mean().reset_index().rename(columns={'time_taken_from_previous_position':'mean_time_taken'})),how='left',on=['unique_tuple']).sort_values(['unique_tuple','date_of_travel'])

input_data.loc[input_data.time_taken_from_previous_position.isnull(),'time_taken_from_previous_position']=input_data['mean_time_taken']

input_data=input_data.sort_values(['unique_tuple','date_of_travel'])


# In[41]:


unique_tuple_df['exp_smooth_prediction']=-1


# In[42]:


unique_tuple_df=unique_tuple_df.merge(input_data[['unique_tuple','mean_time_taken']].drop_duplicates(),how='left',on=['unique_tuple'])


# In[43]:


def exponential_smoothing(panda_series, alpha_value):
    output=sum([alpha_value * (1 - alpha_value) ** i * x for i, x in enumerate(reversed(panda_series))])
    return output


# In[44]:



for i in range(unique_tuple_df.shape[0]):
    panda_series=input_data[input_data.unique_tuple==unique_tuple_df.unique_tuple[i]].reset_index(drop=True)
    panda_series=panda_series.time_taken_from_previous_position
    unique_tuple_df['exp_smooth_prediction'][i]=exponential_smoothing(panda_series,0.4)
    print(i)


# In[45]:


unique_tuple_df.loc[unique_tuple_df.exp_smooth_prediction < 30,'exp_smooth_prediction']=30


# In[ ]:


unique_tuple_df['prediction']=2/((1/unique_tuple_df['mean_time_taken'])+(1/unique_tuple_df['exp_smooth_prediction']))


# In[ ]:


unique_tuple_df = pd.read_excel('unique_tuple_df.xlsx')


# In[ ]:


route_slot_schedule['slot_id']=route_slot_schedule.route_id.map(str) +"_"+ route_slot_schedule.time_in_secs.astype(int).map(str)

route_slot_schedule['hourOfDay']=(route_slot_schedule.time_in_secs/3600)

route_slot_schedule = route_slot_schedule.merge(slot_vehicle_map, how = 'left', on = ['route_id', 'time_in_secs'])

route_slot_schedule=route_slot_schedule[['route_id','slot_id','time_in_secs','hourOfDay','seats']]

route_slot_schedule=route_slot_schedule.merge(route_design, how='left', on=['route_id'])

route_slot_schedule['distance_in_meters_fpp']=(round(route_slot_schedule['distance_in_meters_fpp']/100,0)*100).astype(int)

#route_slot_schedule.previous_location_id=route_slot_schedule.previous_location_id.astype(int)


# In[ ]:


route_slot_schedule['time_buckets']= route_slot_schedule['hourOfDay'].apply(lambda x: hour_bucket(x))
route_slot_schedule['capacity'] = route_slot_schedule['seats'].apply(lambda x: capacity(x))
route_slot_schedule['unique_tuple']= route_slot_schedule.previous_location_id.map(str) + "_" + route_slot_schedule.location_id.map(str) + "_" + route_slot_schedule.capacity.map(str) + "_" +  route_slot_schedule.time_buckets.map(str)


# In[ ]:


route_slot_schedule=route_slot_schedule.merge(unique_tuple_df,how='left',on=['unique_tuple'])

route_slot_schedule.loc[route_slot_schedule['position']==0,'prediction']=route_slot_schedule.time_in_secs

route_slot_schedule.loc[route_slot_schedule['prediction'].isnull(),'prediction']=route_slot_schedule['distance_in_meters_fpp']/8

route_slot_schedule.loc[pd.isnull(route_slot_schedule['prediction']),'prediction']=223.47

route_slot_schedule=route_slot_schedule.sort_values(['slot_id','position'])

route_slot_schedule['promised_time_in_secs']=route_slot_schedule.groupby(['slot_id'])['prediction'].cumsum()


# In[ ]:


route_slot_schedule['shift_time'] = route_slot_schedule.groupby('slot_id').promised_time_in_secs.shift(1)
route_slot_schedule['time_diff'] = route_slot_schedule['promised_time_in_secs'] - route_slot_schedule['shift_time']
bad_entries = route_slot_schedule[route_slot_schedule.time_diff < 0]
route_slot_schedule = route_slot_schedule[~route_slot_schedule.slot_id.isin(bad_entries['slot_id'])]

# COnverting seconds to HHMM
route_slot_schedule['promised_time_in_secs'] = route_slot_schedule['promised_time_in_secs'].apply(lambda x: time.strftime('%H%M', time.gmtime(x)))

route_slot_schedule['time_in_secs'] = route_slot_schedule['time_in_secs'].apply(lambda x: time.strftime('%H%M', time.gmtime(x)))

max_cutoff = pd.read_excel('BOTS max time analysis - Final.xlsx')
max_cutoff['max_eta'] = (pd.to_datetime(max_cutoff.time_in_secs.map(str)) + pd.to_timedelta(max_cutoff['max_eta'], unit = 'm') - pd.to_timedelta(1, unit = 'm')).dt.strftime('%H%M')
max_cutoff['time_in_secs'] = max_cutoff['time_in_secs'].apply(lambda x: x.strftime('%H%M'))
max_cutoff['route_id'] = max_cutoff['route_id'].apply(lambda x: x.lower())

route_slot_schedule = route_slot_schedule.merge(max_cutoff[['route_id', 'time_in_secs', 'max_eta']], on = ['route_id', 'time_in_secs'], how = 'left')

route_slot_schedule = (route_slot_schedule >>
                       mutate(max_eta = if_else(X.max_eta.isnull(), '99999', X.max_eta)) >>
                       mutate(promised_time_in_secs1 = if_else(X.promised_time_in_secs <= X.max_eta, X.promised_time_in_secs, X.max_eta)))


# In[ ]:


route_slot_schedule['Timezone']='Asia/Kolkata'

route_slot_schedule['Day Of Week']=targetDate.weekday_name.upper()

route_slot_schedule['Schedule Date']=str(pd.to_datetime(targetDate))


# In[ ]:


stop_times_predicted=route_slot_schedule.sort_values(['route_id','slot_id','position'])[['route_id','Day Of Week','time_in_secs','location_id','promised_time_in_secs','Timezone']].rename(columns={'route_id':'Route Id','location_id':'Stop Id','promised_time_in_secs':'Departure Time','time_in_secs':'Start Time'})


# In[ ]:


stop_times_predicted.to_csv('stop_times_predicted.csv', index = False)

# csv_buffer = StringIO()
# stop_times_predicted.to_csv(csv_buffer, index = False)

# s3 = get_s3_client()
# key = 'reports/bots_prediction/' + targetDate.strftime('%Y/%m/%d') + '.csv'
# s3.put_object(Body = csv_buffer.getvalue(),Bucket='analytics-reports.shuttl.internal',Key=key)


# yag = yagmail.SMTP('kushal.garg@shuttl.com','Kus@16275')
# subject = 'BOTS predictions for - ' + targetDate.strftime('%d/%m/%Y')#             'Attached is the file containing BOTS predictions.':'body',
#             '/home/kg1740/stop_times_predicted.csv':'file1'
#             }
# yag.send(to=['kushal.garg@shuttl.com', 'jayant.bhawal@shuttl.com', 'himani.joshi@shuttl.com'], subject = subject,contents=contents)

