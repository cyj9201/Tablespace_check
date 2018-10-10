# import module

import time
import boto3
import datetime
import cx_Oracle
import pandas as pd
import os
os.environ['LD_LIBRARY_PATH']=':/ORACLE/db/12c/lib'


# Adjust each Tablespace's Usages Percentage of MAXSIZE
tbs_pct_num = 95

print ("**********************************")
start_time = datetime.datetime.fromtimestamp(time.time())
print ("start_time: ",start_time," seconds")

# Create an SNS client

# DB connencting
con_fims2005 = cx_Oracle.connect('fimsr/vudrk_read@192.168.1.130:1521/FIMS2005')
con_funddb   = cx_Oracle.connect('11834/3793@192.168.1.127:1521/FUNDDB')
con_idx01    = cx_Oracle.connect('11834/3793@192.168.1.151:1521/IDX01')
con_fimsdev  = cx_Oracle.connect('fimsr/vudrk_read@192.168.1.139:1521/FIMS2005')

cur_fims2005 = con_fims2005.cursor()
cur_funddb   = con_funddb.cursor()
cur_idx01    = con_idx01.cursor()
cur_fimsdev  = con_fimsdev.cursor()

# PRINT SQL Result

tablespace_query            = 'SELECT TBS_NM \
                                    , tbs_sz                                as "Total(MB)"\
                                    , tbs_alloc_sz                          as "Alloc(MB)"\
                                    , tbs_alloc_sz - tbs_free_sz            as "Used(MB)"\
                                    , round((tbs_alloc_sz-tbs_free_sz)/tbs_sz*100,1) AS "Used(%)"\
                                    , tbs_sz - (tbs_alloc_sz - tbs_free_sz) as "Free(MB)"\
                                 FROM\
                                     (\
                                       SELECT tbs_nm\
                                            , max(tbs_alloc_sz)       as tbs_alloc_sz\
                                            , CASE WHEN max(tbs_sz) < max(tbs_alloc_sz) THEN max(tbs_alloc_sz)\
                                                   ELSE max(tbs_sz)\
                                              END                     as tbs_sz\
                                            , max(tbs_free_sz)        as tbs_free_sz\
                                         FROM\
                                             (\
                                               select tablespace_name                      as tbs_nm\
                                                    , round(sum(bytes)/power(1024,2),2)    as tbs_alloc_sz\
                                                    , round(sum(MAXBYTES)/power(1024,2),2) as tbs_sz\
                                                    , 0                                    As tbs_free_sz\
                                                    from dba_data_files\
                                                    group by tablespace_name\
                                                    union all\
                                                    select distinct\
                                                    tablespace_name as tbs_nm\
                                                    , 0               as tbs_alloc_sz\
                                                    , 0               as tbs_sz\
                                                    , round(sum(bytes) over (partition by tablespace_name)/power(1024,2)) As tbs_free_sz\
                                                    from dba_free_space\
                                                    )\
                                            group by tbs_nm\
                                            ) where 0=0 order by 5 desc'

tablespace_query2           = 'SELECT TBS_NM \
                                    , tbs_sz                                as "Total(MB)"\
                                    , tbs_alloc_sz                          as "Alloc(MB)"\
                                    , tbs_alloc_sz - tbs_free_sz            as "Used(MB)"\
                                    , round((tbs_alloc_sz-tbs_free_sz)/tbs_sz*100,1) AS "Used(%)"\
                                    , tbs_sz - (tbs_alloc_sz - tbs_free_sz) as "Free(MB)"\
                                 FROM\
                                     (\
                                       SELECT tbs_nm\
                                            , max(tbs_alloc_sz)       as tbs_alloc_sz\
                                            , CASE WHEN max(tbs_sz) < max(tbs_alloc_sz) THEN max(tbs_alloc_sz)\
                                                   ELSE max(tbs_sz)\
                                              END                     as tbs_sz\
                                            , max(tbs_free_sz)        as tbs_free_sz\
                                         FROM\
                                             (\
                                               select tablespace_name                      as tbs_nm\
                                                    , round(sum(bytes)/power(1024,2),2)    as tbs_alloc_sz\
                                                    , round(sum(MAXBYTES)/power(1024,2),2) as tbs_sz\
                                                    , 0                                    As tbs_free_sz\
                                                    from dba_data_files\
                                                    group by tablespace_name\
                                                    union all\
                                                    select distinct\
                                                    tablespace_name as tbs_nm\
                                                    , 0               as tbs_alloc_sz\
                                                    , 0               as tbs_sz\
                                                    , round(sum(bytes) over (partition by tablespace_name)/power(1024,2)) As tbs_free_sz\
                                                    from dba_free_space\
                                             )\
                                        WHERE tbs_nm LIKE \'TRL%\'\
                                        group by tbs_nm\
                                      )'
                                             
# Input Oracle tablespace_amount_used to Pandas DataFrame

tablespace_fims2005 = pd.read_sql(tablespace_query, con_fims2005, index_col=None)
tablespace_funddb   = pd.read_sql(tablespace_query, con_funddb  , index_col=None)
tablespace_idx01    = pd.read_sql(tablespace_query, con_idx01   , index_col=None)
tablespace_fimsdev  = pd.read_sql(tablespace_query2, con_fimsdev, index_col=None)

# add DB_NAME to DataFrame
tablespace_fims2005['DB_NAME'] = 'FIMS2005'
tablespace_funddb['DB_NAME']   = 'FUNDDB'
tablespace_idx01['DB_NAME']    = 'IDX01'
tablespace_fimsdev['DB_NAME']  = 'FIMSDEV'


# concat each tablespace_DataFrame
tablespace_all = pd.concat([tablespace_fims2005, tablespace_funddb, tablespace_idx01, tablespace_fimsdev]).reset_index(drop=True)

# if amount_used > {tbs_pct_num}% record Data to list
tablespace_all_result = []
for i in range(len(tablespace_all)):
    if tablespace_all['Used(%)'][i] > tbs_pct_num:
        tablespace_all_result.append(tablespace_all.iloc[i])

# formatting to text
tablespace_all_result2 = []
if len(tablespace_all_result)>=1:
    for i in range(len(tablespace_all_result)):
        tablespace_all_result2.append('{}. The DB[{}]\'s Tablespace[{}]\'s usages percentage is {} %.'.format('{}'.format(i+1),tablespace_all_result[i]['DB_NAME'],
                                      tablespace_all_result[i]['TBS_NM'],tablespace_all_result[i]['Used(%)']))
tablespace_all_result3 = []
tablespace_all_result3 = '\n'.join(str(x) for x in tablespace_all_result2)

# print SQL Result 2
datafile_query="select TABLESPACE_NAME\
                    , FILE_ID\
                    , FILE_NAME\
                    , round(sum(bytes)/power(1024,2),2) AS MB\
                    , AUTOEXTENSIBLE\
                    , round(sum(MAXBYTES)/power(1024,2),2) AS MAXBYTES\
                   from DBA_DATA_FILES\
                   where tablespace_name= '{}'\
                   group by TABLESPACE_NAME, FILE_ID, FILE_NAME, AUTOEXTENSIBLE\
                   order by 2"

datafile_query2=[]
if tablespace_all_result:
    for i in range(len(tablespace_all_result)):
        datafile_query2.append(datafile_query.format(tablespace_all_result[i]['TBS_NM']))

# Input Oracle Data_file to Pandas DataFrame
datafile_fims2005 = []
datafile_funddb   = []
datafile_idx01    = []
datafile_fimsdev  = []

if datafile_query2:
    for i in range(len(datafile_query2)):
        if tablespace_all_result[i]['DB_NAME']=='FIMS2005':
            datafile_fims2005.append(pd.read_sql(datafile_query2[i],con_fims2005,index_col=None))
        elif tablespace_all_result[i]['DB_NAME']=='FUNDDB':
            datafile_funddb.append(pd.read_sql(datafile_query2[i],con_funddb,index_col=None))
        elif tablespace_all_result[i]['DB_NAME']=='IDX01':
            datafile_idx01.append(pd.read_sql(datafile_query2[i],con_idx01,index_col=None))
        elif tablespace_all_result[i]['DB_NAME']=='FIMSDEV':
            datafile_fimsdev.append(pd.read_sql(datafile_query2[i],con_idx01,index_col=None))


datafile_all = datafile_fims2005 + datafile_funddb + datafile_idx01 + datafile_fimsdev


# from DataFrame to formatting text
datafile_all2=[]

if datafile_all:
    for i in range(len(datafile_all)):
        data=[]
        for j in range(len(datafile_all[i])):
            if datafile_all[i].iloc[j]['TABLESPACE_NAME'] == tablespace_all_result[i]['TBS_NM']:
                data.append('\n'.join(str(x) for x in list(datafile_all[i].iloc[j])).replace('\n',' '))
        datafile_all2.append(data)

datafile_all3=[]

if datafile_all2:
    for i in range(len(datafile_all)):
        datafile_all3.append('{}. {}'.format(i+1,tablespace_all_result[i]['TBS_NM']) + '\n'
                             + '\n'.join(str(x) for x in list(datafile_all[i].columns)).replace('\n',' ') +'\n'
                             + '\n'.join(str(x) for x in list(datafile_all2[i]))+'\n')
datafile_all3_2= '\n'.join(str(x) for x in datafile_all3)

# classify Scale up or Scale out and show sql_query

scale_up  = []
scale_out = []

scale_up  = 'ex) ALTER DATABASE DATAFILE \'FILE_NAME\' AUTOEXTEND ON NEXT 1000M MAXSIZE 10000M ; '
scale_out = 'ex) ALTER TABLESPACE TBS_NAME ADD DATAFILE \'FILE_NAME\' SIZE 1M AUTOEXTEND ON NEXT 1000M MAXSIZE 10000M ;'


# combine all formatting text
sns = []
if tablespace_all_result3:    
    sns = tablespace_all_result3 + '\n\n' + datafile_all3_2 + '\n\n' + scale_up + '\n\n' + scale_out
    
if sns:
    print(sns)
else:
    print("There are not tablespaces more than ", tbs_pct_num, "% of maxsize.")

# DB Closing
cur_fims2005.close()
cur_funddb.close()
cur_idx01.close()
cur_fimsdev.close()

con_fims2005.close()
con_funddb.close()
con_idx01.close()
con_fimsdev.close()

end_time = datetime.datetime.fromtimestamp(time.time())
print ("end_time: ",end_time," seconds")
elapsed = (end_time-start_time)
print ("elapsed: ",elapsed," seconds")
print ("**********************************")
print ("")
