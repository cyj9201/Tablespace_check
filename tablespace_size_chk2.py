## -*- coding: utf-8 -*-

# import module
import time
import datetime
import cx_Oracle
import pandas as pd
import boto3
import os
os.environ['LD_LIBRARY_PATH']=':/ORACLE/db/12c/lib'

# Create an SNS client
client = boto3.client(
    "sns",
    aws_access_key_id="",
    aws_secret_access_key="",
    region_name=""
)

print ("**********************************")
start_time = datetime.datetime.fromtimestamp(time.time())
print(start_time)

con_fims2005 = cx_Oracle.connect('fimsr/vudrk_read@192.168.1.130:1521/FIMS2005')
con_funddb   = cx_Oracle.connect('11834/3793@192.168.1.127:1521/FUNDDB')
con_idx01    = cx_Oracle.connect('11834/3793@192.168.1.151:1521/IDX01')

cur_fims2005 = con_fims2005.cursor()
cur_funddb   = con_funddb.cursor()
cur_idx01    = con_idx01.cursor()

# cur.arraysize=100

# print SQL Result 1
tablespace_query='SELECT TBS_NM \
                                    , tbs_sz                                as "Total(MB)"\
                                    , tbs_alloc_sz                          as "Alloc(MB)"\
                                    , tbs_alloc_sz - tbs_free_sz            as "Used(MB)"\
                                    , round((tbs_alloc_sz-tbs_free_sz)/tbs_sz*100,1) AS "Used(%)"\
                                    , tbs_sz - (tbs_alloc_sz - tbs_free_sz) as "Free(MB)"\
                                    FROM\
                                    (\
                                            SELECT tbs_nm\
                                            , max(tbs_alloc_sz) as tbs_alloc_sz\
                                            , CASE WHEN max(tbs_sz)< max(tbs_alloc_sz) THEN max(tbs_alloc_sz)\
                                                ELSE max(tbs_sz) END as tbs_sz\
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

# Input Oracle tablespace_amount_used to Pandas DataFrame
fims2005_tablespace  = pd.read_sql(tablespace_query,con_fims2005,index_col=None)
funddb_tablespace = pd.read_sql(tablespace_query,con_funddb,index_col=None)
idx01_tablespace  = pd.read_sql(tablespace_query,con_idx01,index_col=None)

# add DB_NAME to DataFrame
fims2005_tablespace['DB_NAME']='FIMS2005'
funddb_tablespace['DB_NAME']='FUNDDB'
idx01_tablespace['DB_NAME']='IDX01'

# concat each tablespace_DataFrame
all_tablespace = pd.concat([fims2005_tablespace, funddb_tablespace, idx01_tablespace]).reset_index(drop=True)

# if amount_used > 95% record Data to list
all_tablespace_result = []
for i in range(len(all_tablespace)):
    if all_tablespace['Used(%)'][i]>95:
        all_tablespace_result.append(all_tablespace.iloc[i])
all_tablespace_result

# from Series to formatting text
all_tablespace_result2 = []
if len(all_tablespace_result)>=1:
    for i in range(len(all_tablespace_result)):
        all_tablespace_result2.append('{}. {}의 {} 사용률이 {}% 입니다'.format('{}'.format(i+1),all_tablespace_result[i]['DB_NAME'],
                                  all_tablespace_result[i]['TBS_NM'],all_tablespace_result[i]['Used(%)']))
all_tablespace_result3 = []
all_tablespace_result3 = '\n'.join(str(x) for x in all_tablespace_result2)


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
if all_tablespace_result:
    for i in range(len(all_tablespace_result)):
        datafile_query2.append(datafile_query.format(all_tablespace_result[i]['TBS_NM']))
            
# Input Oracle Data_file to Pandas DataFrame
datafile_fims2005=[]
datafile_funddb=[]
datafile_idx01=[]
if datafile_query2:
    for i in range(len(datafile_query2)):
        if all_tablespace_result[i]['DB_NAME']=='FIMS2005':
            datafile_fims2005.append(pd.read_sql(datafile_query2[i],con,index_col=None))
        elif all_tablespace_result[i]['DB_NAME']=='FUNDDB':
            datafile_funddb.append(pd.read_sql(datafile_query2[i],con2,index_col=None))
        else:
            datafile_idx01.append(pd.read_sql(datafile_query2[i],con3,index_col=None))

datafile_all = datafile_fims2005 + datafile_funddb + datafile_idx01

# from DataFrame to formatting text
datafile_all2=[]
if datafile_all:
    for i in range(len(datafile_all)):
        data=[]
        for j in range(len(datafile_all[i])):
            if datafile_all[i].iloc[j]['TABLESPACE_NAME'] == all_tablespace_result[i]['TBS_NM']:
                data.append('\n'.join(str(x) for x in list(datafile_all[i].iloc[j])).replace('\n',' '))
        datafile_all2.append(data)

datafile_all3=[]
if datafile_all2:
    for i in range(len(datafile_all)):
        datafile_all3.append('{}. {}'.format(i+1,all_tablespace_result[i]['TBS_NM']) + '\n'
                             + '\n'.join(str(x) for x in list(datafile_all[i].columns)).replace('\n',' ') +'\n'
                             + '\n'.join(str(x) for x in list(datafile_all2[i])))
datafile_all3_2= '\n'.join(str(x) for x in datafile_all3)

# classify Scale up or Scale out and show sql_query
new=[]
if datafile_all:
    for i in range(len(datafile_all)):
        if int(datafile_all[i]['FILE_NAME'][len(datafile_all[i])-1][8:10]) == 10:
            new.append(datafile_all[i]['FILE_NAME'][len(datafile_all[i])-1].replace(datafile_all[i]['FILE_NAME'][len(datafile_all[i])-1][8:10],
                       str('01'), 1).replace(datafile_all[i]['FILE_NAME'][len(datafile_all[i])-1][-6:-4], str(int(datafile_all[i]['FILE_NAME'][len(datafile_all[i])-1][-6:-4])+1)))
        else:
            new.append(datafile_all[i]['FILE_NAME'][len(datafile_all[i])-1].replace(datafile_all[i]['FILE_NAME'][len(datafile_all[i])-1][8:10], 
            str(int(datafile_all[i]['FILE_NAME'][len(datafile_all[i])-1][8:10])+1), 1).replace(datafile_all[i]['FILE_NAME'][len(datafile_all[i])-1][-6:-4],
            str(int(datafile_all[i]['FILE_NAME'][len(datafile_all[i])-1][-6:-4])+1)))

scale_up = []
scale_out = []
if datafile_all:
    for i in range(len(datafile_all)):
        for j in range(len(datafile_all[i])):
            if datafile_all[i]['MAXBYTES'][j] != 10000:
                scale_up.append("{}. ALTER DATABASE DATAFILE '{}' AUTOETEND ON NEXT 1000M MAXSIZE 10000M".format(i+1, datafile_all[i]['FILE_NAME'][j]))
        if len(datafile_all[i][datafile_all[i].MAXBYTES != 10000]) == 0:
            scale_out.append("{}. ALTER TABLESPACE {} ADD DATAFILE '{}' SIZE 1M AUTOEXTEND ON NEXT 1000M MAXSIZE 10000M".format(i+1, all_tablespace_result[i]['TBS_NM'],new[i]))

scale_up2 = '\n'.join(str(x) for x in scale_up)
scale_out2 = '\n'.join(str(x) for x in scale_out)

# combine all formatting text
if all_tablespace_result3:
    sns = all_tablespace_result3 + '\n\n' + datafile_all3_2 + '\n\n - Scale up query- \n' + scale_up2 + '\n\n - Scale out query- \n' + scale_out2         

# send massages
# if sns:
#    response = client.publish(
#              TopicArn='',
#              Message=sns
#              )

cur_fims2005.close()
cur_funddb.close()
cur_idx01.close()

con.close()
con2.close()
con3.close()

end_time = datetime.datetime.fromtimestamp(time.time())
print(end_time)
elapsed = (end_time-start_time)
print (" elapsed: ",elapsed," seconds")
print ("**********************************")
print ("")



