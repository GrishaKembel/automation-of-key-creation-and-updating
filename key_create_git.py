#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyhive import hive
import pandas as pd

a = 0.05 
connection = hive.connect(host="host", username="username",auth=None)
cursor = connection.cursor()

sourcetable = ['sendbox.vd_adhoc_2165']
adhoc = ['analytics_adhoc2165']
lst = list(zip(sourcetable, adhoc))

connection.cursor().execute('''set mapreduce.job.queuename=qvant.A1''')
connection.cursor().execute('''set hive.exec.dynamic.partition.mode=nonstrict''')
connection.cursor().execute('''set hive.exec.dynamic.partition=true''')

for arg in range(0, len(lst)):
    query= """INSERT INTO sendbox.mac_tag PARTITION (tag_name) 
select distinct lcase(regexp_replace(mac,':','-')), concat('R',cast(abs(cast(hash('{}') as bigint)) as varchar(100)))
from {}
where length(regexp_replace(mac,':','-'))=17""".format(lst[arg][1], lst[arg][0])
    cursor.execute(query)
    df = pd.DataFrame()
    df = pd.read_sql('''
                    select concat('R',cast(abs(cast(hash('{}') as bigint)) as varchar(100))),' : {}'
                '''.format(lst[arg][1], lst[arg][1]),connection)
    print(df)


# In[ ]:




