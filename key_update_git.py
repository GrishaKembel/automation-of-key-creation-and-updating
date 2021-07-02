#!/usr/bin/env python
# coding: utf-8

# In[18]:


from pyhive import hive
import pandas as pd

a = 0.05 
connection = hive.connect(host="host", username="username",auth=None)
cursor = connection.cursor()

sourcetable = ['sendbox.vd_adhoc_test']
key = ['R563916239']
lst = list(zip(sourcetable, key))

connection.cursor().execute('''set mapreduce.job.queuename=qvant.A1''')
connection.cursor().execute('''set hive.exec.dynamic.partition.mode=nonstrict''')
connection.cursor().execute('''set hive.exec.dynamic.partition=true''')

for arg in range(0, len(lst)):
    request = """INSERT INTO sendbox.mac_tag PARTITION (tag_name) 
select distinct lcase(regexp_replace(mac,':','-')), '{}' 
from {}
where length(regexp_replace(mac,':','-'))=17""".format(lst[arg][1], lst[arg][0])
    cursor.execute(request)


# In[ ]:




