#!/usr/bin/env python
# coding: utf-8

# In[8]:


import adal,requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType


# In[9]:


account_name = "XXXXXX"
container_name= "blob"
adls_path = 'abfss://%s@%s.dfs.core.windows.net/' % (container_name, account_name)


# In[10]:


get_ipython().run_cell_magic('spark', '', 'import com.microsoft.spark.sqlanalytics.utils.Constants\nimport org.apache.spark.sql.SqlAnalyticsConnector._\nval groupdf = spark.read.synapsesql("azsqldw.sys.database_principals")\ngroupdf.createOrReplaceTempView("dmvdata")')


# In[11]:


df=spark.sql("select name from dmvdata where type_desc='EXTERNAL_GROUP'")


# In[12]:


def azure_ad_api(url):
    print(url)
    authurl = 'https://login.microsoftonline.com/Tenant_nameXXXXXX/oauth2/v2.0/token'
    data = {
        'grant_type': 'client_credentials',
        'client_id': "APP_ID",
        'scope': 'https://graph.microsoft.com/.default',
        'client_secret': "cclient_secret"
             }       
    r = requests.post(authurl, data=data)
    token = r.json().get('access_token')
    headers = {
        'Content-Type' : 'application\json',
        'Authorization': 'Bearer {}'.format(token)
    }
    r = requests.get(url, headers=headers)
    result = r.json()
    return(result)


# In[13]:


datacollect=df.toPandas()
for  index,row in datacollect.iterrows():
    groupname=str(row[0])
    url_group_id="""https://graph.microsoft.com/v1.0/groups?$filter=startswith(displayName, '""" + groupname +  """')&$select=id,displayName"""
    result=azure_ad_api(url_group_id)
    groupinfo=list(list(result.items())[1])[1]
    groupid=str(list(groupinfo[0].values())[0])
    url_member_info ='https://graph.microsoft.com/v1.0/groups/'+ groupid + '/transitiveMembers/microsoft.graph.user?$select=userPrincipalName'
    result=azure_ad_api(url_member_info)
    print(result)
    data=list(list(result.items())[1])[1]
    outdf = pd.DataFrame(data)
    newdf=outdf.assign(group_name=groupname)
    df_sp = spark.createDataFrame(newdf)
    df_sp.write.format("parquet").mode("append")    .option("compression","snappy")    .save(adls_path +"/"+ "ad_role")

