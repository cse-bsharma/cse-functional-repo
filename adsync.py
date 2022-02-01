import adal,requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType
%%spark
import com.microsoft.spark.sqlanalytics.utils.Constants
import org.apache.spark.sql.SqlAnalyticsConnector._
val groupdf = spark.read.synapsesql("azsqldw.sys.database_principals")
groupdf.createOrReplaceTempView("dmvdata")
df=spark.sql("select name from dmvdata where type_desc='EXTERNAL_GROUP'")
def azure_ad_api(url):
    print(url)
    authurl = 'https://login.microsoftonline.com/3f2249c8-3b0e-4071-b8a9-3f187f634217/oauth2/v2.0/token'
    data = {
        'grant_type': 'client_credentials',
        'client_id': "20ebb1f6-adf4-459a-8f38-5940d578e9dc",
        'scope': 'https://graph.microsoft.com/.default',
        'client_secret': "c5v7Q~In8s8Vp6Giix4se00ZBPLh.Ua1RHGCM"
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
datacollect=df.toPandas()
suffix=1
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
    finaldf="adf_"+str(suffix)
    suffix += 1
    outdf = pd.DataFrame(data)
    newdf=outdf.assign(group_name=groupname)
    sparkDF=spark.createDataFrame(outdf) 
    sparkDF.createOrReplaceTempView(finaldf)