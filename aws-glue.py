import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SQLContext

reload(sys)
sys.setdefaultencoding('utf8')

args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])
 
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
sqlContext = SQLContext(sc)

#Source Credentials
replicaHost = "Enter Source Host"
replicaUser = "username"
replicaPass = "password"

#Target Credentials
redshiftHost = "Enter Target Host"
redshiftUser = "username"
redshiftPass = "password"

query= "(SELECT table_name FROM information_schema.tables where table_schema='Database_Name' ) as mainQuery"
datasource0 = sqlContext.read.format("jdbc").option("url", replicaHost ).option("driver", "com.mysql.jdbc.Driver").option("dbtable", query).option("user", replicaUser).option("password", replicaPass).load()

def getQuery(name):
    columnQuery = "(SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.columns WHERE table_schema='Database_Name' AND table_name='%s') as columnQuery" %(name)
    dataColumnQuery = sqlContext.read.format("jdbc").option("url", replicaHost).option("driver", "com.mysql.jdbc.Driver").option("dbtable", columnQuery).option("charset", "utf8").option("user", replicaUser).option("password", replicaPass).load()
    selectQuery = "(SELECT "
    for record in dataColumnQuery.rdd.map(lambda q: q).collect():
        if ("longtext" == record.DATA_TYPE):
            maxQuery = "(SELECT max(character_length(%s)) as maxChars FROM %s) as maxQuery" %(record.COLUMN_NAME, name) 
            dataMaxQuery = sqlContext.read.format("jdbc").option("url", replicaHost).option("driver", "com.mysql.jdbc.Driver").option("dbtable", maxQuery).option("charset", "utf8").option("user", replicaUser).option("password", replicaPass).load()
            
            if( dataMaxQuery.rdd.map(lambda w: w.maxChars).collect()[0] > 65535):
                print record.COLUMN_NAME + " Removed for exceeding limit(65535)"
                continue
            else :
                selectQuery += (record.COLUMN_NAME) + ", " 
        elif (record.COLUMN_NAME == "PasswordHash"):
            print record.COLUMN_NAME + " Removed for containing password Hash"
            continue
        else:
            selectQuery += (record.COLUMN_NAME) + ", "  

    selectQuery = selectQuery.rstrip(", ") + " FROM %s ) as %s " %(name, name)
    return selectQuery

for name in datasource0.rdd.map(lambda p: p.table_name).collect():
    selectQuery = getQuery(name)
    print "table name " + ": " + selectQuery

    datasource1 = sqlContext.read.format("jdbc").option("url", replicaHost).option("driver", "com.mysql.jdbc.Driver").option("dbtable", selectQuery).option("charset", "utf8").option("user", replicaUser).option("password", replicaPass).load()
    datasource1.write.format("com.databricks.spark.redshift").option("url", redshiftHost ).option("dbtable", name).option("charset", "utf8").option("forward_spark_s3_credentials", "true").option("user", redshiftUser).option("password", redshiftPass).option("tempdir", args["TempDir"]).mode("overwrite").save()

job.commit()
