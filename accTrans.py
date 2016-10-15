from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName('Process Data')
sc = SparkContext(conf=conf)
spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .config("spark.some.config.option", "some-value")\
    .getOrCreate()

text  = spark.read.csv("accepted.csv", header=True, mode="DROPMALFORMED")

def detCr(y):
    try:
        if(int(y)>725):
            return 'A'
        elif(int(y)>600):
            return 'B'
        elif(int(y)>475):
            return 'C'
        elif(int(y)>350):
            return 'D'
        elif(int(y)>225):
            return 'E'
        elif(int(y)>100):
            return 'F'
        else:
            return 'G'
    except:
        return 0

def prcessFucnt(x):
    ep ='0'
    if(x[1]==''):
        return ''
    if x[2]=="n/a":
        return ''
    lst = x[2].split()
    if(lst[0]=='<'):
        if(lst[1]=='1'):
            ep = '0'
        else:
            ep = lst[1]
    else:
        ep=lst[0]
    ep = ep.strip('+')
    zp = x[3].strip('xx')
    dti = x[5].strip('%')
    return x[0],x[1],dti,zp,x[4],ep,'1'

text = text.rdd
text = text.map(prcessFucnt).filter(lambda l:l!='').collect()
df = sc.parallelize(text).toDF()
df.write.csv('acceptedProc')