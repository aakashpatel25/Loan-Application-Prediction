from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName('Process Data')
sc = SparkContext(conf=conf)
spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .config("spark.some.config.option", "some-value")\
    .getOrCreate()

#text = sc.textFile('reject.csv')

text  = spark.read.csv("reject.csv", header=True, mode="DROPMALFORMED")
#text.take(5)
data = text.select('Amount Requested','Risk_Score','Debt-To-Income Ratio','Zip Code','State','Employment Length')
data = data.withColumnRenamed("Amount Requested","amount").withColumnRenamed("Debt-To-Income Ratio","dti")
data  = data.withColumnRenamed('Zip Code','zipc').withColumnRenamed('State','st')
data = data.withColumnRenamed('Employment Length','empLen').withColumnRenamed('Risk_Score','credit')

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
    credit = detCr(x[1])
    if credit is 0:
        return ''
    if x[5]=="n/a":
        return ''
    lst = x[5].split()
    if(lst[0]=='<'):
        if(lst[1]=='1'):
            ep = '0'
        else:
            ep = lst[1]
    else:
        ep=lst[0]
    ep = ep.strip('+')
    zp = x[3].strip('xx')
    dti = x[2].strip('%')
    return x[0],credit,dti,zp,x[4],ep,'0'

text = data.rdd
text = text.map(prcessFucnt).filter(lambda l:l!='').collect()
df = sc.parallelize(text).toDF()
df.write.csv('processedReject.csv')