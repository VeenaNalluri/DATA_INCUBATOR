from __future__ import print_function

import sys
import findspark
findspark.init()


from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import*
from pyspark.sql.types import StringType

import matplotlib
import matplotlib.pyplot as plt
#-----------------------------------
if __name__ == "__main__":

    # create an instance of a SparkSession as spark
    spark = SparkSession\
        .builder\
        .appName("first")\
        .getOrCreate()


inputPath = "C:/spark-2.2.1-bin-hadoop2.7/bin/Air_Quality.csv"

# create SparkContext as sc
sc = spark.sparkContext
sqlContext=SQLContext(sc)

# create RDD from a text file
# Loading the text file from input directory
df = sqlContext.read.load('C:/spark-2.2.1-bin-hadoop2.7/bin/Air_Quality.csv', 
                          format='com.databricks.spark.csv', 
                          header='true', 
                          inferSchema='true')
#To know the datatype of each parameter in a table
print(df.dtypes)
#loading the dataframe with specific columns and applying specific filter
df1=df.select('X','Y','Address','Parameter','HRV','Results').filter(df.Parameter=='Methylene Chloride').filter(df.Results>20)
df2=df.select('X','Y','Address','Parameter','HRV','Results').filter(df.Parameter=='Trichloroethene').filter(df.Results>3)
print("The count is %d" % df1.count())
print("The count is %d" % df2.count())
print("The filtered table is")
#loads the first 40 columns of the dataframe which satisfies all the filters
print(df1.show(40))
print(df2.show(40))
#df.describe gives you the count, mean, std deviation, min and max values in a data frame
df3=df1.describe()
print(df3.show())
df4=df2.describe()
print(df4.show())
#Converting the Spark DataFrame to Pandas DataFrame
pdf=df1.toPandas()
#Providing the specifications of the plot
pdf.plot(kind='bar',y='Results',colormap='Blues_r',alpha= 1)
plt.xlabel('Count of Methylene Chloride Values greater than HRV')
plt.ylabel('Result Values')
plt.title('Methylene Chloride Values greater than HRV')

pdf=df2.toPandas()
pdf.plot(kind='bar',y='Results',colormap='Blues_r',alpha= 1)
plt.xlabel('Count of Trichloroethene Values greater than HRV')
plt.ylabel('Result Values')
plt.title('Trichloroethene Values greater than HRV')
#Printing the plot
print(plt.show())

# done!
spark.stop()

