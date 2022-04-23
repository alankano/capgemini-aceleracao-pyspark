from lib2to3.pgen2.pgen import DFAState
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.window import Window
 
schema_census_income = (StructType([
		StructField("age", IntegerType(), True),
		StructField("workclass", StringType(), True),
		StructField("fnlwgt", IntegerType(), True),
		StructField("education", StringType(), True),
		StructField("education-num", IntegerType(), True),
		StructField("marital-status", StringType(), True),
		StructField("occupation", StringType(), True),
		StructField("relationship", StringType(), True),
		StructField("race", StringType(), True),
		StructField("sex", StringType(), True),
		StructField("capital-gain", IntegerType(), True),
		StructField("capital-loss", IntegerType(), True),
		StructField("hours-per-week", IntegerType(), True),
		StructField("native-country", StringType(), True),
		StructField("income", StringType(), True),
]))

def trim_column(df,column):
	df = df.withColumn(column, (
		  F.trim(F.col(column))
	))
	return df

def trim_df(df):
	for c_name in df.columns:
		df = trim_column(df, c_name)
	return df

def age_qa(df):
	df = df.withColumn("age_qa", (
				F.when(F.col('age').isNull(), 'M')
				 .when(F.col('age') < 0, 'I')

	))
	return df

def age_tr(df):
	df = df.withColumn("age", (
		F.when(F.col('age') <0, 0)
	))
	return df

def workclass_qa(df):
	workclass_list = ['Private', 'Self-emp-not-inc', 'Self-emp-inc', 'Federal-gov', 'Local-gov', 'State-gov', 'Without-pay', 'Never-worked']
	df = df.withColumn('workclass_qa',(
		F.when(~F.col('workclass').isin(workclass_list), 'C')
	))
	return df

def workclass_tr(df):
	pass 

def df_qa(df):
	pass

def df_tr(df):
	pass

def pergunta_1(df):
	bigger_than_50 = df.filter(F.col('income') == '>50K')
	return (bigger_than_50.groupBy('workclass')
			  .agg({'income':'count'})
			  .withColumnRenamed('count(income)', 'income')
			  .sort('income', ascending = False)
			  .show()
			)

def pergunta_2(df):
	return (df.groupBy('race')
			  .agg(F.ceil(F.avg(F.col('hours-per-week'))))
			  .withColumnRenamed("ceil(avg(hours-per-week))", 'hours-per-week')
			  .sort('hours-per-week', ascending = False)
			  .show()
			)

def pergunta_3(df):
	# df_sex =  df.groupBy('sex').count()
	# return (df_sex.withColumn('percent', F.col('sex')/F.sum('sex').over(Window.partitionBy())*100).show())
	return (df.groupBy('sex')
			  .count()
			  .withColumn('percentage', F.round(F.col('count')/ F.sum('count')
			  .over(Window.partitionBy()),2))
			  .show()
	)

if __name__ == "__main__":
	sc    = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Census Income]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_census_income)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/census-income/census-income.csv"))
	
	df = trim_df(df)
	df.show()
	# df.printSchema()
	#pergunta_1(df)
	#pergunta_2(df)
	pergunta_3(df)