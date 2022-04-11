from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
 
def trim_column(df,column):
	df = df.withColumn(column, (
		  F.trim(column)
	))
	return df

def df_qa(df):
	pass

def df_tr(df):
	pass

def invoiceNo_qa(df):
	df = trim_column(df, 'InvoiceNo')
	df = df.withColumn("InvoiceNo_qa", (
					F.when( (F.length(df.InvoiceNo) != 6) &
							(~df.InvoiceNo.startswith('c')), "F")
					 .when((df.InvoiceNo.isNull()) | 
						   (df.InvoiceNo == ''), 'M')
				     .when(df.InvoiceNo.rlike("[a-zA-Z]+"), "A")
					 	)
					 )
	return df

def InvoiceNo_tr(df):
	pass

def pergunta_1(df):
	df = trim_column(df, "InvoiceNo")
	pergunta_1_qa(df)
	pergunta_1_tr(df)
	pass 

if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Census Income]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          #.schema(schema_census_income)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/online-retail/online-retail.csv"))
	#df.show()

	df = invoiceNo_qa(df)
	#df.filter(df.InvoiceNo.startswith('c')).show(5)