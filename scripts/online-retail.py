from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

def trim_column(df,column):
	df = df.withColumn(column, (
		  F.trim(column)
	))
	return df

def check_empty_column(column):
	return (F.col(column).isNull()) | (F.col(column) == '') 


def InvoiceNo_qa(df):
	df = trim_column(df, 'InvoiceNo')
	df = df.withColumn("InvoiceNo_qa", (
							F.when((F.length(df.InvoiceNo) != 6) &
											(~df.InvoiceNo.startswith('c')), "F")
					 		 .when( check_empty_column("InvoiceNo") , 'M')
				     		 .when(df.InvoiceNo.rlike("[a-zA-Z]+"), "A")
					 	)
					 )
	return df

def StockCode_qa(df):
	df = trim_column(df, 'StockCode')
	df = df.withColumn('StockCode_qa', (
					   		F.when(F.length(df.StockCode) != 5, "F" )
					 		 .when( check_empty_column("StockCode") , 'M')
							 .when(df.StockCode.rlike("[a-zA-Z]+"), "A")
	))
	return df

def Quantity_qa(df):
	df = trim_column(df, 'Quantity')
	df = df.withColumn('Quantity_qa',(
							F.when(check_empty_column("Quantity") , 'M')))
	return df

def InvoiceDate_qa(df):
	df = trim_column(df, 'InvoiceDate')
	df = df.withColumn('InvoiceDate_qa',(
							F.when(check_empty_column("InvoiceDate") , 'M')))
	return df

def InvoiceDate_tr(df):
	df = df.withColumn('InvoiceDate',(
						F.to_timestamp( df.InvoiceDate)
	))
	return df

def UnitPrice_qa(df):
	df = trim_column(df, 'UnitPrice')
	df = df.withColumn('UnitPrice_qa',(
							F.when(check_empty_column("UnitPrice") , 'M')))
	return df

def CustomerID_qa(df):
	df = trim_column(df, 'CustomerID')
	df = df.withColumn('CustomerID_qa',(
							F.when(check_empty_column("CustomerID") , 'M')
							 .when(df.CustomerID.rlike("[a-zA-Z]+"), "A")
							 .when(F.length(df.CustomerID) != 5, "F")
	))
	return df

def Country_qa(df):
	df = trim_column(df, 'Country')
	df = df.withColumn('Country_qa',(
							F.when(check_empty_column("Country") , 'M')
							.when(df.Country.rlike("[0-9]+"), "A")						   
	))
	return df



if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          #.schema(schema_online_retail)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/online-retail/online-retail.csv"))

	#df.show(5)
	#df.printSchema()
	#df = InvoiceNo_qa(df)
	#df = StockCode_qa(df)
	#df = Quantity_qa(df)
	#df = InvoiceDate_qa(df)
	#df = UnitPrice_qa(df)
	#df = CustomerID_qa(df)
	#df = Country_qa(df)

	df = df.withColumn("Sale", (
		   F.when((~df.InvoiceNo.startswith('c')),
		   		  (df.UnitPrice * df.Quantity)
		   )))

	df = InvoiceDate_tr(df)
	
	print("Pergunta 1")
	df.filter(df.StockCode == 'gift_0001').show(5)
	print("Pergunta 2")
	df.groupby(F.month(df.InvoiceDate)).agg(sum('Sale').alias('sum_sale')).where(df.StockCode == 'gift_0001').show(5)
	df.show(5)
	
	#df.filter(df.StockCode == "0001").show(5)
	print("rodou ate o final")