from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

#schema_online_retail = (StructType([ 
#    StructField("InvoiceNo",StringType(),True), 
#    StructField("StockCode",IntegerType(),True), 
#    StructField("Description",StringType(),True), 
#    StructField("Quantity", IntegerType(), True), 
#    StructField("InvoiceDate", StringType(), True), 
#    StructField("UnitPrice", FloatType(), True),
#    StructField("CustomerID", IntegerType(), True ),
#    StructField("Country", StringType(), True)
#                   ])
#                )

def trim_column(df,column):
	df = df.withColumn(column, ( F.trim(column)))
	return df

def check_empty_column(column):
	return (F.col(column).isNull()) | (F.col(column) == '') 

def InvoiceNo_qa(df):
	df = trim_column(df, 'InvoiceNo')
	df = df.withColumn("InvoiceNo_qa", (
							F.when((F.length(df.InvoiceNo) != 6) &
											(~df.InvoiceNo.contains('c')), "F")
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
							F.when(check_empty_column("Quantity") , 'M')
							.when(df.Quantity < 0, 'I')
							))
	return df

def InvoiceDate_qa(df):
	df = trim_column(df, 'InvoiceDate')
	df = df.withColumn('InvoiceDate_qa',(
							F.when(check_empty_column("InvoiceDate") , 'M')))
	return df

def InvoiceDate_tr(df):
	df = df.withColumn('InvoiceDate',(F.to_timestamp(df.InvoiceDate, 'dd/MM/yyyy HH:MM')))
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

def quality_process(df):
	df = InvoiceNo_qa(df)
	df = StockCode_qa(df)
	df = Quantity_qa(df)
	df = InvoiceDate_qa(df)
	df = InvoiceDate_tr(df)
	df = UnitPrice_qa(df)
	df = CustomerID_qa(df)
	df = Country_qa(df)
	return df


def UnitPrice_tr(df):
	df = df.withColumn('UnitPrice', (F.regexp_replace(df.UnitPrice, ",", ".")))
	df = df.withColumn('UnitPrice', df.UnitPrice.cast('double'))
	df = df.withColumn('UnitPrice', (F.when(df.UnitPrice < 0,0)
									  .otherwise(df.UnitPrice)
	))
	return df

def Quantity_tr(df):
	df = df.withColumn('Quantity', df.Quantity.cast('integer'))
	df = df.withColumn('Quantity', F.when(df.Quantity < 0, 0)
									.otherwise(df.Quantity)
	)
	return df 
	
def Sale_tr(df):
	df = df.withColumn('Sale', (
			F.ceil((df.UnitPrice * df.Quantity).cast('double'))		
	))
	return df 

def InvoiceDate_tr(df):
	df = df.withColumn('InvoiceDate', 
					   F.to_timestamp(df.InvoiceDate, 'd/M/yyyy HH:mm')
	)
	return df 

def StockCode_tr(df):
	df = df.withColumn('StockCode',(
		   F.when(F.col('StockCode').startswith('gift'), 'gift')
		   	.otherwise(F.col('StockCode'))
	))
	return df

def transformation_process(df):
	df = Quantity_tr(df)
	df = UnitPrice_tr(df)
	df = Sale_tr(df)
	df = InvoiceDate_tr(df)
	df = StockCode_tr(df)
	return df 


def pergunta_1(df):
	gifts = df.filter(F.col('StockCode').contains('gift'))
	return (gifts.groupBy('StockCode')
				 .agg({'Quantity': 'sum'})
				 .withColumnRenamed('sum(Quantity)', 'Quantity')
				 .show()
			)

def pergunta_2(df):
	gifts = df.filter(F.col('StockCode').contains('gift'))
	return (gifts.groupby('StockCode', F.month('InvoiceDate'))
				 .agg({'Quantity': 'sum'})
				 .withColumnRenamed('sum(Quantity)', 'Quantity')
				 .withColumnRenamed('month(InvoiceDate)', 'Month')
				 .sort('Month', ascending = False)
				 .show()
		   )

def pergunta_3(df):
	samples = df.filter((F.col('StockCode').endswith('S')) & (F.col('StockCode') != 'PADS'))
	return (samples.groupBy('StockCode')
				 .agg({'Quantity': 'sum'})
				 .withColumnRenamed('sum(Quantity)', 'Quantity')
				 .show()
			)

def pergunta_4(df):
	return (df.groupBy("StockCode")
	 		  .agg({'Quantity': 'sum'})
			  .withColumnRenamed('sum(Quantity)','Quantity')
			  .sort('Quantity', ascending = False)
			  .show()
			) 

def pergunta_5(df):
	return (df.groupBy("StockCode", F.month('InvoiceDate'))
	 		  .agg({'Quantity': 'sum'})
			  .withColumnRenamed('sum(Quantity)','Quantity')
			  .withColumnRenamed('month(InvoiceDate)', 'Month')
			  .sort('Quantity', ascending = False)
			  .show()
			) 

def pergunta_6(df):
	return (df.groupBy("StockCode", F.hour('InvoiceDate'))
	 		  .agg({'Sale': 'sum'})
			  .withColumnRenamed('sum(Sale)','Sale')
			  .withColumnRenamed('hour(InvoiceDate)', 'Hour')
			  .sort('Sale', ascending = False)
			  .show()
			) 

def pergunta_7(df):
	return (df.groupBy("StockCode", F.month('InvoiceDate'))
	 		  .agg({'Sale': 'sum'})
			  .withColumnRenamed('sum(Sale)','Sale')
			  .withColumnRenamed('month(InvoiceDate)', 'Month')
			  .sort('Sale', ascending = False)
			  .show()
			) 

if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          #.schema(schema_online_retail)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/online-retail/online-retail.csv"))

	df.show(5)
	#df = quality_process(df)
	df = transformation_process(df)
	print("Pergunta 1")
	pergunta_1(df)
	#df.show(5)

	print("Pergunta 2")
	pergunta_2(df)	

	print("Pergunta 3")
	pergunta_3(df)	

	print("Pergunta 4")
	pergunta_4(df)	

	print('Pergunta 5')
	pergunta_5(df)
	
	print('Pergunta 6')
	pergunta_6(df)	

	print('Pergunta 7')
	pergunta_7(df)	
	#print("rodou ate o final")