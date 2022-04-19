from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.window import Window

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
	df = df.withColumn(column, F.trim(F.col(column)))							#trim column
	return df

def check_empty_column(column):
	return (F.col(column).isNull()) | (F.col(column) == '') 					#check empty column

def upper_column(df, column):
	df = df.withColumn(column, F.upper(F.col(column)))							#upper column
	return df

def InvoiceNo_qa(df):
	df = df.withColumn("InvoiceNo_qa", (
							F.when((F.length(F.col('InvoiceNo')) != 6) &				#check string length 
										  (~(F.col('InvoiceNo').contains('C'))), "F")   #check startswith
					 		 .when( check_empty_column("InvoiceNo") , 'M')				#check empty column
				     		 .when(df.InvoiceNo.rlike("[a-zA-Z]+"), "A")				#check alphabet
					 	)
					 )
	return df

def StockCode_qa(df):
	df = df.withColumn('StockCode_qa', (
					   		F.when(F.length(F.col('StockCode')) != 5, "F" )
					 		 .when( check_empty_column("StockCode") , 'M')				#check empty column
							 .when(F.col('StockCode').rlike("[a-zA-Z]+"), "A")
	))
	return df

def Quantity_qa(df):
	df = df.withColumn('Quantity_qa',(
							F.when(check_empty_column("Quantity") , 'M')				#check empty column
							))
	return df

def InvoiceDate_qa(df):
	df = df.withColumn('InvoiceDate_qa',(
							F.when(check_empty_column("InvoiceDate") , 'M')))			#check empty column
	return df

def UnitPrice_qa(df):
	df = df.withColumn('UnitPrice_qa',(
							F.when(check_empty_column("UnitPrice") , 'M')))				#check empty column
	return df

def CustomerID_qa(df):
	df = df.withColumn('CustomerID_qa',(
							F.when(check_empty_column("CustomerID") , 'M')				#check empty column
							 .when(F.col('CustomerID').rlike("[a-zA-Z]+"), "A")
							 .when(F.length(F.col('CustomerID')) != 5, "F")
	))
	return df

def Country_qa(df):
	df = df.withColumn('Country_qa',(
							F.when(check_empty_column("Country") , 'M')					#check empty column
							.when(F.col('Country').rlike("[0-9]+"), "A")						   
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
	#df = trim_column(df, 'UnitPrice')
	df = df.withColumn('UnitPrice', (F.regexp_replace(F.col('UnitPrice'), ",", ".")))   #trocando padrão da moeda
	df = df.withColumn('UnitPrice', F.col('UnitPrice').cast('double'))				    #transformando de string para float
	df = df.withColumn('UnitPrice', (F.when(F.col('UnitPrice') < 0,0)					#UnitPrice precisa ser maior que zero
									  .otherwise(F.col('UnitPrice'))					#retorna a própria coluna
	))
	return df

def Quantity_tr(df):
	#df = trim_column(df, 'Quantity')
	df = df.withColumn('Quantity', F.col('Quantity').cast('integer'))					    #transformando string em int
	#df = df.withColumn('Quantity', F.when(F.col('Quantity') < 0, 0)					    #Quantity precisa ser maior que zero
    #									.otherwise(F.col('Quantity')))						#retorna a própria coluna
	return df 
	
def Sale_tr(df):
	df = df.withColumn('Sale', (
			F.when(F.col('Quantity') > 0, F.ceil((F.col('UnitPrice') * F.col('Quantity'))))	#cálculo e arrendondamento de sale
			 .otherwise(F.ceil((F.col('UnitPrice') * -F.col('Quantity'))))							
	))
	return df 

def InvoiceDate_tr(df):
	#df = trim_column(df, 'InvoiceDate')
	df = df.withColumn('InvoiceDate', 
					   F.to_timestamp(F.col('InvoiceDate'), 'd/M/yyyy HH:mm')			#string to timestamp
	)
	return df 

def StockCode_tr(df):
	#df = trim_column(df, 'StockCode')
	df = df.withColumn('StockCode',(
		   F.when(F.col('StockCode').startswith('gift_0001'), 'gift_0001')				#transform generic gift to gift
		   	.otherwise(F.col('StockCode'))
	))
	return df

def InvoiceNo_tr(df):
	df = upper_column(df, 'InvoiceNo')
	return df

def transformation_process(df):
	df = Quantity_tr(df)
	df = UnitPrice_tr(df)
	df = Sale_tr(df)
	df = InvoiceDate_tr(df)
	df = StockCode_tr(df)
	return df 

def pergunta_1(df):
	gifts = df.filter((F.col('StockCode').contains('gift')) & (F.col('Quantity') > 0))
	return (gifts.groupBy('StockCode')
				 .agg({'Quantity': 'sum'})
				 .withColumnRenamed('sum(Quantity)', 'Quantity')
				 .show()
			)

def pergunta_2(df):
	gifts = df.filter((F.col('StockCode').contains('gift')) & (F.col('Quantity') > 0) )
	return (gifts.groupby('StockCode', F.month('InvoiceDate'))
				 .agg({'Quantity': 'sum'})
				 .withColumnRenamed('sum(Quantity)', 'Quantity')
				 .withColumnRenamed('month(InvoiceDate)', 'Month')
				 .sort('Month', ascending = False)
				 .show()
		   )

def pergunta_3(df):
	samples = df.filter((F.col('StockCode') == 'S') & (F.col('StockCode') != 'PADS') & (F.col('Quantity') > 0))
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
	quantity_per_month = (df.groupBy("StockCode", F.month('InvoiceDate'))
						   .agg({'Quantity': 'sum'})
						   .withColumnRenamed('sum(Quantity)','Quantity')
						   .withColumnRenamed('month(InvoiceDate)', 'Month')
						   .sort('Quantity', ascending = False)
						)
						   
	windowsSpec = Window.partitionBy('Month').orderBy(F.col('Quantity').desc())
	quantity_per_month2 = quantity_per_month.withColumn("row_number", F.row_number().over(windowsSpec))
	return quantity_per_month2.filter(quantity_per_month2.row_number == 1).show() 

def pergunta_6(df):
	return (df.groupBy(F.hour('InvoiceDate'))
	 		  .agg({'Sale': 'sum'})
			  .withColumnRenamed('sum(Sale)','Sale')
			  .withColumnRenamed('hour(InvoiceDate)', 'Hour')
			  .sort('Sale', ascending = False)
			  .show()
			) 

def pergunta_7(df):
	return (df.groupBy(F.month('InvoiceDate'))
	 		  .agg({'Sale': 'sum'})
			  .withColumnRenamed('sum(Sale)','Sale')
			  .withColumnRenamed('month(InvoiceDate)', 'Month')
			  .sort('Sale', ascending = False)
			  .show()
			) 

def pergunta_8(df):
	sales_per_month = (df.groupBy(F.month('InvoiceDate'), "StockCode")
						 .agg({'Sale': 'sum'})
						 .withColumnRenamed('sum(Sale)', 'Sale')
						 .withColumnRenamed('month(InvoiceDate)', 'month'))

	windowsSpec      = Window.partitionBy('month').orderBy(F.col('Sale').desc())
	sales_per_month2 = sales_per_month.withColumn("row_number", F.row_number().over(windowsSpec))
	return sales_per_month2.filter(sales_per_month2.row_number == 1).show()

def pergunta_9(df):
	return (df.groupBy("Country")
	  	   .agg({'Sale':'count'})
	  	   .withColumnRenamed('count(Sale)', 'Sale')
	       .sort('Sale', ascending = False).show()
	)

def pergunta_10(df):
	manual = df.filter((F.col('StockCode').startswith('M')) & (F.col('StockCode') != 'PADS') & (F.col('Quantity') < 0))
	return (manual.groupBy("Country")
	  	   .agg({'Sale':'count'})
	  	   .withColumnRenamed('count(Sale)', 'Sale')
	       .sort('Sale', ascending = False).show()
	)

def pergunta_11(df):
	return (df.groupBy('InvoiceNo')
			  .agg({'UnitPrice': 'max'})
			  .withColumnRenamed('max(UnitPrice)', 'UnitPrice')
			  .sort('UnitPrice', ascending = False)
			  .show()
	)

def pergunta_12(df):
	return (df.groupBy('InvoiceNo')
			.agg({'UnitPrice': 'count'})
			.withColumnRenamed('count(UnitPrice)', 'Count')
			.sort('Count', ascending = False)
			.show()
	)

def pergunta_13(df):
	customerID_not_null = df.filter(F.col('CustomerID').isNotNull())
	return (customerID_not_null.groupBy('CustomerID')
							   .agg({'UnitPrice': 'count'})
							   .withColumnRenamed('count(UnitPrice)', 'Count')
							   .sort('Count', ascending = False)
							   .show()
	)

def pergunta_1_sql():
	spark.getOrCreate().sql(''' 
							SELECT SUM(Quantity) AS GIFT_QUANTITY
							FROM online_retail
							WHERE StockCode LIKE "%gift%" 
					''').show()

if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          #.schema(schema_online_retail)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/online-retail/online-retail.csv"))

	df.createOrReplaceTempView('online_retail')
	df = quality_process(df)
	df = transformation_process(df)
	sem_cancelamento = df.filter(~F.col('InvoiceNo').startswith('C'))

	#pergunta_1_sql()

	# print("Pergunta 1")
	# pergunta_1(df)

	# print("Pergunta 2")
	# pergunta_2(df)	

	# print("Pergunta 3")
	# pergunta_3(df)	
	
	# print("Pergunta 4")
	# pergunta_4(df)	

	# print('Pergunta 5')
	# pergunta_5(df)
	
	# print('Pergunta 6')
	# pergunta_6(df)	

	# print('Pergunta 7')
	# pergunta_7(df)	

	# print('Pergunta 8')
	# pergunta_8(df)	

	# print('Pergunta 9')
	# pergunta_9(df)	

	# print('Pergunta 10')
	# pergunta_10(df)	

	# print('Pergunta 11')
	# pergunta_11(sem_cancelamento)

	# print('Pergunta 12')
	# pergunta_12(sem_cancelamento)

	# print('Pergunta 13')
	# pergunta_13(sem_cancelamento)

