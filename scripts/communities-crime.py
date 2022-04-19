from ast import expr
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.window import Window

def trim_all_columns(df):
	for c_name in df.columns:
		df = df.withColumn(c_name, F.trim(F.col(c_name))) 
	return df

def missing_to_none(df):
	for c_name in df.columns:
		df = df.withColumn(c_name, (
						F.when(F.col(c_name) == '?', None)
						 .otherwise(F.col(c_name))
		))
	return df 

def pergunta_1(df):
	print('pergunta 1')
	return (df.groupBy('communityname')
			  .agg({'PolicOperBudg': 'max'})
			  .withColumnRenamed('max(PolicOperBudg)', 'PolicOperBudg')
			  .sort('PolicOperBudg', ascending =False).show()
			  )

def pergunta_2(df):
	print('pergunta 2')

	return (df.groupBy('communityname')
			  .agg({'ViolentCrimesPerPop': 'max'})
			  .withColumnRenamed('max(ViolentCrimesPerPop)', 'ViolentCrimesPerPop')
			  .sort('ViolentCrimesPerPop', ascending = False).show()
			  )

	# df = (df.withColumn('AuxViolentCrimesPerPop', 
	# 					F.round(F.col('population') * F.col('ViolentCrimesPerPop'),2)
	# 	))
	# return (df.groupBy('communityname')
	# 		  .agg({'AuxViolentCrimesPerPop': 'max'})
	# 		  .withColumnRenamed('max(AuxViolentCrimesPerPop)', 'AuxViolentCrimesPerPop')
	# 		  .sort('AuxViolentCrimesPerPop', ascending =False).show()
	# 		  )

def pergunta_3(df):
	print('pergunta 3')
	return (df.groupBy('communityname')
			  .agg({'population': 'max'})
			  .withColumnRenamed('max(population)', 'population')
			  .sort('population', ascending =False).show()
			  )

def pergunta_4(df):
	print('pergunta 4')
	return (df.groupBy('communityname')
			  .agg({'racepctblack': 'max'})
			  .withColumnRenamed('max(racepctblack)', 'racepctblack')
			  .sort('racepctblack', ascending =False).show()
			  )

def pergunta_5(df):
	print('pergunta 5')
	return (df.groupBy('communityname')
			  .agg({'pctWWage': 'max'})
			  .withColumnRenamed('max(pctWWage)', 'pctWWage')
			  .sort('pctWWage', ascending =False).show()
			  )

def pergunta_6(df):
	print('pergunta 6')
	return (df.groupBy('communityname')
			  .agg({'agePct16t24': 'max'})
			  .withColumnRenamed('max(agePct16t24)', 'agePct16t24')
			  .sort('agePct16t24', ascending =False).show()
			  )

def pergunta_7(df):
	print('pergunta 7')
	df = df.withColumn('PolicOperBudg',
						F.col('PolicOperBudg').cast(FloatType()))
	df = df.withColumn('ViolentCrimesPerPop',
						F.col('ViolentCrimesPerPop').cast(FloatType()))
	print('A correlação é:')
	print(df.corr('ViolentCrimesPerPop', 'PolicOperBudg'))

	# windowViolentCrimesPerPop = Window.partitionBy('PolicOperBudg').orderBy(F.col('ViolentCrimesPerPop').desc())
	# return (df.withColumn('row', F.row_number().over(windowViolentCrimesPerPop))
	# 		  .select('row', 'PolicOperBudg', 'ViolentCrimesPerPop')
	# 		  .filter(F.col('ViolentCrimesPerPop').isNotNull())
	# )

def pergunta_8(df):
	print('pergunta 8')
	df = df.withColumn('PolicOperBudg',
						F.col('PolicOperBudg').cast(FloatType()))
	df = df.withColumn('PctPolicWhite',
						F.col('PctPolicWhite').cast(FloatType()))
	print('A correlação é:')
	print(df.corr('PctPolicWhite', 'PolicOperBudg'))

def pergunta_9(df):
	print('pergunta 9')
	df = df.withColumn('PolicOperBudg',
						F.col('PolicOperBudg').cast(FloatType()))
	df = df.withColumn('population',
						F.col('population').cast(FloatType()))
	print('A correlação é:')
	print(df.corr('population', 'PolicOperBudg'))

def pergunta_10(df):
	print('pergunta 10')
	df = df.withColumn('ViolentCrimesPerPop',
						F.col('ViolentCrimesPerPop').cast(FloatType()))
	df = df.withColumn('population',
						F.col('population').cast(FloatType()))
	print('A correlação é:')
	print(df.corr('population', 'ViolentCrimesPerPop'))

def pergunta_11(df):
	print('pergunta 11')
	df = df.withColumn('ViolentCrimesPerPop',
						F.col('ViolentCrimesPerPop').cast(FloatType()))
	df = df.withColumn('medFamInc',
						F.col('medFamInc').cast(FloatType()))
	print('A correlação é:')
	print(df.corr('medFamInc', 'ViolentCrimesPerPop'))

def pergunta_12(df):
	print('pergunta 12')
	ethnicity_columns = ['racepctblack', 'racePctWhite', 'racePctAsian', 'racePctHisp']
	df = df.withColumn('max_value', (
					F.greatest(F.col('racepctblack'),
							   F.col('racePctWhite'),
							   F.col('racePctAsian'),
							   F.col('racePctHisp')    
							  ))
	)

	cond = "F.when" + ".when".join(["(F.col('" + c + "') == F.col('max_value'), F.lit('" + c + "'))" for c in ethnicity_columns])

	df = df.withColumn('ViolentCrimesPerEthnicity',(
						eval(cond)
	))

	return (df.select('communityname', 'ViolentCrimesPerEthnicity', 'ViolentCrimesPerPop' )
			  .sort('ViolentCrimesPerPop', ascending =False).show()
			  )

if __name__ == "__main__":
	sc    = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Communities & Crime]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          #.schema(schema_communities_crime)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/communities-crime/communities-crime.csv"))
	
	df = missing_to_none(df)
	
	
	#pergunta_1(df)
	#pergunta_2(df)
	#pergunta_3(df)
	#pergunta_4(df)
	#pergunta_5(df)
	#pergunta_6(df)
	#pergunta_7(df)
	#pergunta_8(df)
	#pergunta_9(df)
	#pergunta_10(df)
	#pergunta_11(df)
	pergunta_12(df)
	# arrumar erros mais fáceis como trim, upper and empty string 
	# transformar empty em null se possível 
	#  
	#communityname -> identificador da comunidade  
	#pergunta 1 
	
