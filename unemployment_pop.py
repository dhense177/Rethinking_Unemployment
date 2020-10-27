import pandas as pd
import numpy as np
import pickle, os, csv, requests, zipfile, io, re
import pyspark.sql.functions as f
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkContext





if __name__=='__main__':
    filepath = '/home/dhense/PublicData/Economic_analysis/Data/Unemployment_Analysis/'
    pickle_path = '/home/dhense/PublicData/Economic_analysis/intermediate_files/'

    pop_pickle = 'pop.pickle'
    pop_pickle2 = 'pop2.pickle'

    sc = SparkContext("local", "Population Parser")
    sqlContext = SQLContext(sc)

    if not os.path.isfile(pickle_path+pop_pickle2):

        spark_df = sqlContext.read.format('com.databricks.spark.csv').options(header='false', inferschema='true').load(filepath+'us.1990_2018.singleages.adjusted.txt')

        dfs = spark_df.withColumn('Year',f.substring(f.col('_c0'),0,4)).withColumn('State',f.substring(f.col('_c0'),5,2)).withColumn('State_FIPS',f.substring(f.col('_c0'),7,2)).withColumn('County_FIPS',f.substring(f.col('_c0'),9,3)).withColumn('Registry',f.substring(f.col('_c0'),12,2)).withColumn('Race',f.substring(f.col('_c0'),14,1)).withColumn('Origin',f.substring(f.col('_c0'),15,1)).withColumn('Sex',f.substring(f.col('_c0'),16,1)).withColumn('Age',f.substring(f.col('_c0'),17,2)).withColumn('Population',f.substring(f.col('_c0'),19,8))

        dfs = dfs.withColumn("Population", dfs["Population"].cast(IntegerType()))

        grouped_dfs = dfs.groupby('Year','Race','Sex','Age').sum('Population')
        df_pandas = grouped_dfs.toPandas()
        df_pandas = df_pandas.rename(columns={'sum(Population)':'Population'})

        grouped_dfs2 = dfs.groupby('Year','State_FIPS','County_FIPS').sum('Population')
        df_pandas2 = grouped_dfs2.toPandas()
        df_pandas2 = df_pandas2.rename(columns={'sum(Population)':'Population'})

        print("...saving pickle")
        tmp = open(pickle_path+pop_pickle,'wb')
        pickle.dump(df_pandas,tmp)
        tmp.close()
        print("...saving pickle")
        tmp = open(pickle_path+pop_pickle2,'wb')
        pickle.dump(df_pandas2,tmp)
        tmp.close()
    else:
        print("...loading pickle")
        tmp = open(pickle_path+pop_pickle,'rb')
        df = pickle.load(tmp)
        tmp.close()
        print("...loading pickle")
        tmp = open(pickle_path+pop_pickle2,'rb')
        df2 = pickle.load(tmp)
        tmp.close()
