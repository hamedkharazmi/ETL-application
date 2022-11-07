import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as funcs
from pyspark.sql.functions import col, when, round, rank, mean
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
import mysql.connector
import pandas as pd
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

my_user = 'root'
my_password = 'Hamed_11351'
my_database='lahman2016'

def connect_to_db():
    return mysql.connector.connect(user=my_user, password=my_password, database=my_database)

spark = SparkSession.builder.appName("lahman2016").getOrCreate()


class AverageSalary():
    def __init__(self):
        self.cnx = connect_to_db()


    def extract(self): 
        salaries_data = pd.read_sql_query(
            '''select s.playerID, salary, f.yearID, POS
            from salaries as s join fielding as f 
            on s.playerId=f.playerId and f.yearId=s.yearId;''', self.cnx)  
        self.salaries_sc = spark.createDataFrame(salaries_data)
        return self.salaries_sc
    

    def transform(self):
        salaries_sc_2 = self.salaries_sc.groupby('yearID').pivot('POS').avg('salary')
        salaries_sc_3 = salaries_sc_2.withColumn('infield', (salaries_sc_2['1B'] + salaries_sc_2['2B'] + \
                                                            salaries_sc_2['3B'] + salaries_sc_2['SS'])/4)
        self.salaries_transformed = salaries_sc_3.select(col('yearID').alias('Year'), 
                                                        round(col('infield'),0).cast(IntegerType()).alias('Fielding'),  
                                                        round(col('P'),0).cast(IntegerType()).alias('Pitching')).orderBy("yearID")


    def load(self):
        salaries_transformed_pd = self.salaries_transformed.toPandas()
        salaries_transformed_pd['Pitching'] = salaries_transformed_pd.apply(lambda x: '{0:,}'.format(x['Pitching']), axis=1)
        salaries_transformed_pd['Fielding'] = salaries_transformed_pd.apply(lambda x: '{0:,}'.format(x['Fielding']), axis=1)
        salaries_transformed_pd.to_csv('AverageSalary.csv', index=False)
    
    def run_ETL(self):
        self.extract()
        self.transform()
        self.load()


def main():
    AverageSalary().run_ETL()    
    
if __name__ == '__main__':
    main()