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


class HallofFameAllStarPitchers():
    def __init__(self):
        self.cnx = connect_to_db()

    
    def extract(self):
        self.hof_data = pd.read_sql_query(
            '''select playerID, yearid 
            from HallOfFame 
            where inducted="Y"''', self.cnx)  

        self.pitchers_data = pd.read_sql_query(
            '''select playerID, ERA
                from Pitching''',self.cnx) 

        self.allstar_data = pd.read_sql_query(
            '''select playerID, yearID as allstar_years, gameNum
            from AllStarFull 
            where GP=1''', self.cnx) 
        

    def transform(self):
        hof_sc = spark.createDataFrame(self.hof_data).alias('hof_sc')
        pitchers_sc = spark.createDataFrame(self.pitchers_data).alias('pitchers_sc')
        allstar_sc = spark.createDataFrame(self.allstar_data).alias('allstar_sc')

        pitchers_sc = pitchers_sc.groupby('playerID').avg('ERA')\
            .select(col('playerID'), round(col('avg(ERA)'),2).alias('ERA')).alias('pitchers_sc')

        hof_pitchers = pitchers_sc.join(hof_sc, hof_sc.playerID==pitchers_sc.playerID, how='inner')\
            .select(col('pitchers_sc.playerID'), col('yearid'), col('ERA')).alias('hof_pitchers')

        allstar_agg_sc = allstar_sc.withColumn('gameNum', when(col('gameNum') == 0, 1).otherwise(col('gameNum')))\
            .groupby('playerID').sum('gameNum').select(col('playerID'), col('sum(gameNum)').alias('total_games')).\
            alias('allstar_pitcher_num_games')
        
        self.hof_allstar_pitchers_transformed = hof_pitchers.join(allstar_agg_sc, allstar_agg_sc.playerID==hof_pitchers.playerID,how='inner').\
            select(col('hof_pitchers.playerID').alias('Player'),col('ERA'), col('total_games').alias(' # All Star Appearances'),
            col('yearid').alias('Hall of Fame Induction Year'))
        
    def load(self):
        self.hof_allstar_pitchers_transformed.toPandas().to_csv('HallofFameAllStarPitchers.csv', index=False)


    def run_ETL(self):
        self.extract()
        self.transform()
        self.load()


def main():
    HallofFameAllStarPitchers().run_ETL()    
    
if __name__ == '__main__':
    main()