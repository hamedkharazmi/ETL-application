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
my_password = '***********'
my_database='lahman2016'

def connect_to_db():
    return mysql.connector.connect(user=my_user, password=my_password, database=my_database)

spark = SparkSession.builder.appName("lahman2016").getOrCreate()


class Rankings():
    def __init__(self):
        self.cnx = connect_to_db()

        
    def extract(self):
        team_data = pd.read_sql_query(
            '''select yearID, teamID, W, L, AB from Teams''', self.cnx)  
        self.team_sc = spark.createDataFrame(team_data).alias('team_sc')

        
    def transform(self):
        team_sc_wl = self.team_sc.withColumn('W/L', (col('W')/col('L')))
        windowSpec  = Window.partitionBy("yearID").orderBy("W/L")
        rank_sc = team_sc_wl.withColumn("rank",rank().over(windowSpec))
        best_team = rank_sc.filter(col('rank')==1).select('yearID', 'rank', 'teamID', 'AB')
        worst_rank_years = rank_sc.groupby('yearID').max('rank').select(col('yearID'),col('max(rank)').alias('rank'))
        worst_team = rank_sc.join(worst_rank_years, [worst_rank_years.yearID==rank_sc.yearID,\
                                                     worst_rank_years.rank==rank_sc.rank], how='inner' )
        worst_team = spark.createDataFrame(worst_team.toPandas().iloc[:,[0, 6, 1, 4]])
        self.rankings_transformed = best_team.union(worst_team).orderBy('yearID').select(col('teamID').alias('Team ID'), col('yearID').alias('Year'), col('rank').alias('Rank') ,col('AB').alias('At Bats'))
     
    
    def load(self):
        self.rankings_transformed.toPandas().to_csv('Rankings.csv', index=False)

    
    def run_ETL(self):
        self.extract()
        self.transform()
        self.load()


def main():
    Rankings().run_ETL()    
    
if __name__ == '__main__':
    main()
