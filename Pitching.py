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


class Pitching():
    def __init__(self):
        self.cnx = connect_to_db()

        
    def extract(self):
        post_players = pd.read_sql_query(
            """select  playerID, yearID, ER, IPouts, W, L, ERA
            from PitchingPost""", self.cnx)     
        reg_players = pd.read_sql_query(
            '''select  playerID, yearID, ER, IPouts, W, L, ERA 
            from Pitching''', self.cnx)   
        self.post_players_sc = spark.createDataFrame(post_players)
        self.reg_players_sc = spark.createDataFrame(reg_players)

    
    def transform(self):
        self.post_players_sc.createOrReplaceTempView("post_players_sc")
        spark.sql('''
            select playerID, yearID, avg(ERA) as reg_ERA, avg(ER) as ER, sum(IPouts) as IPouts, sum(W) as W, sum(L) as L, sum(W) + sum(L) as G
            from post_players_sc
            group by playerID, yearID''').createOrReplaceTempView("post_players_sc")

        self.reg_players_sc.createOrReplaceTempView("reg_players_sc")
        spark.sql('''
            select playerID, yearID, avg(ERA) as post_ERA, avg(ER) as ER, sum(IPouts) as IPouts, sum(W) as W, sum(L) as L, sum(W) + sum(L) as G
            from reg_players_sc
            group by playerID, yearID''').createOrReplaceTempView("reg_players_sc")

        spark.sql('''
            select reg.yearID, reg.playerID, (reg.ER+post.ER) as ER, (reg.IPouts+post.IPouts)/3 as inningsPitched,
                reg.W as reg_W, reg.L as reg_L, post.W as post_W, post.L as post_L, reg_ERA, post_ERA
            from post_players_sc post join reg_players_sc reg
            on post.playerID=reg.playerID and post.yearID=reg.yearID
            where post.G>0''').createOrReplaceTempView("all_players_sc")

        spark.sql('''
            select * from (select yearID, PLAYERID, rank() over(partition by yearID order by ER/inningsPitched) as Rank, 
                reg_W/(reg_W+reg_L) as RegSeasonWinLoss, post_W/(post_W+post_L) as PostSeasonWinLoss, reg_ERA, post_ERA
            from all_players_sc) as a 
            where Rank<=10''').createOrReplaceTempView('best_pitchers_per_year')

        pitching_transformed = spark.sql('''
            select playerID, yearID, round(reg_ERA,2) as RegualarERA, round(RegSeasonWinLoss,2) as RegSeasonWinLoss, 
                round(PostSeasonWinLoss,2) as PostSeasonWinLoss, round(post_ERA,2) as PostERA
            from best_pitchers_per_year order by 2 desc''')

        self.pitching_transformed = pitching_transformed.select(col('yearID').alias('Year'), 
                                                                col('playerID').alias('Player'), 
                                                                col('RegualarERA').alias('Regular Season ERA'), 
                                                                col('RegSeasonWinLoss').alias('Regular Season Win/Loss'), 
                                                                col('PostERA').alias('Post-season ERA'), 
                                                                col('PostSeasonWinLoss').alias('Post-season Win/Loss')).orderBy('yearID')

    def load(self):
        self.pitching_transformed.toPandas().to_csv('Pitching.csv', index=False)

    
    def run_ETL(self):
        self.extract()
        self.transform()
        self.load()


def main():
    Pitching().run_ETL()    
    
if __name__ == '__main__':
    main()
