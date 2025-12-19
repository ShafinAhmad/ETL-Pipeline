from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame

def read_csv(fileName: Path, spark_session: SparkSession) -> SparkDataFrame:
    # [Poster_Link, Series_Title, Released_Year, Certificate, Runtime, Genre, IMDB_Rating, Meta_Score, Director, Star1, Star2, Star3, Star4, No_of_Voters, Gross, Number_of_Movies]
    result = spark_session.read.csv(fileName.absolute().as_posix(), header=True, inferSchema=True, multiLine=True, escape='"')
    print(result)
    return result
