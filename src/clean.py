# import pandas as pd
from logger import logger
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import regexp_replace

def clean(input: SparkDataFrame) -> SparkDataFrame:
    # [Poster_Link, Series_Title, Released_Year, Certificate, Runtime, Genre, IMDB_Rating, Overview, Meta_score, Director, Star1, Star2, Star3, Star4, No_of_Votes, Gross, Number_of_Movies]
    # Save everything but Poster_link, Certificate, Number_of_Movies, and Overview
    # Convert everything to the proper format
    # [Series_Title, Released_Year, Runtime, Genre, IMDB_Rating, Meta_Score, Director, Star1, Star2, Star3, Star4, No_of_Voters, Gross]
    # [String, Int, Int, String, Float, Int, String, String, String, String, String, Int, Int]
    # Turn all nulls in box office to 0
    
    df = input.drop(*["Poster_Link", "Certificate", "Overview", "Number_of_Movies"])
    # df.toPandas().to_csv("cleaned_input.csv")
    df = df.withColumn("Released_Year", df["Released_Year"].cast("int"))
    df = df.withColumn("Runtime", (regexp_replace(df["Runtime"], "min", "")).cast("int"))
    df = df.withColumn("IMDB_Rating", df["IMDB_Rating"].cast("float"))
    df = df.withColumn("Meta_score", df["Meta_score"].cast("int"))
    df = df.withColumn("No_of_Votes", df["No_of_Votes"].cast("int"))
    df = df.withColumn("Gross", (regexp_replace(df["Gross"], ",", "")).cast("int"))
    
    string_columns = ["Series_Title", "Director", "Star1", "Star2", "Star3", "Star4"]
    for col in string_columns:
        df = df.withColumn(col, df[col].cast("string"))
        
    logger.info(f"clean.completed rows={df.count()}")
    # df.toPandas().to_csv("cleaned_output.csv")
    return df