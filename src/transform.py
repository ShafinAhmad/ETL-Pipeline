# import pandas as pd
import cpi
from logger import logger
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import regexp_count, array, floor, when, col, lit, udf
from pyspark.sql.types import IntegerType
# cpi.update()

def transform(input: SparkDataFrame) -> SparkDataFrame:
    
    # Add Genre Count column (handle list-type Genre from cleaning)

    df = input.withColumn("Genre", regexp_count(col("Genre"), lit(", ")) + 1)
        
    print("Genre Count Added")
    
    # Turn separate star columns into a single list column
    
    df = df.withColumn("Stars", array("Star1", "Star2", "Star3", "Star4"))
    df = df.drop(*["Star1", "Star2", "Star3", "Star4"])
    
    print("Stars Column Created")
    
    # Add Decade column
    df = df.withColumn("Decade", (floor(df["Released_Year"] / 10) * 10))
    
    print("Decade Column Added")
    
    # Add Movie Length Category
    df = df.withColumn("Movie_Length", when(df["Runtime"] <= 90, "Short")
                       .when((df["Runtime"] > 90) & (df["Runtime"] <= 120), "Regular")
                       .when((df["Runtime"] > 120) & (df["Runtime"] <= 180), "Long")
                       .otherwise("Extreme"))
        
    print("Movie Length Category Added")
    
    # Add Rating Category
    rating_bins = [0, 5, 7, 8, 9, 10]
    rating_labels = ["Bad", "Ok", "Good", "Very Good", "GoAT"]
    
    df = df.withColumn("Rating_Category", when(df["IMDB_Rating"] <= 5, "Bad")
                       .when((df["IMDB_Rating"] > 5) & (df["IMDB_Rating"] <= 7), "Ok")
                       .when((df["IMDB_Rating"] > 7) & (df["IMDB_Rating"] <= 8), "Good")
                       .when((df["IMDB_Rating"] > 8) & (df["IMDB_Rating"] <= 9), "Very Good")
                       .otherwise("GoAT"))
        
    print("Rating Category Added")
    
    # Add Popularity Score
    df = df.withColumn("Popularity_Score", df["No_of_Votes"] * df["IMDB_Rating"])
        
    print("Popularity Score Added")
    
    # Adjust Gross for Inflation to current dollars
    
    def _inflate(gross, year):
        if gross is None or year is None:
            return None
        try:
            inflated_value = cpi.inflate(gross, year)
            return int(inflated_value)
        except Exception as e:
            logger.error(f"Error inflating gross value {gross} from year {year}: {e}")
            return None 
    
    inflate_udf = udf(_inflate, IntegerType())
    df = df.withColumn("Gross_Inflation_Adjusted", inflate_udf(df["Gross"], df["Released_Year"]))
    # print("Gross Inflation Adjustment Done")
    logger.info(f"transform.completed rows={df.count()}")
    # df.toPandas().to_csv("transformed_sample.csv")
    print("Gross Inflation Adjustment Added")
    
    return df