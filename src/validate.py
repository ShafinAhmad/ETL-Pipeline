import pandas as pd
from logger import logger
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import lit

def validate(input: SparkDataFrame) -> tuple[SparkDataFrame, SparkDataFrame]:
    # [Poster_Link, Series_Title, Released_Year, Certificate, Runtime, Genre, IMDB_Rating, Meta_Score, Director, Star1, Star2, Star3, Star4, No_of_Voters, Gross, Number_of_Movies]
    optional_columns: set[str] = {"Poster_Link", "Certificate", "Number_of_Movies"}
    required_columns: list[str] = [a for a in input.columns if a not in optional_columns]
    # Reject if anything but Poster_link, Certificate, Number_of_Movies is empty
    notNullData = input.dropna(subset=required_columns)
    rejectedNullData = input.subtract(notNullData).withColumn("Rejection_Reason", lit("Missing required fields"))
    
    # Reject if movie name is duplicated
    notDupeData = notNullData.dropDuplicates(["Series_Title"])
    rejectedDupeData = notNullData.subtract(notDupeData).withColumn("Rejection_Reason", lit("Duplicate movie title"))
    
    # combine rejected data
    rejected_data = rejectedNullData.unionByName(rejectedDupeData)
    
    logger.info(f"validate.completed valid={notDupeData.count()} rejected={rejected_data.count()}")
    
    return notDupeData, rejected_data