import pandas as pd

def clean(input: pd.DataFrame) -> pd.DataFrame:
    # [Poster_Link, Series_Title, Released_Year, Certificate, Runtime, Genre, IMDB_Rating, Meta_Score, Director, Star1, Star2, Star3, Star4, No_of_Voters, Gross, Number_of_Movies]
    # Save everything but Poster_link, Certificate, Number_of_Movies, and Overview
    # Convert everything to the proper format 
    # Turn all nulls in box office to 0
    print(input.head(1))
    pass