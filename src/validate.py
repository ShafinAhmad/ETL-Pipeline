import pandas as pd

def validate(input: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # [Poster_Link, Series_Title, Released_Year, Certificate, Runtime, Genre, IMDB_Rating, Meta_Score, Director, Star1, Star2, Star3, Star4, No_of_Voters, Gross, Number_of_Movies]
    # Reject if anything but Poster_link, Certificate, Number_of_Movies is empty
    # Reject if movie name is duplicated
    pass