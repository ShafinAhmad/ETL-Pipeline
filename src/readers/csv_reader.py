import pandas as pd
from pathlib import Path

def read_csv(fileName: Path) -> pd.DataFrame:
    data: pd.DataFrame = pd.read_csv(fileName)
    # [Poster_Link, Series_Title, Released_Year, Certificate, Runtime, Genre, IMDB_Rating, Meta_Score, Director, Star1, Star2, Star3, Star4, No_of_Voters, Gross, Number_of_Movies]
    return data