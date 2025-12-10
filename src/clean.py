import pandas as pd

def clean_runtime(runtime: str) -> int:
    return int(runtime.replace("min", "").strip())
def clean_gross(gross: str) -> int:
    return int(gross.replace(",", "").strip())


def clean(input: pd.DataFrame) -> pd.DataFrame:
    # [Poster_Link, Series_Title, Released_Year, Certificate, Runtime, Genre, IMDB_Rating, Overview, Meta_score, Director, Star1, Star2, Star3, Star4, No_of_Votes, Gross, Number_of_Movies]
    # Save everything but Poster_link, Certificate, Number_of_Movies, and Overview
    # Convert everything to the proper format
    # [Series_Title, Released_Year, Runtime, Genre, IMDB_Rating, Meta_Score, Director, Star1, Star2, Star3, Star4, No_of_Voters, Gross]
    # [String, Int, Int, String, Float, Int, String, String, String, String, String, Int, Int]
    # Turn all nulls in box office to 0
    
    df: pd.DataFrame = input.copy()
    df = df.drop(columns=["Poster_Link", "Certificate", "Overview", "Number_of_Movies"])
    df["Released_Year"] = df["Released_Year"].astype(int)
    df["Runtime"] = df["Runtime"].apply(clean_runtime).astype(int)
    df["IMDB_Rating"] = df["IMDB_Rating"].astype(float)
    df["Meta_score"] = df["Meta_score"].astype(int)
    df["No_of_Votes"] = df["No_of_Votes"].astype(int)
    df["Gross"] = df["Gross"].apply(clean_gross).astype(int)
    
    string_columns = ["Series_Title", "Director", "Star1", "Star2", "Star3", "Star4"]
    for col in string_columns:
        df[col] = df[col].astype(str).str.strip()
    
    df["Genre"] = df["Genre"].str.split(", ")
    
    return df