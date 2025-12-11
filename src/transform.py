import pandas as pd
import cpi
from logger import logger
# cpi.update()

def transform(input: pd.DataFrame) -> pd.DataFrame:
    df: pd.DataFrame = input.copy()
    
    # Add Genre Count column (handle list-type Genre from cleaning)
    def _genre_count(g):
        if g is None:
            return 0
        if isinstance(g, float) and pd.isna(g):
            return 0
        if isinstance(g, (list, tuple)):
            return len(g)
        try:
            return int(str(g).count(',') + 1)
        except Exception:
            return 0

    df['Genre_Count'] = df['Genre'].apply(_genre_count)
    
    # print("Genre Count Added")
    
    # Turn separate star columns into a single list column
    df["Stars"] = df[["Star1", "Star2", "Star3", "Star4"]].values.tolist()
    df = df.drop(columns=["Star1", "Star2", "Star3", "Star4"])
    
    # print("Stars Column Created")
    
    # Add Decade column
    df["Decade"] = (df["Released_Year"] // 10) * 10
    
    # print("Decade Column Added")
    
    # Add Movie Length Category
    length_bins = [0, 90, 120, 180, float("inf")]
    length_labels = ["Short", "Regular", "Long", "Extreme"]
    df["Movie_Length"] = pd.cut(df["Runtime"], bins=length_bins, labels=length_labels)
    
    # print("Movie Length Category Added")
    
    # Add Rating Category
    rating_bins = [0, 5, 7, 8, 9, 10]
    rating_labels = ["Bad", "Ok", "Good", "Very Good", "GoAT"]
    df["Rating_Category"] = pd.cut(df["IMDB_Rating"], bins=rating_bins, labels=rating_labels)
    
    # print("Rating Category Added")
    
    # Add Popularity Score
    df["Popularity_Score"] = df["No_of_Votes"] * df["IMDB_Rating"]
    
    # print("Popularity Score Added")
    
    # Adjust Gross for Inflation to current dollars
    all_years = df["Released_Year"].unique()
    # print("Unique years extracted")
    dict_years = {year: cpi.inflate(1, year) for year in all_years}
    # print("Inflation factors dictionary created")
    inflation_factors = df["Released_Year"].map(dict_years)
    # print("Inflation Factors Computed")
    df["Gross_Inflation_Adjusted"] = ((df["Gross"] * inflation_factors // 1)).astype(int)
    
    logger.info(f"transform.completed rows={len(df)}")
    return df