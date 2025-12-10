import pandas as pd
import cpi
# cpi.update()

def transform(input: pd.DataFrame) -> pd.DataFrame:
    df: pd.DataFrame = input.copy()
    
    # Add Genre Count column
    df['Genre_Count'] = df['Genre'].str.count(',') + 1
    
    print("Genre Count Added")
    
    # Turn separate star columns into a single list column
    df["Stars"] = df[["Star1", "Star2", "Star3", "Star4"]].values.tolist()
    df = df.drop(columns=["Star1", "Star2", "Star3", "Star4"])
    
    print("Stars Column Created")
    
    # Add Decade column
    df["Decade"] = (df["Released_Year"] // 10) * 10
    
    print("Decade Column Added")
    
    # Add Movie Length Category
    length_bins = [0, 90, 120, 180, float("inf")]
    length_labels = ["Short", "Regular", "Long", "Extreme"]
    df["Movie_Length"] = pd.cut(df["Runtime"], bins=length_bins, labels=length_labels)
    
    print("Movie Length Category Added")
    
    # Add Rating Category
    rating_bins = [0, 5, 7, 8, 9, 10]
    rating_labels = ["Bad", "Ok", "Good", "Very Good", "GoAT"]
    df["Rating_Category"] = pd.cut(df["IMDB_Rating"], bins=rating_bins, labels=rating_labels)
    
    print("Rating Category Added")
    
    # Add Popularity Score
    df["Popularity_Score"] = df["No_of_Votes"] * df["IMDB_Rating"]
    
    print("Popularity Score Added")
    
    # Adjust Gross for Inflation to current dollars
    all_years = df["Released_Year"].unique()
    print("Unique years extracted")
    dict_years = {year: cpi.inflate(1, year) for year in all_years}
    print("Inflation factors dictionary created")
    inflation_factors = df["Released_Year"].map(dict_years)
    print("Inflation Factors Computed")
    df["Gross_Inflation_Adjusted"] = (df["Gross"] * inflation_factors).astype(int)
    
    print("Gross Adjusted for Inflation")
    return df