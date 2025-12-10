import pandas as pd

def validate(input: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # [Poster_Link, Series_Title, Released_Year, Certificate, Runtime, Genre, IMDB_Rating, Meta_Score, Director, Star1, Star2, Star3, Star4, No_of_Voters, Gross, Number_of_Movies]
    # Reject if anything but Poster_link, Certificate, Number_of_Movies is empty
    # Reject if movie name is duplicated
    df = input.copy()
    optional_columns = {"Poster_Link", "Certificate", "Number_of_Movies", "Gross"}
    required_columns = [a for a in df.columns if a not in optional_columns]

    missing_required = df[required_columns].isna().any(axis=1)
    duplicated_titles = df.duplicated(subset=["Series_Title"], keep=False)
    invalid_masks = missing_required | duplicated_titles
    
    rejected_data = df[invalid_masks]
    valid_data = df[~invalid_masks]
    return valid_data, rejected_data