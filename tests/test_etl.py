import sys
from pathlib import Path

# Ensure `src` is importable (tests/ is one level below repo root)
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import types
import importlib
import pandas as pd
import pytest

from clean import clean_runtime, clean_gross, clean as clean_df
from validate import validate
from readers.csv_reader import read_csv


def test_clean_runtime_and_gross():
    assert clean_runtime("142 min") == 142
    assert clean_gross("1,234") == 1234


def test_clean_df():
    data = pd.DataFrame([{
        "Poster_Link": "p",
        "Series_Title": " Movie ",
        "Released_Year": "1994",
        "Certificate": None,
        "Runtime": "142 min",
        "Genre": "Drama, Comedy",
        "IMDB_Rating": "8.5",
        "Overview": "ov",
        "Meta_score": "85",
        "Director": " Dir ",
        "Star1": "A",
        "Star2": "B",
        "Star3": "C",
        "Star4": "D",
        "No_of_Votes": "12345",
        "Gross": "1,234",
        "Number_of_Movies": None,
    }])

    cleaned = clean_df(data)
    # Columns removed
    assert "Poster_Link" not in cleaned.columns
    assert "Certificate" not in cleaned.columns
    assert "Overview" not in cleaned.columns
    assert "Number_of_Movies" not in cleaned.columns

    # Values cleaned and types converted
    assert cleaned.loc[0, "Series_Title"] == "Movie"
    assert cleaned.loc[0, "Released_Year"] == 1994
    assert cleaned.loc[0, "Runtime"] == 142
    assert isinstance(cleaned.loc[0, "Genre"], list)
    assert cleaned.loc[0, "Gross"] == 1234
    assert cleaned.loc[0, "No_of_Votes"] == 12345


def test_validate_rejects_missing_and_duplicates():
    df = pd.DataFrame([
        {"Series_Title": "A", "Released_Year": 1990, "Runtime": "90 min", "Genre": "G", "IMDB_Rating": 5, "Meta_score": 50, "Director": "D", "Star1": "a", "Star2": "b", "Star3": "c", "Star4": "d", "No_of_Votes": 100, "Gross": "1,000", "Poster_Link": None, "Certificate": None, "Number_of_Movies": None},
        {"Series_Title": "A", "Released_Year": 1991, "Runtime": "90 min", "Genre": "G", "IMDB_Rating": 6, "Meta_score": 60, "Director": "D", "Star1": "a", "Star2": "b", "Star3": "c", "Star4": "d", "No_of_Votes": 200, "Gross": "2,000", "Poster_Link": None, "Certificate": None, "Number_of_Movies": None},
        {"Series_Title": "B", "Released_Year": None, "Runtime": "90 min", "Genre": "G", "IMDB_Rating": 6, "Meta_score": 60, "Director": "D", "Star1": "a", "Star2": "b", "Star3": "c", "Star4": "d", "No_of_Votes": 200, "Gross": "2,000", "Poster_Link": None, "Certificate": None, "Number_of_Movies": None},
    ])

    valid, rejected = validate(df)
    # All three rows should be rejected: two are duplicates by title, one has missing required field
    assert len(valid) == 0
    assert len(rejected) == 3


def test_transform_with_cpi():
    # Use the installed `cpi` package to compute expected inflation adjustment
    import cpi

    transform = importlib.import_module("transform").transform

    df = pd.DataFrame([{
        "Series_Title": "T",
        "Released_Year": 2000,
        "Runtime": 120,
        "Genre": "Drama",
        "IMDB_Rating": 8.0,
        "Meta_score": 80,
        "Director": "D",
        "Star1": "A",
        "Star2": "B",
        "Star3": "C",
        "Star4": "D",
        "No_of_Votes": 1000,
        "Gross": 1000,
    }])

    out = transform(df)
    assert "Genre_Count" in out.columns
    assert out.loc[0, "Genre_Count"] == 1
    assert "Stars" in out.columns

    # Compute expected inflation-adjusted gross using cpi.inflate
    factor = cpi.inflate(1, int(df.loc[0, "Released_Year"]))
    expected = int(df.loc[0, "Gross"] * factor)
    assert out.loc[0, "Gross_Inflation_Adjusted"] == expected


def test_read_csv(tmp_path):
    p = tmp_path / "mini.csv"
    header = "Poster_Link,Series_Title,Released_Year,Certificate,Runtime,Genre,IMDB_Rating,Meta_score,Director,Star1,Star2,Star3,Star4,No_of_Votes,Gross,Number_of_Movies\n"
    p.write_text(header + "p,Title,1999,,100 min,Drama,8.0,80,Dir,A,B,C,D,100,1,\n")

    df = read_csv(Path(p))
    assert df.shape[0] == 1
    assert "Series_Title" in df.columns


def test_clean_runtime_edge_cases():
    assert clean_runtime(" 90 min") == 90
    assert clean_runtime("0 min") == 0
    with pytest.raises(ValueError):
        clean_runtime("N/A")


def test_clean_gross_edge_cases():
    assert clean_gross("0") == 0
    assert clean_gross("10,000") == 10000
    with pytest.raises(ValueError):
        clean_gross("")


def test_validate_allows_optional_empty():
    df = pd.DataFrame([
        {"Series_Title": "OK", "Released_Year": 2001, "Runtime": "100 min", "Genre": "G", "IMDB_Rating": 7.0, "Meta_score": 70, "Director": "D", "Star1": "a", "Star2": "b", "Star3": "c", "Star4": "d", "No_of_Votes": 100, "Gross": "1,000", "Poster_Link": None, "Certificate": None, "Number_of_Movies": None}
    ])
    valid, rejected = validate(df)
    assert len(valid) == 1
    assert len(rejected) == 0


def test_read_csv_accepts_str_path(tmp_path):
    p = tmp_path / "mini2.csv"
    header = "Poster_Link,Series_Title,Released_Year,Certificate,Runtime,Genre,IMDB_Rating,Meta_score,Director,Star1,Star2,Star3,Star4,No_of_Votes,Gross,Number_of_Movies\n"
    p.write_text(header + "p,Title2,2005,,95 min,Action,7.5,75,Dir,A,B,C,D,200,500,\n")

    df = read_csv(str(p))
    assert df.iloc[0]["Series_Title"] == "Title2"


def test_transform_categories_and_scores():
    import cpi
    transform = importlib.import_module("transform").transform

    df = pd.DataFrame([
        {"Series_Title": "S1", "Released_Year": 1999, "Runtime": 85, "Genre": "A, B", "IMDB_Rating": 4.5, "Meta_score": 40, "Director": "D", "Star1": "a", "Star2": "b", "Star3": "c", "Star4": "d", "No_of_Votes": 10, "Gross": 100},
        {"Series_Title": "S2", "Released_Year": 2005, "Runtime": 100, "Genre": "C", "IMDB_Rating": 6.5, "Meta_score": 60, "Director": "D", "Star1": "e", "Star2": "f", "Star3": "g", "Star4": "h", "No_of_Votes": 20, "Gross": 200},
        {"Series_Title": "S3", "Released_Year": 2011, "Runtime": 130, "Genre": "D, E, F", "IMDB_Rating": 7.5, "Meta_score": 75, "Director": "D", "Star1": "i", "Star2": "j", "Star3": "k", "Star4": "l", "No_of_Votes": 30, "Gross": 300},
        {"Series_Title": "S4", "Released_Year": 2020, "Runtime": 200, "Genre": "G", "IMDB_Rating": 9.0, "Meta_score": 90, "Director": "D", "Star1": "m", "Star2": "n", "Star3": "o", "Star4": "p", "No_of_Votes": 40, "Gross": 400},
    ])

    out = transform(df)

    # Movie length categories mapped correctly
    assert out.loc[out.Series_Title == "S1", "Movie_Length"].iloc[0] == "Short"
    assert out.loc[out.Series_Title == "S2", "Movie_Length"].iloc[0] == "Regular"
    assert out.loc[out.Series_Title == "S3", "Movie_Length"].iloc[0] == "Long"
    assert out.loc[out.Series_Title == "S4", "Movie_Length"].iloc[0] == "Extreme"

    # Rating category labels
    assert out.loc[out.Series_Title == "S1", "Rating_Category"].iloc[0] == "Bad"
    assert out.loc[out.Series_Title == "S2", "Rating_Category"].iloc[0] == "Ok"
    assert out.loc[out.Series_Title == "S3", "Rating_Category"].iloc[0] == "Good"
    assert out.loc[out.Series_Title == "S4", "Rating_Category"].iloc[0] == "Very Good"

    # Popularity score
    assert out.loc[out.Series_Title == "S2", "Popularity_Score"].iloc[0] == pytest.approx(20 * 6.5)

    # Decade calculation
    assert out.loc[out.Series_Title == "S1", "Decade"].iloc[0] == 1990
    assert out.loc[out.Series_Title == "S4", "Decade"].iloc[0] == 2020

    # Stars combined
    assert out.loc[out.Series_Title == "S3", "Stars"].iloc[0] == ["i", "j", "k", "l"]
    # Check inflation-adjusted gross computed via cpi for one sample
    factor_s2 = cpi.inflate(1, int(df.loc[1, "Released_Year"]))
    expected_s2 = int(df.loc[1, "Gross"] * factor_s2)
    assert out.loc[out.Series_Title == "S2", "Gross_Inflation_Adjusted"].iloc[0] == expected_s2
