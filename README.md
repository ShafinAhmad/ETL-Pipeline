# ETL Pipeline Project

A Python-based ETL (Extract, Transform, Load) pipeline for processing IMDb movie dataset. This project demonstrates data cleaning, validation, and transformation workflows using pandas.

## Overview

This project implements a complete ETL pipeline that processes movie metadata from an IMDb CSV dataset. The pipeline extracts raw data, validates records, cleans formatting issues, transforms the data into a standardized format, and loads the results for further analysis.

## Features

-   **Extract**: Read movie data from CSV files using pandas
-   **Validate**: Reject records with missing critical fields or duplicate entries
-   **Clean**: Remove unnecessary columns, standardize data formats, and handle missing values
-   **Transform**: Enrich and reshape data for downstream analysis
-   **Load**: Export processed data

## Data Schema

The pipeline works with the following IMDb movie fields:

-   Series_Title
-   Released_Year
-   Certificate (rating)
-   Runtime
-   Genre
-   IMDB_Rating
-   Overview
-   Meta_score
-   Director
-   Star1, Star2, Star3, Star4
-   No_of_Votes
-   Gross (box office)

The pipeline will:

1.  Load the IMDb dataset from imdb.csv
2.  Validate records and separate rejected data
3.  Clean and standardize the validated data
4.  Transform the data into final format
5.  Load the results to output destination
6.  Display summary information

## Usage

1.  Create a virtual environment in the project root:

```cmd
python -m venv .venv
```

2.  Activate the venv (cmd.exe):

```cmd
.venv\Scripts\activate.bat
```

3.  Upgrade pip and install dependencies (from `requirements.txt`):

```cmd
python -m pip install -U pip
python -m pip install -r requirements.txt
```

4.  Run tests with pytest:

```cmd
python -m pytest -q
```

5.  Run the pipeline

```cmd
python ./src/main.py
```

## CPI data

This project uses the `cpi` package in `src/transform.py` to compute inflation-adjusted gross values. The package caches CPI data and may warn that data is stale. To refresh the CPI cache (network required), run:

```cmd
python -c "import cpi; cpi.update()"
```

Running `cpi.update()` once in the venv will remove `StaleDataWarning` and ensure `cpi.inflate()` returns up-to-date factors.

## Cleanup

Deactivate the venv with:

```cmd
deactivate
```
