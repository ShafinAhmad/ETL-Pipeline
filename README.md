# Basic ETL Pipeline Project

A Python-based ETL (Extract, Transform, Load) pipeline for processing IMDb movie dataset. This project demonstrates data cleaning, validation, and transformation workflows using pandas.

## Overview

This project implements a complete ETL pipeline that processes movie metadata from an IMDb CSV dataset. The pipeline extracts raw data, validates records, cleans formatting issues, transforms the data into a standardized format, and loads the results for further analysis.

## Features

- **Extract**: Read movie data from CSV files using pandas
- **Validate**: Reject records with missing critical fields or duplicate entries
- **Clean**: Remove unnecessary columns, standardize data formats, and handle missing values
- **Transform**: Enrich and reshape data for downstream analysis
- **Load**: Export processed data

## Data Schema

The pipeline works with the following IMDb movie fields:
- Series_Title
- Released_Year
- Certificate (rating)
- Runtime
- Genre
- IMDB_Rating
- Overview
- Meta_score
- Director
- Star1, Star2, Star3, Star4
- No_of_Votes
- Gross (box office)

## Usage

Run the pipeline:
```bash
python src/main.py
```

The pipeline will:
1. Load the IMDb dataset from imdb.csv
2. Validate records and separate rejected data
3. Clean and standardize the validated data
4. Transform the data into final format
5. Load the results to output destination
6. Display summary information

## Requirements

- Python 3.8+
- pandas
- pathlib (standard library)