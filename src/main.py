from readers.csv_reader import read_csv
from load import load
from clean import clean
from validate import validate
from transform import transform
import pandas as pd
from pathlib import Path

root_dir = Path(__file__).resolve().parent.parent

def display(input: pd.DataFrame) -> None:
    pass

def main():
    dataDirectory: Path = root_dir / "data" / "imdb.csv"
    data: pd.DataFrame = read_csv(dataDirectory.resolve())
    # print(data)
    validatedData, rejectedData = validate(data)
    cleanedData = clean(validatedData)
    finalData = transform(cleanedData)
    load(finalData, rejectedData)
    display(finalData)
    pass

if __name__=="__main__":
    main()