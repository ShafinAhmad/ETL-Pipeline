from readers.csv_reader import read_csv
from load import load
from clean import clean
from validate import validate
from transform import transform
from pathlib import Path
import pandas as pd

root_dir = Path(__file__).resolve().parent.parent

pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)

def display(input: pd.DataFrame) -> None:
    return

def main():
    dataDirectory: Path = root_dir / "data" / "imdb.csv"
    data: pd.DataFrame = read_csv(dataDirectory.resolve())
    # print(data)
    validatedData, rejectedData = validate(data)
    print("Validated Data")
    cleanedData = clean(validatedData)
    print("Cleaned Data")
    # print(cleanedData)
    finalData = transform(cleanedData)
    print("Transformed Data")
    # print(finalData)
    print(finalData[["Series_Title", "Gross", "Gross_Inflation_Adjusted"]])
    load(finalData, rejectedData, password="test")
    display(finalData)

if __name__=="__main__":
    main()