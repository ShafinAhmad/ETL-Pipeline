from readers.csv_reader import read_csv
import os
python_path = r"C:\Python 3.10.0\python.exe"
os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path
from load import load
from clean import clean
from validate import validate
from transform import transform
from visualize import visualize, genre_intersection_union
from pathlib import Path
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame

root_dir = Path(__file__).resolve().parent.parent

def main():
    conf = SparkConf() \
    .set("spark.executorEnv.PYSPARK_PYTHON", python_path) \
    .set("spark.executorEnv.PYSPARK_DRIVER_PYTHON", python_path)
    spark = SparkSession.builder.config(conf=conf).appName("ETL-Pipeline").getOrCreate()
    dataDirectory: Path = root_dir / "data" / "imdb.csv"
    data: SparkDataFrame = read_csv(dataDirectory.resolve(), spark)
    validatedData, rejectedData = validate(data)
    cleanedData: SparkDataFrame = clean(validatedData)
    finalData: SparkDataFrame = transform(cleanedData)
    load(finalData, rejectedData, password="test")
    # visualize(finalData)
    # genre_intersection_union(finalData)
    spark.stop()

if __name__=="__main__":
    main()