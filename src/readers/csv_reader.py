import pandas as pd
from pathlib import Path

def read_csv(fileName: Path) -> pd.DataFrame:
    data: pd.DataFrame = pd.read_csv(fileName)
    return data