import pandas as pd
from logger import logger

def validate(input: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # [Poster_Link, Series_Title, Released_Year, Certificate, Runtime, Genre, IMDB_Rating, Meta_Score, Director, Star1, Star2, Star3, Star4, No_of_Voters, Gross, Number_of_Movies]
    # Reject if anything but Poster_link, Certificate, Number_of_Movies is empty
    # Reject if movie name is duplicated
    df = input.copy()
    optional_columns: set[str] = {"Poster_Link", "Certificate", "Number_of_Movies"}
    required_columns: list[str] = [a for a in df.columns if a not in optional_columns]

    # detect missing required columns per row
    missing_df = df[required_columns].isna()
    missing_required: pd.Series = missing_df.any(axis=1)
    # build a per-row list of which required columns are missing
    missing_cols: pd.Series = missing_df.apply(
        lambda row: [col for col, is_na in row.items() if is_na], axis=1
    )

    duplicated_titles: pd.Series = df.duplicated(subset=["Series_Title"], keep=False)

    missing_reason: pd.Series = missing_cols.apply(
        lambda cols: f"missing required columns: {', '.join(cols)}" if cols else ""
    )
    duplicate_reason: pd.Series = duplicated_titles.apply(
        lambda d: "duplicate title" if d else ""
    )

    invalid_masks: pd.Series = missing_required | duplicated_titles

    # combine reasons for rows that fail multiple checks
    def _combine_reasons(m: str, d: str) -> str:
        parts: list[str] = []
        if m:
            parts.append(m)
        if d:
            parts.append(d)
        return "; ".join(parts) if parts else ""

    combined_reasons = pd.Series(
        [_combine_reasons(m, d) for m, d in zip(missing_reason, duplicate_reason)],
        index=df.index,
    )

    rejected_data: pd.DataFrame = df[invalid_masks].copy()
    # attach reason column explaining why each row was rejected
    rejected_data["reason"] = combined_reasons[invalid_masks].values

    valid_data: pd.DataFrame = df[~invalid_masks].copy()
    
    logger.info(f"validate.completed valid={len(valid_data)} rejected={len(rejected_data)}")
    return valid_data, rejected_data