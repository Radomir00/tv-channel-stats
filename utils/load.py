import pandas as pd
from sqlalchemy import create_engine
from typing import Optional


DB_URI = "mysql+pymysql://statsuser:statspass@localhost:3306/statsdb"


def load_data(query: Optional[str] = None) -> pd.DataFrame:
    engine = create_engine(DB_URI)

    if query is None:
        query = """
        SELECT *
        FROM statistic
        """

    df = pd.read_sql(query, engine)

    return df
