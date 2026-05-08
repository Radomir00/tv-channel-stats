import pandas as pd
import logging

logger = logging.getLogger(__name__)


def process(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    event_type = (
        df["type"].iloc[0] if "type" in df.columns and not df.empty else "UNKNOWN"
    )

    logger.warning(f"No processor defined for event type: {event_type}")

    result = pd.DataFrame()
    return {"result": result}
