from typing import Callable, Dict, Any
import pandas as pd

# import svih processora
from .cu_asset_viewed import process as process_cu_asset_viewed
from .live_usage import process as process_live_usage
from .media_seen_program import process as process_media_seen_program
from .media_seen_vod import process as process_media_seen_vod
from .movie_favorite_added import process as process_movie_favorite_added
from .movie_rent import process as process_movie_rent
from .network_dvr_usage import process as process_network_dvr_usage
from .network_timeshift_usage import process as process_network_timeshift_usage
from .restart_usage import process as process_restart_usage
from .scheduling_added import process as process_scheduling_added
from .shop_loaded import process as process_shop_loaded
from .startover_usage import process as process_startover_usage
from .vod_usage_movie import process as process_vod_usage_movie

# default fallback
from .default import process as process_default


Processor = Callable[..., dict[str, pd.DataFrame]]


PROCESSORS: Dict[str, Processor] = {
    "CUAssetViewed": process_cu_asset_viewed,
    "LiveUsage": process_live_usage,
    "mediaSeen_PROGRAM": process_media_seen_program,
    "mediaSeen_VOD": process_media_seen_vod,
    "movieFavoriteAdded": process_movie_favorite_added,
    "movieRent": process_movie_rent,
    "NETWORK_DvrUsage": process_network_dvr_usage,
    "NETWORK_TIMESHIFTUsage": process_network_timeshift_usage,
    "RESTARTUsage": process_restart_usage,
    "schedulingAdded": process_scheduling_added,
    "shopLoaded": process_shop_loaded,
    "STARTOVERUsage": process_startover_usage,
    "VodUsageMOVIE": process_vod_usage_movie,
}


def get_processor(event_type: str) -> Processor:
    return PROCESSORS.get(event_type, process_default)
