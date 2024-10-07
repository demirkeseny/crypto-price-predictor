import hopsworks
from src.config import hopsworks_config as config
from typing import List
import pandas as pd

def push_value_to_feature_group(
        value:dict,
        feature_group_name:str,
        feature_group_version:int,
        feature_group_primary_keys: List[str],
        feature_group_event_time: str
):
    """
    Push the value to the given feature_group_name in the feature storage

    Args:
        value (dict): The value to push to the Feature Store
        feature_group_name (str):
        feature_group_version (int): 

    Return:
        Null
    """
    breakpoint()
    project = hopsworks.login(
            project=config.hopsworks_project_name,
            api_key=config.hopsworks_api_key,
    )

    feature_store = project.get_feature_store()

    feature_group = feature_store.get_or_create_feature_group(
                    name=feature_group_name,
                    version=feature_group_version,
                    primary_key=feature_group_primary_keys,
                    online_enable=True,
                    event_time=feature_group_event_time,
                    # expectation_suite=expectation_suite_transactions,
                )
    
    # transform the value dict into a pandas DataFrame
    value_df = pd.DataFrame(value)

    # push to fs
    feature_group.insert(
        value_df
    )