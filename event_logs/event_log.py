from pandas import DataFrame
from typing import Optional

def ensure_event_log_column_convention(data:DataFrame, current_name:str, convention_name:str, rename_dict:dict):
    """
    Checks if the convention name is in the dataset. If not checks that an alternative has been provided,
    if an alternative has been provided it is added to the rename_dict, if no alternative is provided then a value
    error is raised.
    """
    if current_name not in data.columns:
        if convention_name is None:
            raise ValueError(f'{convention_name} not in columns and no {convention_name} column is specified')
        rename_dict[current_name] = convention_name
        

class EventLog:
    """
    The event log requires, as a mininum, a case id column, a activity name column, and a timestamp
    column. Other attributes can be included but these 3 must exist. The constructor expects the following
    naming convention for the three necessary columns "case_id", "activity", "timestamp". If dataframe does 
    not conform to this convention then the names of each column must be specified in the constructor.
    """

    def __init__(self, data:DataFrame, 
                 case_id:Optional[str]=None, activity_name:Optional[str]=None, timestamp:Optional[str]=None):
        
        rename_dict = {}
        
        #these populate the rename dict to ensure naming convention is adhered to
        ensure_event_log_column_convention(data, case_id, 'case_id', rename_dict)
        ensure_event_log_column_convention(data, activity_name, 'activity', rename_dict)
        ensure_event_log_column_convention(data, timestamp, 'timestamp', rename_dict)

        data.rename(columns=rename_dict, inplace=True)

        self.data = data

    



        
        
        


            
            
        


