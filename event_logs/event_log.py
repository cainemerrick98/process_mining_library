import datetime as dt
from pandas import DataFrame
from numpy import datetime64
from typing import Iterator, Optional
from collections.abc import MutableSequence


def ensure_event_log_column_convention(data:DataFrame, current_name:str, convention_name:str, rename_dict:dict):
    """
    Checks if the convention name is in the dataset. If not checks that an alternative has been provided,
    if an alternative has been provided it is added to the rename_dict, if no alternative is provided then a value
    error is raised.
    """
    if convention_name not in data.columns:
        if current_name is None:
            raise ValueError(f'{convention_name} not in columns and no {convention_name} column is specified')
        elif current_name not in data.columns:
            raise ValueError(f'{current_name} not in dataframe columns')
        rename_dict[current_name] = convention_name


class EventLog:
    """
    The event log requires, as a mininum, a case id column, a activity name column, and a timestamp
    column. Other attributes can be included but these 3 must exist. The constructor expects the following
    naming convention for the three necessary columns "case_id", "activity", "timestamp". If dataframe does 
    not conform to this convention then the names of each column must be specified in the constructor.

    The timestamp column must be of the type datetime64 or dt.datetime, this is required before passing the dataframe to the 
    constructor. This avoid the requirement to pass the current datetime format into the constructor.

    The data parameter is deep copied - this ensures the original dataframe is not modified when creating 
    an event log.
    """

    def __init__(self, data:DataFrame,
                 case_id:Optional[str]=None, activity_name:Optional[str]=None, timestamp:Optional[str]=None,
                 ):

        rename_dict = {}
        data = data.copy(deep=True)
        #these populate the rename dict to ensure naming convention is adhered to
        ensure_event_log_column_convention(data, case_id, 'case_id', rename_dict)
        ensure_event_log_column_convention(data, activity_name, 'activity', rename_dict)
        ensure_event_log_column_convention(data, timestamp, 'timestamp', rename_dict)

        data.rename(columns=rename_dict, inplace=True)

        if isinstance(data['timestamp'], (datetime64, dt.datetime)):
            raise ValueError('timestamp column must be type datetime64 or dt.datetime')

        data.sort_values('timestamp', ascending=True, inplace=True)
        self.data = data

    def __str__(self):
        return self.data.__str__()


class Trace(MutableSequence):
    """
    A trace is a sequence of activities. One trace represents a case where the elements in the trace represent
    the chronological order of activities performed on that case.
    """
    def __init__(self, case_id:str, data:list=[]):
        self.case_id = case_id
        self._data = data
    
    def __str__(self):
        return f'CASE ID: {self.case_id} = <{",".join(self._data)}>'
    
    def __hash__(self) -> int:
        return hash(self.case_id)
    
    def __getitem__(self, index):
        return self._data[index]
    
    def __setitem__(self, index, value):
        self._data[index] = value
    
    def __delitem__(self, index):
        del self._data[index]
    
    def __len__(self):
        return len(self._data)
    
    def insert(self, index, value):
        self._data.insert(index, value)
    
    def __iter__(self) -> Iterator:
        return self._data.__iter__()
    
    def __eq__(self, other):
        if not isinstance(other, Trace):
            return False
        for i in range(len(self._data)):
            if self._data[i] != other._data[i]:
                return False
        return True

    def append(self, value):
        self.insert(len(self), value)


class SimplifiedEventLog:
    """
    A simplified event log is a multiset of traces. To build a simplified event log requires a event log to be passed 
    in the constructor. We iterate over the rows in the event log creating traces by adding activities to the trace when 
    we find them.
    """
    def __init__(self, event_log:EventLog) -> None:
        self.traces = {}

        for i in range(len(event_log.data)):
            event = event_log.data.iloc[i]
            case_id = event['case_id']
            activity = event['activity']
            if self.traces.get(case_id) is not None:
                trace = self.traces[case_id]
                trace.append(activity)
            else:
                self.traces[case_id] = Trace(case_id, [activity])
                
    
    def count_traces(self) -> dict:
        """
        Counts the number of occurences of each trace in the simplified event log.
        """
        trace_count = {}
        for trace in self.traces.values():
            if trace_count.get(trace) is not None:
                trace_count[trace] += 1
            else:
                trace_count[trace] = 1
        return trace_count


            
            

        
        
        



            
        


