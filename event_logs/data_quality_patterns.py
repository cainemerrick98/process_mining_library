"""
This module represents an attempt to formalise in code the data quality pattern identified in the paper 
*Event log imperfection patterns for process mining: Towards a systematic approach to cleaning event logs. Information Systems, 64, pp. 132-150.*
"""
from event_log import EventLog, SimplifiedEventLog
from typing import Union

class DataQualityPattern:
    """
    An abstract base class to provide a common interface for interaction with the different 
    data quality patterns.
    """
    def __init__(self):
        pass

    def identify(self, event_log:Union[EventLog, SimplifiedEventLog]):
        """
        concrete classes will instantiate a method to identify if a particular data quality pattern 
        is present.
        """
        raise NotImplementedError()
    
    def remediate(self, event_log:Union[EventLog, SimplifiedEventLog]):
        """
        concrete classes will instantiate a method to remediate a particular data quality pattern 
        if it is present.
        """
        raise NotImplementedError()