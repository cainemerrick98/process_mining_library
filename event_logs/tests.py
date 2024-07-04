import unittest
from event_log import EventLog, ensure_event_log_column_convention
from pandas import DataFrame

class TestEnsureColumnConvention(unittest.TestCase):

    def setUp(self) -> None:
        self.convention = ['case_id', 'activity', 'timestamp']
        self.correct_df = DataFrame(columns=['case_id', 'activity', 'timestamp'])
        self.incorrect_df = DataFrame(columns=['c', 'a', 't'])
        return super().setUp()
    
    def test_correct_convention_no_columns_specified(self):
        """
        if the dataframe columns are correctly specified and no alternative columns are given then the 
        event logs data attribute columns and the convention column names should be equal and no value errors
        should be raised
        """
        event_log = EventLog(self.correct_df)
        self.assertListEqual(self.convention, event_log.data.columns.to_list())

    def test_correct_convention_columns_specified(self):
        """
        if the dataframe columns are correctly specified and alternative columns are given then the 
        event logs data attribute columns and the convention column names should be equal and no value errors
        should be raised - i.e. same as above
        """
        event_log = EventLog(self.correct_df, 'case', 'ac', 'ts')
        self.assertListEqual(self.convention, event_log.data.columns.to_list())
    
    def test_incorrect_convention_with_no_columns_specified(self):
        """
        if the dataframe columns are incorrect and no alternatives are given we should get a value error
        """
        self.assertRaises(ValueError, EventLog, data=self.incorrect_df)

    def test_incorrect_convention_with_correct_columns_specified(self):
        """
        if the dataframe columns are incorrect and the correct alternatives are given we should get an event
        log with a data attribute which has columns equal to the convention
        """
        event_log = EventLog(self.incorrect_df, 'c', 'a', 't')
        self.assertListEqual(self.convention, event_log.data.columns.to_list())
    
    def test_incorrect_convention_with_correct_columns_specified(self):
        """
        if the dataframe columns are incorrect and the incorrect alternatives are given (i.e. no columns with the given name exist) 
        we should get a value error
        """
        self.assertRaises(ValueError, EventLog, data=self.incorrect_df, case_id='cc')
        






    
    

