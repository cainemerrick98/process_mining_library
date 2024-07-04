import unittest
from event_log import EventLog, ensure_event_log_column_convention, Trace, SimplifiedEventLog
from pandas import DataFrame
import datetime as dt

class TestEnsureColumnConvention(unittest.TestCase):

    def setUp(self) -> None:
        self.rename_dict = {}
        self.convention = ['case_id', 'activity', 'timestamp']
        self.correct_df = DataFrame(columns=['case_id', 'activity', 'timestamp'])
        self.incorrect_df = DataFrame(columns=['c', 'a', 't'])
        return super().setUp()
    
    def test_correct_column_name(self):
        """
        if the convetional name is already in the columns then the rename dict should remain empty
        """
        ensure_event_log_column_convention(self.correct_df, None, 'case_id', self.rename_dict)
        self.assertDictEqual({}, self.rename_dict)

    def test_incorrect_column_name_no_alternative(self):
        """
        if the convetional name is not already in the columns and the current name is not specified then a value error
        is thrown
        """
        self.assertRaises(ValueError,
                          ensure_event_log_column_convention,
                          data = self.incorrect_df, 
                          current_name = None, 
                          convention_name = 'case_id', 
                          rename_dict = self.rename_dict
                          )
        
    def test_incorrect_column_name_incorrect_alternative(self):
        """
        if the convetional name is not already in the columns and the current name is not in the columns then a value error
        should be thrown
        """
        self.assertRaises(ValueError,
                          ensure_event_log_column_convention,
                          data = self.incorrect_df, 
                          current_name = 'abc', 
                          convention_name = 'case_id', 
                          rename_dict = self.rename_dict
                          )
    def test_incorrect_column_name_correct_alternative(self):
        """
        if the convetional name is not already in the columns and the current name is in the df columns then 
        the rename dict should map the current name to the conventional name
        """
        ensure_event_log_column_convention(self.incorrect_df, 'c', 'case_id', self.rename_dict)
        self.assertDictEqual({'c':'case_id'}, self.rename_dict)
        
class TestEventLogConstruction(unittest.TestCase):

    def setUp(self) -> None:
        self.convention = ['case_id', 'activity', 'timestamp']

        correct_columns = {
            'case_id':[1,2,3,4],
            'activity':['a', 'b', 'a', 'c'],
            'timestamp':[dt.datetime(2024, 6, 3, 11, 30, 35), dt.datetime(2024, 6, 3, 11, 30, 35), 
                         dt.datetime(2024, 6, 3, 11, 30, 35), dt.datetime(2024, 6, 3, 11, 30, 35)]
        }

        incorrect_columns = {
            'c':[1,2,3,4],
            'a':['a', 'b', 'a', 'c'],
            't':[dt.datetime(2024, 6, 3, 11, 30, 35), dt.datetime(2024, 6, 3, 11, 30, 35), 
                         dt.datetime(2024, 6, 3, 11, 30, 35), dt.datetime(2024, 6, 3, 11, 30, 35)]
        }

        self.correct_df = DataFrame(data=correct_columns)
        self.incorrect_df = DataFrame(data=incorrect_columns)
        return super().setUp()
    
    def test_correct_convention_no_columns_specified(self):
        """
        if the dataframe columns are correctly specified and no alternative column is given then the 
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
    
    def test_incorrect_convention_with_incorrect_columns_specified(self):
        """
        if the dataframe columns are incorrect and the incorrect alternatives are given (i.e. no columns with the given name exist) 
        we should get a value error
        """
        self.assertRaises(ValueError, EventLog, data=self.incorrect_df, case_id='cc')
        
class TestTraceClass(unittest.TestCase):
    
    def setUp(self) -> None:
        return super().setUp()
    
    def test_trace_construction(self):
        trace = Trace('case_1')
        trace.append('a')
        trace.append('b')
        trace.append('c')
        trace.append('d')
        print(trace)

class TestSimplifiedEventLog(unittest.TestCase):
    
    def setUp(self) -> None:
        event_data = {
            'case_id':['a1', 'a2', 'a1', 'a2'],
            'activity':['a', 'a', 'b', 'c'],
            'timestamp':[dt.datetime(2024, 7, 1, 11, 20, 1), dt.datetime(2024, 7, 2, 13, 20, 1),
                         dt.datetime(2024, 7, 3, 11, 20, 1), dt.datetime(2024, 7, 5, 11, 20, 1)]

        }
        self.data = DataFrame(data=event_data)
        return super().setUp()

    def test_sel_construction(self):
        """
        In this setup the simplfied event log should contain 2 traces. One for case 'a1' and one for case 'a2'.
        The 'a1' trace should be <a, b> and the 'a2' trace should be <a, c> 
        """
        event_log = EventLog(self.data)
        simplified_event_log = SimplifiedEventLog(event_log)
        traces = simplified_event_log.traces

        a1_expected_trace = Trace('a1', ['a', 'b'])
        a2_expected_trace = Trace('a2', ['a', 'c'])

        self.assertEqual(traces['a1'], a1_expected_trace)
        self.assertEqual(traces['a2'], a2_expected_trace)

    def test_count_traces(self):
        event_log = EventLog(self.data)
        simplified_event_log = SimplifiedEventLog(event_log)
        trace_count = simplified_event_log.count_traces()
        print(trace_count)
        expected_trace_count = {Trace('any', ['a', 'b']) : 1, Trace('any2', ['a', 'c']):2}
        self.assertDictEqual(trace_count, expected_trace_count)



    
    

