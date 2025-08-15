from dice import *
import unittest
class TestDiceGameETL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[2]").appName("tests").getOrCreate()
        
        # Create test data
        cls.test_data = {
            "channel_codes": cls.spark.createDataFrame(
                [("WEB", "Browser"), ("MOB", "Mobile App")],
                ["play_session_channel_code", "english_description"]
            ),

        }
        
        cls.dimensional_data = transform_data(cls.test_data)
    
    def test_dimension_table_counts(self):
        self.assertEqual(self.dimensional_data["dim_channels"].count(), 2)

    def test_fact_table_columns(self):
        required_columns = {"session_id", "user_id", "channel_id", "start_time"}
        actual_columns = set(self.dimensional_data["fact_play_sessions"].columns)
        self.assertTrue(required_columns.issubset(actual_columns))

if __name__ == "__main__":
    unittest.main()