import os
import shutil
import tempfile
import unittest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import broadcast
from src.main.topXitem import process_data

class ProcessDataTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create a local Spark session for testing.
        cls.spark = SparkSession.builder.master("local[2]").appName("TestTopXItems").getOrCreate()
        # Create a temporary directory to store dummy Parquet files.
        cls.temp_dir = tempfile.mkdtemp()
        
        # Directories for main and reference data.
        cls.main_dir = os.path.join(cls.temp_dir, "main")
        cls.ref_dir = os.path.join(cls.temp_dir, "reference")
        os.mkdir(cls.main_dir)
        os.mkdir(cls.ref_dir)
        
        # Create dummy main data.
        # Columns: geographical_location_oid, video_camera_oid, detection_oid, item_name, timestamp_detected.
        main_data = [
                    # Geographical ID 1
                    (1, "cam_a", "det_1", "Cat", datetime(2024, 2, 26, 10, 30, 0)),
                    (1, "cam_a", "det_2", "Cat", datetime(2024, 2, 26, 10, 31, 0)),
                    (1, "cam_a", "det_3", "Cat", datetime(2024, 2, 26, 11, 0, 0)),
                    (1, "cam_a", "det_4", "Cat", datetime(2024, 2, 26, 12, 15, 0)),
                    (1, "cam_a", "det_5", "Cat", datetime(2024, 2, 26, 12, 16, 0)),
                    (1, "cam_a", "det_6", "Cat", datetime(2024, 2, 26, 12, 45, 0)),
                    (1, "cam_a", "det_7", "Cat", datetime(2024, 2, 26, 13, 10, 0)),
                    (1, "cam_a", "det_8", "Cat", datetime(2024, 2, 26, 13, 11, 0)),
                    (1, "cam_a", "det_9", "Cat", datetime(2024, 2, 26, 14, 0, 0)),
                    (1, "cam_a", "det_10", "Cat", datetime(2024, 2, 26, 15, 5, 0)),

                    (1, "cam_b", "det_11", "Person", datetime(2024, 2, 26, 15, 6, 0)),
                    (1, "cam_b", "det_12", "Person", datetime(2024, 2, 26, 15, 7, 0)),
                    (1, "cam_b", "det_13", "Person", datetime(2024, 2, 26, 15, 8, 0)),
                    (1, "cam_b", "det_14", "Person", datetime(2024, 2, 26, 16, 30, 0)),
                    (1, "cam_b", "det_15", "Person", datetime(2024, 2, 26, 16, 31, 0)),
                    (1, "cam_b", "det_16", "Person", datetime(2024, 2, 26, 16, 45, 0)),
                    (1, "cam_b", "det_17", "Person", datetime(2024, 2, 26, 17, 0, 0)),
                    (1, "cam_b", "det_18", "Person", datetime(2024, 2, 26, 17, 1, 0)),
                    (1, "cam_b", "det_19", "Person", datetime(2024, 2, 26, 17, 10, 0)),
                    (1, "cam_b", "det_20", "Person", datetime(2024, 2, 26, 17, 11, 0)),
                    (1, "cam_b", "det_20", "Person", datetime(2024, 2, 26, 17, 11, 0)),
                    (1, "cam_b", "det_20", "Person", datetime(2024, 2, 26, 17, 11, 0)),

                    (1, "cam_c", "det_21", "Motorcycle", datetime(2024, 2, 26, 17, 13, 0)),
                    (1, "cam_c", "det_21", "Motorcycle", datetime(2024, 2, 26, 17, 13, 0)),
                    (1, "cam_c", "det_21", "Motorcycle", datetime(2024, 2, 26, 17, 13, 0)),
                    (1, "cam_c", "det_22", "Motorcycle", datetime(2024, 2, 26, 17, 15, 0)),
                    (1, "cam_c", "det_23", "Motorcycle", datetime(2024, 2, 26, 17, 20, 0)),
                    (1, "cam_c", "det_24", "Motorcycle", datetime(2024, 2, 26, 17, 25, 0)),
                    (1, "cam_c", "det_25", "Motorcycle", datetime(2024, 2, 26, 17, 30, 0)),
                    (1, "cam_c", "det_26", "Motorcycle", datetime(2024, 2, 26, 17, 35, 0)),
                    (1, "cam_c", "det_27", "Motorcycle", datetime(2024, 2, 26, 17, 40, 0)),
                    (1, "cam_c", "det_28", "Motorcycle", datetime(2024, 2, 26, 17, 45, 0)),
                    (1, "cam_c", "det_29", "Motorcycle", datetime(2024, 2, 26, 17, 50, 0)),

                    # Geographical ID 2
                    (2, "cam_d", "det_30", "Car", datetime(2024, 2, 26, 18, 0, 0)),
                    (2, "cam_d", "det_31", "Car", datetime(2024, 2, 26, 18, 5, 0)),
                    (2, "cam_d", "det_32", "Car", datetime(2024, 2, 26, 18, 10, 0)),
                    (2, "cam_d", "det_33", "Car", datetime(2024, 2, 26, 18, 15, 0)),
                    (2, "cam_d", "det_34", "Car", datetime(2024, 2, 26, 18, 20, 0)),
                    (2, "cam_d", "det_35", "Car", datetime(2024, 2, 26, 18, 25, 0)),
                    (2, "cam_d", "det_36", "Car", datetime(2024, 2, 26, 18, 30, 0)),
                    (2, "cam_d", "det_37", "Car", datetime(2024, 2, 26, 18, 35, 0)),

                    (2, "cam_e", "det_38", "Dog", datetime(2024, 2, 26, 18, 40, 0)),
                    (2, "cam_e", "det_39", "Dog", datetime(2024, 2, 26, 18, 45, 0)),
                    (2, "cam_e", "det_40", "Dog", datetime(2024, 2, 26, 18, 50, 0)),
                    (2, "cam_e", "det_41", "Dog", datetime(2024, 2, 26, 18, 55, 0)),
                    (2, "cam_e", "det_42", "Dog", datetime(2024, 2, 26, 19, 0, 0)),

                    (2, "cam_f", "det_43", "Pigeon", datetime(2024, 2, 26, 19, 5, 0)),
                    (2, "cam_f", "det_44", "Pigeon", datetime(2024, 2, 26, 19, 10, 0)),
                    (2, "cam_f", "det_45", "Pigeon", datetime(2024, 2, 26, 19, 15, 0)),
                    (2, "cam_f", "det_46", "Pigeon", datetime(2024, 2, 26, 19, 20, 0)),
                    (2, "cam_f", "det_47", "Pigeon", datetime(2024, 2, 26, 19, 25, 0)),

                    (2, "cam_g", "det_48", "Lamppost", datetime(2024, 2, 26, 19, 30, 0)),
                    (2, "cam_g", "det_49", "Lamppost", datetime(2024, 2, 26, 19, 35, 0)),
                    (2, "cam_g", "det_50", "Lamppost", datetime(2024, 2, 26, 19, 40, 0)),

                    (3, "cam_h", "det_51", "Cat", datetime(2024, 3, 26, 10, 31, 0)),
                    (3, "cam_h", "det_52", "Cat", datetime(2024, 3, 26, 11, 0, 0)),
                    (3, "cam_i", "det_53", "Cat", datetime(2024, 3, 26, 12, 15, 0)),
                    (3, "cam_i", "det_54", "Cat", datetime(2024, 3, 26, 12, 16, 0)),
                    (3, "cam_j", "det_55", "Cat", datetime(2024, 3, 26, 12, 45, 0)),
                    (3, "cam_j", "det_55", "Cat", datetime(2024, 3, 26, 12, 45, 0)),
                    (3, "cam_k", "det_56", "Cat", datetime(2024, 3, 26, 13, 11, 0)),
                    (3, "cam_k", "det_57", "Cat", datetime(2024, 3, 26, 14, 0, 0)),
                    (3, "cam_k", "det_58", "Cat", datetime(2024, 3, 26, 15, 5, 0))
                ]

        # Mapping Main Data with column names
        df_main = cls.spark.createDataFrame(main_data, ["geographical_location_oid", "video_camera_oid", "detection_oid", "item_name", "timestamp_detected"])
        
        # Creating actual parquet file
        cls.main_file = os.path.join(cls.main_dir, "dataA.parquet")
        df_main.write.mode("overwrite").parquet(cls.main_file)

        ref_data = [
            (1, "Location1"),
            (2, "Location2"),
            (3, "Location3"),
            (4, "location4"),
            (5, "Location5"),
            (6, "Location6")
        ]

        df_ref = cls.spark.createDataFrame(ref_data, ["geographical_location_oid", "geographical_location"])
        cls.ref_file = os.path.join(cls.ref_dir, "dataB.parquet")
        df_ref.write.mode("overwrite").parquet(cls.ref_file)

    @classmethod
    def tearDownClass(cls):
        # Stop Spark session and remove temporary directory.
        cls.spark.stop()
        shutil.rmtree(cls.temp_dir)

    def test_process_data(self):
        # Pass our dummy file paths as a list for main data.
        input_path = self.main_file
        output_path = os.path.join(self.temp_dir, "output.parquet")
        result = process_data(self.spark, input_path, self.ref_file, output_path, topx=10)
        
        # Check if the Parquet output exists
        self.assertTrue(os.path.exists(output_path), "Output file was not created!")

        # Read output and verify content
        df_output = self.spark.read.parquet(output_path)
        df_output.show()

        #Check if the output has rows
        self.assertGreater(df_output.count(), 0, "Output should have at least 1 row")

if __name__ == '__main__':
    unittest.main()
