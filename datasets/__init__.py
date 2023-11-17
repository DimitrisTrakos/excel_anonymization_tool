
from utils.types import Dataset
import pandas as pd
import os

def get_dataset_params(name):
    if name == Dataset.ADULT:
        QI_INDEX = [1, 2, 3, 4, 5, 6, 7, 8]
        target_var = 'salary-class'
        IS_CAT = [True, False, True, True, True, True, True, True]
        max_numeric = {"age": 79}
    if name == Dataset.BREAST:
        QI_INDEX = [4]
        # target_var = 'Gender'
        # max_numeric = {"age": 79}
    elif name == Dataset.CMC:
        QI_INDEX = [1, 2, 4]
        target_var = 'method'
        IS_CAT = [False, True, False]
        max_numeric = {"age": 32.5, "children": 8}
    elif name == Dataset.MGM:
        QI_INDEX = [1, 2, 3, 4, 5]
        target_var = 'severity'
        IS_CAT = [True, False, True, True, True]
        max_numeric = {"age": 50.5}
    elif name == Dataset.CAHOUSING:
        QI_INDEX = [1, 2, 3, 8, 9]
        target_var = 'ocean_proximity'
        IS_CAT = [False, False, False, False, False]
        max_numeric = {"latitude": 119.33, "longitude": 37.245, "housing_median_age": 32.5,
                    "median_house_value": 257500, "median_income": 5.2035}
    elif name == Dataset.INFORMS:
        QI_INDEX = [3, 4, 6, 13, 16]
        target_var = "poverty"
        IS_CAT = [True, True, True, True, False]
        max_numeric = {"DOBMM": None, "DOBYY": None, "RACEX":None, "EDUCYEAR": None, "income": None}
    elif name == Dataset.ITALIA:
        QI_INDEX = [1, 2, 3]
        target_var = "disease"
        IS_CAT = [False, True, False]
        max_numeric = {"age": 50, "city_birth": None, "zip_code":50000}
    else:
        print(f"Not support {name} dataset")
        raise ValueError
    return {
        'qi_index': QI_INDEX,
        # 'is_category': IS_CAT,
        # 'target_var': target_var,
        # 'max_numeric': max_numeric
    }
    
def read_config_file(file_path):
    """
    Read column-specific differential privacy parameters from a text file.

    :param file_path: Path to the text file containing configuration parameters
    :return: Tuple of lists (columns, epsilons, max_changes, min_values, round_values)
    """
    if (os.path.getsize(file_path) == 0):
        return None
    
    columns = []
    epsilons = []
    max_changes = []
    min_values = []
    round_values = []

    with open(file_path, 'r') as file:
        # Skip the header line
        for line in file:
            values = line.strip().split(',')
            columns.append(values[0])
            epsilons.append(float(values[1]))
            max_changes.append(float(values[2]))
            min_values.append(float(values[3]))
            round_values.append(values[4].lower() == 'true')

    return {
            "columns":columns,
            "epsilons":epsilons,
            "max_changes":max_changes,
            "min_values":min_values,
            "round_values":round_values
        }

def convert_columns_to_indexes(data,columnsNames):
    if(columnsNames==None):
        return None

    column_indices = [data.columns.get_loc(col) for col in columnsNames]
    return column_indices

def extract_columns_from_hierarchies(folder_path):
   # Get the list of files in the folder
    files = os.listdir(folder_path)

    # Initialize an empty list to store the extracted strings
    columnsNames = []

    # Flag to check if there are CSV files in the folder
    csv_files_exist = False

    # Iterate through each file
    for file_name in files:
        # Check if the file is a CSV file
        if file_name.endswith(".csv"):
            csv_files_exist = True
            # Split the file name by "_"
            parts = file_name.split("_")

            # If there is more than one part after splitting, take the last part
            if len(parts) > 1:
                last_part = parts[-1]

                # Remove the ".csv" extension and add the extracted string to the list
                columnsNames.append(last_part[:-4])

    # If no CSV files were found, return None
    if not csv_files_exist:
        return None

    return columnsNames
