
import pandas as pd
import os
    
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

def find_csv_file(folder_path, file_names):
    """
    Search for CSV files in a folder and return the names of files that match the given list of names.

    Parameters:
    - folder_path (str): The path to the folder containing CSV files.
    - file_names (list): A list of strings representing the names to search for.

    Returns:
    - list: A list of CSV file names that were found in the folder.
    """

    # Get a list of files in the folder
    files_in_folder = os.listdir(folder_path)

    # Filter files that are CSV files and match the given names
    matching_files = [file_name for file_name in files_in_folder if file_name.endswith('.csv') and file_name in file_names]

    return matching_files

def get_cancer_type(value):
    cancer_types = ["breast", "colorectal", "lung", "prostate"]

    # Convert the input value to lowercase for case-insensitive comparison
    lowercase_value = value.lower()
    
    # Check if any cancer type is present in the lowercase version of the value
    for cancer_type in cancer_types:
        if cancer_type.lower() in lowercase_value:
            return cancer_type
    
    # If no match is found, return None or any other value you prefer
    return None
    
def skipRowsArray(value):
    length=len(value)
    result=list(range(length+1))
    
    result.append(length+2)
    
    return result
  

def findPatienNumber(header_row,df):
    flag=False
    columnDiscription=''
    unwantedRows=''
    
 
    if "Patient Number*" in  header_row or "Patient Number" in  header_row:
            columnDiscription=df.iloc[0].values
    else:
        if "Patient Number*" in df.iloc[:1].values or "Patient Number" in  df.iloc[:1].values:
            flag=True
            columnDiscription=df.iloc[1].values
        else:
            for i in range(10):
                row=df.iloc[:i].values
                if "Patient Number*" in row or "Patient Number" in  row:
                    unwantedRows=df.iloc[:i-1].values
                    columnDiscription=df.iloc[i].values
                    break
    return flag,columnDiscription,unwantedRows