from diffprivlib.mechanisms import Laplace
import pandas as pd
import numpy as np
from scipy.stats import laplace
import re
from pycanon import anonymity
import math
import os
import json

def fixDeleteFreeText(value):
    # Check if the value is a string and has at least two characters
    if isinstance(value, str) and len(value) >= 2:
        # Check if the first character is an alphabet character and the rest are numerical
        if value[0].isalpha() and value[1:].isdigit():
            # Remove the first character
            return float(value[1:])
        if value[0].isalpha() and value[1].isalpha():
            return None
    return value
    
        
def add_noise(value,epsilon):
      # Laplace noise generation
    scale = 1 / epsilon
    noise = np.random.laplace(loc=0, scale=scale)
    return value + noise
def apply_differential_privacy(data, columns, epsilons, max_changes, min_values, round_values):
    """
    Apply differential privacy to specified columns in a Pandas DataFrame and limit the change.

    :param data: Pandas DataFrame with numerical data
    :param columns: List of column names to which differential privacy should be applied
    :param epsilons: List of epsilon values for each column
    :param max_changes: List of max_change values for each column
    :param min_values: List of min_value values for each column
    :param round_values: Whether to round the values after applying differential privacy (default: False)
    """
    for column, epsilon, max_change, min_value,round_values in zip(columns, epsilons, max_changes, min_values,round_values):
        if column in data.columns:
            data[column] = data[column].apply(lambda x: fixDeleteFreeText(x))
            

            data[column] = pd.to_numeric(data[column], errors='coerce')            

            if data[column].dtype.kind in 'biufc':
            # Set sensitivity to max_change to limit the change to max ±max_change
            
                for i, value in enumerate(data[column]):
                    if value is not None and not np.isnan(value):

                        noisy_value = add_noise(value, epsilon,)

                        # Apply max_change if specified
                        if max_change is not None:
                            if (round_values):
                                while abs(noisy_value - value) >= max_change or round(noisy_value) == value:
                                    noisy_value = add_noise(value, epsilon)
                        
                            else:
                                while abs(noisy_value - value) >= max_change or noisy_value == value:
                                    noisy_value = add_noise(value, epsilon)
                            
                            if value==0 and max_change==1 and min_value==0 and round_values==True:
                                noisy_value=1

                        # Apply min_value if specified
                        
                        
                        if min_value is not None:
                            noisy_value = max(noisy_value, min_value)

                        if round_values:
                            noisy_value = round(noisy_value)
                        
                        data.at[i, column] = noisy_value
                        

   
def idsMappingPatienNumber(patient_number,data_name,folder_path):
    ids_mapping={
    "001": "953",
    "002": "572",
    "003": "249",
    "004": "430",
    "005": "579",
    "006": "746",
    "007": "529",
    "008": "393",
    "009": "338",
    }

    json_file = os.path.join(folder_path,"id_mapping_"+data_name+".json")
    with open(json_file, 'r') as f:
        json_data = json.load(f)
        
    if patient_number == np.nan or patient_number=='nan' or patient_number=="" or (patient_number is None) or patient_number==" " :
        return patient_number     
    
    prefix, suffix = patient_number.split('-')
    if suffix in json_data:
        suffix=json_data[suffix]
    mapped_prefix = ids_mapping.get(prefix, prefix) # Default to original if not found in mapping
    return f'{mapped_prefix}-{suffix}'

def idsMappingProvider(provider):
    ids_mapping={
    "001": "953",
    "002": "572",
    "003": "249",
    "004": "430",
    "005": "579",
    "006": "746",
    "007": "529",
    "008": "393",
    "009": "338",
    "1": "953",
    "2": "572",
    "3": "249",
    "4": "430",
    "5": "579",
    "6": "746",
    "7": "529",
    "8": "393",
    "9": "338",
    1: "953",
    2: "572",
    3: "249",
    4: "430",
    5: "579",
    6: "746",
    7: "529",
    8: "393",
    9: "338",
    }            
    mapped_provider= ids_mapping.get(provider, provider) # Default to original if not found in mapping
    return f'{mapped_provider}'

def ethnicityGrouping(df,columnDescription):
    descriptionList=list(columnDescription)
    found_irish = [element for element in descriptionList if isinstance(element, str) and 'Irish' in element]
    flag=False
    
    if found_irish:
        flag=True

        
    ethnicity_mapping = {
    1: 'White',
    2: 'White',
    3: 'White',
    4: 'African',
    5: 'African',
    6: 'Asian',
    7: 'Asian',
    8: 'Asian',
    9: 'Arabic',
    10: 'Mixed',
    11: 'Other',
    "1": 'White',
    "2": 'White',
    "3": 'White',
    "4": 'African',
    "5": 'African',
    "6": 'Asian',
    "7": 'Asian',
    "8": 'Asian',
    "9": 'Arabic',
    "10": 'Mixed',
    "11": 'Other',
    "Greek":"White"
            }
    
    if flag:
        ethnicity_mapping={
            1: 'White',
            2: 'White',
            3: 'White',
            4: 'White',
            5: 'African',
            6: 'African',
            7: 'Asian',
            8: 'Asian',
            9: 'Asian',
            10: 'Arabic',
            11: 'Mixed',  
            12:'Other',
            "1": 'White',
            "2": 'White',
            "3": 'White',
            "4": 'White',
            "5": 'African',
            "6": 'African',
            "7": 'Asian',
            "8": 'Asian',
            "9": 'Asian',
            "10": 'Arabic',
            "11": 'Mixed',  
            "12":'Other',
            "Greek":"White"

        } 

    # Step 3: Replace values in the 'Ethnicity' column using the mapping dictionary
    df['Ethnicity'] = df['Ethnicity'].replace(ethnicity_mapping) 

def split_string(s):
    # Define a pattern that includes various delimiters: commas, semicolons, slashes, spaces, etc.
    pattern = r'[,;/]+'
    # Split the string based on the pattern and filter out any empty strings
    return [word.strip() for word in re.split(pattern, s) if word.strip()]

def process_string(input_str):
   pattern = r'^[A-Za-z]\d\d[A-Za-z]{2}(\d{1,2})?$'
   startSpaceFlag=False
   endSpaceFlag=False
   input_str = input_str.strip()
   substrings = split_string(input_str)
   processed_substrings = []

   for substring in substrings:
    if substring.startswith(" "):
        startSpaceFlag=True
        substring=substring.lstrip()
    if substring.endswith(" "):
        endSpaceFlag=True
        substring=substring.rstrip()
       
    if bool(re.match(pattern, substring)):
        levelUpSubString=substring
        if len(substring)==7:
            levelUpSubString=substring[:-2]
            if startSpaceFlag:
                levelUpSubString=" " + levelUpSubString
            if endSpaceFlag:
                levelUpSubString= levelUpSubString + " "
            
        processed_substrings.append(levelUpSubString)        

   return ','.join(processed_substrings)
   


def medicationLevelUp(cancerType,df):
        if(cancerType=='breast'):
           columns_to_check = ['Medications','Type of CTX','Type of CIT','Type of CRT','Type of RT','Type of HT','Type of TT','Type of IT']  
           for column in columns_to_check:
               if column in df.columns:
                   df[column] = df[column].astype(str)
                   df[column]=df[column].apply(process_string) 
        
        if(cancerType=='colorectal'):
           columns_to_check = ['Type of CT','Type of CRT','Type of CIT']  
           for column in columns_to_check:
               if column in df.columns:
                   df[column] = df[column].astype(str)
                   df[column]=df[column].apply(process_string)
        
        if(cancerType=='lung'):
           columns_to_check = ['Type of CT','Type of CRT','Type of CIT','Type of TT','Type of IT']  
           for column in columns_to_check:
               if column in df.columns:
                   df[column] = df[column].astype(str)
                   df[column]=df[column].apply(process_string)

        if(cancerType=='prostate'):
           columns_to_check = ['Medications']  
           for column in columns_to_check:
               if column in df.columns:
                   df[column] = df[column].astype(str)
                   df[column]=df[column].apply(process_string)


def all_values_match(s, pattern):
    values = re.split(r',|;|/', s)
    return all(re.match(pattern, value) for value in values)


def remove_dot_and_digit(input_string):
    input_string=str(input_string)
    processed_substrings = []
    input_string = input_string.strip()
    substrings = split_string(input_string)
    pattern = r'[A-Za-z]\d{2}(\.\d)?$'
    
    for substring in substrings:
        if re.match(pattern,substring):
            processed_substrings.append(substring)

    
    substrings =','.join(processed_substrings)
    
    if type(substrings) == int  or substrings== None or substrings=="":
        return substrings
    return re.sub(r'\.\d', '', substrings)
   

def calculateJaccardSimilarity(dfOriginal,dfAnonymized,data_name,sheat):
    QI=[]
    if sheat=='General info':
        if 'Ethnicity' in dfOriginal.columns:
            if data_name=='breast':
                QI=['Medical History', 'Ethnicity', 'Medications']
            if data_name=='colorectal':
                QI=['Medical History', 'Ethnicity']
            if data_name=='lung':
                QI=['Medical History', 'Ethnicity']
            if data_name=='prostate':
                QI=['Medical History', 'Ethnicity', 'Medications']
        else:
            if data_name=='breast':
                QI=['Medical History', 'Medications']
            if data_name=='colorectal':
                QI=['Medical History']
            if data_name=='lung':
                QI=['Medical History']
            if data_name=='prostate':
                QI=['Medical History', 'Medications']
            
    
    if sheat=='Treatment':
        if data_name=='breast':
            QI=['Type of CTX','Type of CIT','Type of CRT','Type of RT','Type of HT','Type of TT','Type of IT']
        if data_name=='colorectal':
            QI=['Type of CT','Type of CRT','Type of CIT']
        if data_name=='lung':
            QI=['Type of CT','Type of CRT','Type of CIT','Type of TT','Type of IT']

    if QI:
        dfAnonymized.replace('nan', np.nan, inplace=True)
        categories1 = set(dfOriginal[QI].astype(str).values.flatten())
        categories2 = set(dfAnonymized[QI].astype(str).values.flatten())
    
        intersection = len(categories1.intersection(categories2))
        union = len(categories1.union(categories2))
    
        jaccard_similarity_coefficient = intersection / union if union != 0 else 0
        return jaccard_similarity_coefficient
        
    return None

def calculate_generalization_level(original_df, anonymized_df):
    #column_name = "Medications"
    metrics = {}
    columns = ['Medical History', 'Ethnicity', 'Medications','Type of CT','Type of CTX','Type of CIT','Type of CRT','Type of RT','Type of HT','Type of TT','Type of IT']
    for column_name in columns:
        if column_name in original_df.columns :
            original_df[column_name] =original_df[column_name].fillna('').astype(str)
            anonymized_df[column_name] =anonymized_df[column_name].fillna('').astype(str)

            valuesOr = set([item.strip() for sublist in original_df[column_name].str.split(',') for item in sublist])
            valuesAn = set([item.strip() for sublist in anonymized_df[column_name].str.split(',') for item in sublist])

            # Calculating metrics
            common_values = valuesOr.intersection(valuesAn)
            exclusive_values_original = valuesOr - valuesAn
            exclusive_values_anonymized = valuesAn - valuesOr

            total_unique_values_original = len(valuesOr)
            total_unique_values_anonymized = len(valuesAn)
            common_values_count = len(common_values)
            exclusive_original_count = len(exclusive_values_original)
            exclusive_anonymized_count = len(exclusive_values_anonymized)

            percentage_overlap = (common_values_count / total_unique_values_anonymized) * 100 if total_unique_values_anonymized else 0
            percentage_retained = (common_values_count / total_unique_values_original) * 100 if total_unique_values_original else 0
            percentage_new_anonymized = (exclusive_anonymized_count / total_unique_values_anonymized) * 100 if total_unique_values_anonymized else 0

            metrics = {
                "Total Unique Values Original": total_unique_values_original,
                "Total Unique Values Anonymized": total_unique_values_anonymized,
                "Common Values Count": common_values_count,
                "Exclusive Values in Original": exclusive_original_count,
                "Exclusive Values in Anonymized": exclusive_anonymized_count,
                "Percentage Overlap": percentage_overlap,
                "Percentage of Original Data Retained": percentage_retained,
                "Percentage of New Values in Anonymized": percentage_new_anonymized
            }

            print("Metrics fot column {} is: {}".format(column_name,metrics))
        
       