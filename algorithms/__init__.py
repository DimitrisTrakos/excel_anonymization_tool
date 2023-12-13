from diffprivlib.mechanisms import Laplace
import pandas as pd
import numpy as np
from scipy.stats import laplace
import re
from pycanon import anonymity


        
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
            data[column] = data[column].astype(float)

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

                        # Apply min_value if specified
                        
                        
                        if min_value is not None:
                            noisy_value = max(noisy_value, min_value)

                        if round_values:
                            noisy_value = round(noisy_value)
                        
                        data.at[i, column] = noisy_value
                # mechanism = Laplace(epsilon=epsilon, sensitivity=max_change)
                # noisy_values = data[column].apply(lambda x: mechanism.randomise(x))
                # noisy_values = noisy_values.round(2)

                # if min_value is not None:
                #     noisy_values = noisy_values.clip(lower=min_value)

                # if round_values:
                #     noisy_values = noisy_values.apply(lambda x: int(x) if x is not None and not np.isnan(x) else x)
                #     noisy_values = noisy_values.apply(lambda x: str(x))

            

def ethnicityGrouping(df,columnDescription):
    searchString='Irish'
    descriptionList=list(columnDescription)
    flag=False
    for desc in descriptionList:
        if searchString in desc:
            flag=True
            break
        
    ethnicity_mapping = {
    1: 'White',
    2: 'White',
    3: 'White',
    4: 'Black',
    5: 'Black',
    6: 'Asian',
    7: 'Asian',
    8: 'Asian',
    9: 'Arabic',
    10: 'Mixed',
    11: 'Other'
            }
    
    if flag:
        ethnicity_mapping={
            1: 'White',
            2: 'White',
            3: 'White',
            4: 'White',
            5: 'Black',
            6: 'Black',
            7: 'Asian',
            8: 'Asian',
            9: 'Asian',
            10: 'Arabic',
            11: 'Mixed',  
            12:'Other'
        }


    # Step 3: Replace values in the 'Ethnicity' column using the mapping dictionary
    df['Ethnicity'] = df['Ethnicity'].replace(ethnicity_mapping) 

def process_string(input_str):
   input_str = re.sub(r'\s', '', input_str)

   if input_str.isdigit() or input_str == 'None':
        return input_str
        
   substrings = re.split(r'[,\s"]+', input_str)
   processed_substrings = []

   for substring in substrings:
    if len(substring) == 7 and substring[-3].isalpha():
        processed_substrings.append(substring[:-2])
    else:
        processed_substrings.append(substring)

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
    
def remove_dot_and_digit(input_string):
    
    if type(input_string) == int  or input_string== None or input_string=="":
        return input_string
    
    return re.sub(r'\.\d', '', input_string)

def calculateK(df,data_name,sheat,excelName):
    if(sheat=='General info'):
        if data_name==('breast' or 'lung' or 'prostate'):
            QI = ["Gender,""Case"]
            SA=['Patient Number*']
            df2 = df.dropna(subset=QI)
            
        if data_name=='lung':
            QI = ["Gender","Case*"]
            SA=['Patient Number*']
            df2 = df.dropna(subset=QI)
        
        k=anonymity.k_anonymity(df2,QI)
    
    if(sheat=='Baseline'):
        if  data_name=='breast':
            QI = ["Mammography","Ultrasound","CT","MRI","PET/CT","Histopathology Image","Xray","Left Breast BIRADS classification","Right  Breast BIRADS classification"]
            SA=['Patient Number*']
            df2 = df.dropna(subset=QI)
            
        
        if  data_name=='prostate':
            QI = ["CT scan","PET","MRI","Bone Scintigraphy Scan","Histopathology Image","Xray","Maximum PIRADS"]
            SA=['Patient Number*']
            df2 = df.dropna(subset=QI)
        
        if  data_name=='lung':
            QI = ["CT","MRI","PET-CT","Histopathology Image","Xray","Stage"]
            SA=['Patient Number*']
            df2 = df.dropna(subset=QI)
        
        if  data_name=='colorectal':
            QI = ["CT","MRI","PET-CT","Histopathology Image","Xray","TNM Staging"]
            SA=['Patient Number*']
            df2 = df.dropna(subset=QI)
        
        k=anonymity.k_anonymity(df2,QI)
    
    if(sheat=='Timepoints'):
        if data_name=='breast':
            QI = ["Label*","Mammography","Ultrasound","CT","MRI","PET/CT","Histopathology Image","Xray","Left Breast BIRADS classification","Right  Breast BIRADS classification"]
            SA=['Patient Number*']
            df2 = df.dropna(subset=QI)
        
        if  data_name=='prostate':
            QI = ["Label","CT scan","PET","MRI","Bone Scintigraphy Scan","Histopathology Image","Xray","Maximum PIRADS"]
            SA=['Patient Number*']
            df2 = df.dropna(subset=QI)
        
        if  data_name=='lung':
            QI = ["Label*","CT","PET","MRI","PET-CT","Histopathology Image","Xray","Stage"]
            SA=['Patient Number*']
            df2 = df.dropna(subset=QI)
        
        if  data_name=='lung':
            QI = ["Label*","CT","PET","MRI","PET-CT","Histopathology Image","Xray","Stage"]
            SA=['Patient Number*']
            df2 = df.dropna(subset=QI)
        
        k=anonymity.k_anonymity(df2,QI)

    
    if(sheat=='Treatment'):
        if  data_name=='breast':
            QI = ["Treatment label*","Treatment","Surgery",'Chemotherapy (CTX)','ChemoImmunotherapy (CIT)','Chemoradiotherapy (CRT)','Radiation therapy','Boost','Targeted therapy','Immune therapy','ADJUVANT TREATMENT','NEOADJUVANT TREATMENT']
            SA=['Patient Number*']
            df2 = df.dropna(subset=QI)

        
        if  data_name=='prostate':
            QI = ["Treatment label","Treatment","lymph node dissection"]
            SA=['Patient Number*']
            df2 = df.dropna(subset=QI)

        
        if  data_name=='lung':
            QI = ["Treatment label*","Chemotherapy (CT)","Chemoradiotherapy (CRT)","Chemoimmunoherapy (CIT)","Targeted-therapy (TT)","Immunotherapy (IT)","Radiation therapy (RT)","Post-treatment surgery"]
            SA=['Patient Number*']
            df2 = df.dropna(subset=QI)

        
        if  data_name=='colorectal':
            QI = ["Treatment label","Surgery","Chemotherapy (CT)",'Chemoradiotherapy (CRT)','Chemoimmunotherapy (CIT)','Radiation therapy (RT)','Radiation therapy','Boost','Post-treatment surgery']
            SA=['Patient Number*']
            df2 = df.dropna(subset=QI)
    
    if(QI):    
        k=anonymity.k_anonymity(df2,QI)
        print(f"Calculating k for  {sheat} of {excelName}..")
        print(f"K is: {k}")
        return k
    return 10000
    