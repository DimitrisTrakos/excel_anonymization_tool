import os
import numpy as np
import pandas as pd
import argparse


from algorithms import apply_differential_privacy,ethnicityGrouping,medicationLevelUp,remove_dot_and_digit,calculateJaccardSimilarity,calculate_generalization_level,idsMappingPatienNumber,idsMappingProvider
from datasets import read_config_file,get_cancer_type,skipRowsArray,findPatienNumber
class Anonymizer():
    def __init__(self,excel_path=None,sheat_Name=None,data_name=None,sheat=None,data_provider=None):
        self.sheat_Name=sheat_Name
        self.excelName=data_name
        self.data_name = get_cancer_type(data_name)
        self.fileName=self.excelName+"-"+self.sheat_Name+"-anonymized.xlsx"
        self.dataProvider=data_provider
        
        # Dataset path
        xlxs_Path=excel_path
        # Data path
        self.path = os.path.join('data', self.data_name)  # trailing /
        # Generalization hierarchies path
        self.hierarchies_path=os.path.join(self.path,'hierarchies')
        self.gen_path = os.path.join(
            self.hierarchies_path,
            sheat_Name)  # trailing /
       
        
        #keep the header and the second row
        df=pd.read_excel(xlxs_Path,sheet_name=sheat)
        
        column_indices_with_value = [i for i, column in enumerate(df.columns) if df[column].eq('Year of birth').any()]
        
        column_name_to_drop = df.columns[column_indices_with_value]
        df.drop(column_name_to_drop, axis=1,inplace=True)
      
            
        self.header_row=df.columns.tolist()
       
        self.flag, self.columnDiscription, self.unwantedRows=findPatienNumber(self.header_row,df)
      
        
        
        
        if(len(self.unwantedRows)==0 and self.flag==False ):
            skipRows=[1]
        elif(self.flag==True):
            skipRows=[0,2]
        else:
            skipRows=skipRowsArray(self.unwantedRows)
         
        del df
        df=pd.read_excel(xlxs_Path,skiprows=skipRows,sheet_name=sheat)
        
     
    
        if sheat=='General info' and 'Year of birth' in df.columns:
            df.drop('Year of birth',axis=1,inplace=True)
        
     
       
        dpArgumentsFolder=os.path.join(self.path,'differetial_p_arguments')
        
        dfargumentSheatFolder=os.path.join(dpArgumentsFolder,sheat_Name)
        txtFilePath=os.path.join(dfargumentSheatFolder,self.data_name+'.txt')
        differentialPrParams=read_config_file(txtFilePath)
        
        

        if(differentialPrParams != None):
            columnsList=differentialPrParams['columns']
            epsilonList=differentialPrParams['epsilons']
            maxChangeList=differentialPrParams['max_changes']
            minValueList=differentialPrParams['min_values']
            roundValuesList=differentialPrParams['round_values']
            apply_differential_privacy(df,columnsList,epsilonList,maxChangeList,minValueList,roundValuesList)
        
        
        dfOriginal=df.copy()
        medicationLevelUp(self.data_name,df)
        
        if 'Medical History' in df.columns:
            df['Medical History'] = df['Medical History'].fillna('')

            df['Medical History'] = df['Medical History'].apply(remove_dot_and_digit)
            df['Medical History'].replace('', np.nan, inplace=True)

        if(sheat=='General info') and "Ethnicity" in df.columns:
            ethnicityGrouping(df,self.columnDiscription)
        
        if "Patient Number*" in df.columns:
            df['Patient Number*'] = df['Patient Number*'].apply(idsMappingPatienNumber)
           
        
        if "Patient Number" in df.columns:
            df['Patient Number'] = df['Patient Number'].apply(idsMappingPatienNumber)
        
        if (sheat=='General info') and "Provider*" in df.columns:
            df['Provider*'] = df['Provider*'].apply(idsMappingProvider)
           
        
        if (sheat=='General info') and "Provider" in df.columns:
            df['Provider'] = df['Provider'].apply(idsMappingProvider)




        
        dfAnonymized=df.copy()
        JaccardSimilarity=calculateJaccardSimilarity(dfOriginal,dfAnonymized,self.data_name,sheat)
        if JaccardSimilarity:
              print(f"Jaccard similarity coefficient for {self.sheat_Name} :")
              print(JaccardSimilarity)
        # k=calculateK(df,self.data_name,sheat,self.excelName)
        # print(k)
        # self.k=k
        
        calculate_generalization_level(dfOriginal,dfAnonymized)
             
        if(self.flag==True):
            headers=df.columns.tolist()
            df.columns=self.header_row   
            df = pd.concat([df.iloc[:0], pd.DataFrame([headers], columns=df.columns), df.iloc[0:]])
            df = pd.concat([df.iloc[:1], pd.DataFrame([self.columnDiscription], columns=df.columns), df.iloc[1:]])
        
        if(len(self.unwantedRows)==0 and self.flag==False):
            
            df = pd.concat([df.iloc[:0], pd.DataFrame([self.columnDiscription], columns=df.columns), df.iloc[0:]])

        if(len(self.unwantedRows)!= 0):
            headers=df.columns.tolist()
            df.columns=self.header_row
            
            df = pd.concat([df.iloc[:0], pd.DataFrame([headers], columns=df.columns), df.iloc[0:]])

            for i in range(len(self.unwantedRows)):
                df = pd.concat([df.iloc[:i], pd.DataFrame([self.unwantedRows[i]], columns=df.columns), df.iloc[i:]])
            
            df = pd.concat([df.iloc[:len(self.unwantedRows)+1], pd.DataFrame([self.columnDiscription], columns=df.columns), df.iloc[len(self.unwantedRows)+1:]])
        
        # folder for all results
        pre_res_folder = os.path.join(
            'results',
            self.data_name)
        
        res_folder=os.path.join(
            pre_res_folder,self.dataProvider
        )
        
      
        # path for anonymized datasets
        self.anon_folder = res_folder  # trailing /
        
        # name for result file
        self.resultFilename=os.path.join(self.anon_folder,self.fileName)

        os.makedirs(self.anon_folder, exist_ok=True)
        
        
        df.to_excel(self.resultFilename, sheet_name=sheat, index=False)
        print(f"Conversion complete. XLSX file saved at {self.resultFilename}.")
    
    # def get_K(self):
    #     return self.k     
    
    def xlsx_to_excel(self,excel_sheets):
        outputPath=os.path.join(self.anon_folder,self.excelName+'_anonym'+'.xlsx')
        excel_writer = pd.ExcelWriter(outputPath, engine='xlsxwriter')

        for xlsx_file in os.listdir(self.anon_folder):
            if('anonymized' in xlsx_file):
                file_name = xlsx_file.split('-')[1]
            
                replacement_key = next((key for key, value in excel_sheets.items() if value == file_name), None)
                if replacement_key:
                    file_name = replacement_key
                
                if xlsx_file.endswith('.xlsx'):
                    # Read the Excel file
                    df = pd.read_excel(os.path.join(self.anon_folder, xlsx_file))
                    df.to_excel(excel_writer, sheet_name=file_name, index=False)

        # Save the Excel file
        excel_writer._save()
    
    def deleteFiles(self,names_to_delete):
        # Get the list of files in the folder
        files = os.listdir(self.anon_folder)

        # Iterate through each file in the folder
        for file_name in files:
            # Check if the file is an xlsx file and contains any of the specified names
            if file_name.endswith(".xlsx") and any(name in file_name for name in names_to_delete):
                # Construct the full path to the file
                file_path = os.path.join(self.anon_folder, file_name)

                # Delete the file
                os.remove(file_path)
                print(f"Deleted: {file_path}")
    
                 
def exec_anonymization(excel_path):
    dataProvider=excel_path.split('/')[-2]
    excel_Sheats={
            "General info": "General_info",
            "Timepoints": "Timepoints",
            "Baseline":"Baseline",
            "Histology - Mutations": 'Histology_Mutations',
            "Treatment":"Treatment",
            "Lab Results": "Lab_Results"
        }
    namesToDelete= {"General_info", "Timepoints", "Baseline", 'Histology_Mutations', "Treatment", "Lab_Results"}

    sheats_names=pd.ExcelFile(excel_path).sheet_names
   
    excel_file_name = os.path.splitext(os.path.basename(excel_path))[0]

    kSheat=[]
    for sheat_name in sheats_names:
        sheat=excel_Sheats[sheat_name]
        anonymizer = Anonymizer(excel_path=excel_path,sheat_Name=sheat,data_name=excel_file_name,sheat=sheat_name,data_provider=dataProvider)
        # k=anonymizer.get_K()
        # kSheat.append(k)
    
    # print(f"Final k for  {excel_file_name} is {min(kSheat)}")
    
    anonymizer.xlsx_to_excel(excel_sheets=excel_Sheats)
    anonymizer.deleteFiles(names_to_delete=namesToDelete)
    
def main():
    parser = argparse.ArgumentParser(description='Description of your script.')
    parser.add_argument('arg1', type=str, help='Folder Path...')
    args = parser.parse_args()
    folder_path = args.arg1
    cancer_types = ["breast", "colorectal", "lung", "prostate"]
    excel_suffixes = ["_cancer.xls", "_cancer_training.xls", "_cancer_observational.xls", "_cancer_feasibility.xls","_cancer.xlsx", "_cancer_training.xlsx", "_cancer_observational.xlsx", "_cancer_feasibility.xlsx"]
    possible_file_names = [f"{cancer_type}{suffix}".lower() for cancer_type in cancer_types for suffix in excel_suffixes]    
    for filename in os.listdir(folder_path):
        if filename.lower() in possible_file_names:
            file_path = os.path.join(folder_path, filename)
            exec_anonymization(file_path)
    

    
   

            
if __name__ == '__main__':
    main()
   
            