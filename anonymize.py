from utils.types import AnonMethod
import os
import argparse
import numpy as np
import pandas as pd
from metrics import NCP

from algorithms import (
        k_anonymize,
        read_tree,
        apply_differential_privacy)
from datasets import read_config_file,convert_columns_to_indexes,extract_columns_from_hierarchies,find_csv_file
from utils.data import read_raw, write_anon



class Anonymizer():
    def __init__(self,excel_path=None,sheat_Name=None,data_name=None,sheat=None):
        self.method ='mondrian'
        self.k =8
        self.sheat_Name=sheat_Name
        self.data_name = data_name
        self.csv_name=self.data_name+'-'+sheat_Name
        self.csv_path = self.csv_name+'.csv'
       

        # Data path
        self.path = os.path.join('data', self.data_name)  # trailing /
        # Dataset path
        xlxs_Path=os.path.join(self.path, self.data_name+'.xlsx')
        
        #keep the header and the second row
        df=pd.read_excel(xlxs_Path,sheet_name=sheat)
        self.first_row=df.columns.tolist()
        self.third_row=df.iloc[1].values
        
        del df
        df=pd.read_excel(xlxs_Path,skiprows=(0,2),sheet_name=sheat)
       
        # headers=df.columns.tolist()
        # df.columns=first_row 
        # df.loc[0]=headers
        # df.loc[1]=third_row
        
        # new_df=pd.DataFrame(columns=first_row)
        # new_df.loc[1]=third_row
        # new_df=new_df._append([new_df,df[:1]],ignore_index=True)
        
    
      

       
       

        #add a ID column
        df.insert(0, 'ID', range(1, len(df) + 1))
        self.csv_file_path = os.path.join(self.path, self.csv_path)
      
        
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
        

        df.to_csv(self.csv_file_path,index=False,sep=';')
        self.data_path = os.path.join(self.path, self.csv_path)
        
       
        # Generalization hierarchies path
        self.hierarchies_path=os.path.join(self.path,'hierarchies')
        
        
        self.gen_path = os.path.join(
            self.hierarchies_path,
            sheat_Name)  # trailing /

        # folder for all results
        res_folder = os.path.join(
            'results', 
            self.data_name)
        
        resultFolder=os.path.join('results',self.data_name)
        self.resultFolder=resultFolder
      
        # path for anonymized datasets
        self.anon_folder = res_folder  # trailing /
        
        os.makedirs(self.anon_folder, exist_ok=True)
        os.makedirs(self.resultFolder, exist_ok=True)

    def anonymize(self):
        
        data = pd.read_csv(self.csv_file_path, delimiter=';')

        hierarchiesToColumnsNames=extract_columns_from_hierarchies(self.gen_path)
        data_params=convert_columns_to_indexes(data,hierarchiesToColumnsNames)

        if(data_params==None):
            destFolder=os.path.join(self.resultFolder,self.data_name+"-"+self.sheat_Name+"-anonymized.csv")
            data = data.drop('ID', axis=1)
            data.to_csv(destFolder,index=False,sep=';')
        
        else:
   
            ATT_NAMES = list(data.columns)
            
            QI_INDEX = data_params

            QI_NAMES = list(np.array(ATT_NAMES)[QI_INDEX])
            IS_CAT = [True] * len(QI_INDEX) # is all cat because all hierarchies are provided
            SA_INDEX = [index for index in range(len(ATT_NAMES)) if index not in QI_INDEX]
            SA_var = [ATT_NAMES[i] for i in SA_INDEX]

            ATT_TREES = read_tree(
                self.gen_path, 
                self.data_name, 
                ATT_NAMES, 
                QI_INDEX, IS_CAT)

            raw_data, header = read_raw(
                self.path, 
                self.csv_name, 
                QI_INDEX, IS_CAT)

            anon_params = {
                "name" :self.method,
                "att_trees" :ATT_TREES,
                "value" :self.k,
                "qi_index" :QI_INDEX, 
                "sa_index" :SA_INDEX
            }
            anon_params.update({'data': raw_data})

            print(f"Anonymize with {self.method}")
            anon_data, runtime = k_anonymize(anon_params)

            # Write anonymized table
            if anon_data is not None:
                nodes_count = write_anon(
                    self.anon_folder, 
                    anon_data, 
                    header, 
                    self.k,
                    self.data_name,
                    self.sheat_Name)

                # Normalized Certainty Penalty
            ncp = NCP(anon_data, QI_INDEX, ATT_TREES)
            ncp_score = ncp.compute_score()

            # Discernibility Metric

            # raw_dm = DM(raw_data, QI_INDEX, self.k)
            # raw_dm_score = raw_dm.compute_score()

            # anon_dm = DM(anon_data, QI_INDEX, self.k)
            # anon_dm_score = anon_dm.compute_score()

            # # Average Equivalence Class

            # raw_cavg = CAVG(raw_data, QI_INDEX, self.k)
            # raw_cavg_score = raw_cavg.compute_score()

            # anon_cavg = CAVG(anon_data, QI_INDEX, self.k)
            # anon_cavg_score = anon_cavg.compute_score()

            print(f"NCP score (lower is better): {ncp_score:.3f}")
            # print(f"CAVG score (near 1 is better): BEFORE: {raw_cavg_score:.3f} || AFTER: {anon_cavg_score:.3f}")
            # print(f"DM score (lower is better): BEFORE: {raw_dm_score} || AFTER: {anon_dm_score}")
            print(f"Time execution: {runtime:.3f}s")
            
        targetFilesNames=[self.data_name+"-"+self.sheat_Name+"-anonymized.csv",self.data_name +"-"+self.sheat_Name+ "-anonymized_" + str(self.k) + ".csv"]
        targetName=find_csv_file(str(self.resultFolder),targetFilesNames)
        self.targetName=targetName[0]
            
        # return self.resultFolder,targetName[0]
            


            # return self.anon_folder
            # raw_cavg_score, anon_cavg_score, raw_dm_score, anon_dm_score
    def csvToXlsxResultsFiles(self,delimiter=';'):
        # Ensure the folder path exists
        if not os.path.exists(self.anon_folder):
            print(f"The folder path '{self.anon_folder}' does not exist.")
            return

        # Create the full path for the target CSV file
        targetCsvFilePath = os.path.join(self.anon_folder,  self.targetName)

        # Check if the target CSV file exists
        if not os.path.exists(targetCsvFilePath):
            print(f"The target CSV file '{ self.targetName}' does not exist in the specified folder.")
            return

        print(f"Converting { self.targetName} to XLSX...")

        # Read CSV file into a DataFrame
        df = pd.read_csv(targetCsvFilePath, delimiter=delimiter)
        headers=df.columns.tolist()
        df.columns=self.first_row

        
        df = pd.concat([df.iloc[:0], pd.DataFrame([headers], columns=df.columns), df.iloc[0:]])

        # Insert a new row with values from the third row at the second position
        df = pd.concat([df.iloc[:1], pd.DataFrame([self.third_row], columns=df.columns), df.iloc[1:]])

        # Create the path for the output XLSX file
        xlsx_file_path = os.path.join(self.anon_folder, os.path.splitext( self.targetName)[0] + ".xlsx")

        # Save DataFrame to XLSX file
        df.to_excel(xlsx_file_path, index=False)

        # Delete the original CSV file
        os.remove(targetCsvFilePath)

        print(f"Conversion complete. XLSX file saved at {xlsx_file_path}. CSV file deleted.")
    

    def xlsx_to_excel(self,excel_sheets):
        outputPath=os.path.join(self.anon_folder,self.data_name+'.xlsx')
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
                 
def main(excel_path):
    excel_Sheats={
            "General info": "General_info",
            "Timepoints": "Timepoints",
            "Baseline":"Baseline",
            "Histology - Mutations": 'Histology_Mutations',
            "Treatment":"Treatment",
            "Lab Results": "Lab_Results"
        }
    sheats_names=pd.ExcelFile(excel_path).sheet_names
   
    excel_file_name = os.path.splitext(os.path.basename(excel_path))[0]

    
    for sheat_name in sheats_names:
        sheat=excel_Sheats[sheat_name]
        anonymizer = Anonymizer(excel_path,sheat_Name=sheat,data_name=excel_file_name,sheat=sheat_name)
        anonymizer.anonymize()
        anonymizer.csvToXlsxResultsFiles()
    
    anonymizer.xlsx_to_excel(excel_sheets=excel_Sheats)


            
if __name__ == '__main__':
    main('data/breast/breast.xlsx')