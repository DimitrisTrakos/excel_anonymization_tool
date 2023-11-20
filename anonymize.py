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
from datasets import read_config_file,convert_columns_to_indexes,extract_columns_from_hierarchies
from utils.data import read_raw, write_anon


class Anonymizer():
    def __init__(self,excel_path=None,sheat_Name=None,data_name=None):
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
        df=pd.read_excel(xlxs_Path, skiprows=[0, 2])
        
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

        # data_params = read_config_file_k_anonymity(data,'data/breast/k_anonymity_arguments/breast_1.txt')
        hierarchiesToColumnsNames=extract_columns_from_hierarchies(self.gen_path)
        data_params=convert_columns_to_indexes(data,hierarchiesToColumnsNames)

        if(data_params==None):
            destFolder=os.path.join(self.resultFolder,self.data_name+"-"+self.sheat_Name+"_anonymized.csv")
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

            return ncp_score,
            # raw_cavg_score, anon_cavg_score, raw_dm_score, anon_dm_score


def main(excel_path):
    excel_Sheats={
            "General info": "General_info",
            "Timepoints": "Timepoints",
            "Baseline":"Baseline",
            "Histology - Mutations": 'Histology-Mutations',
            "Treatment":"Treatment",
            "Lab Results": "Lab_Results"
        }
    sheats_names=pd.ExcelFile(excel_path).sheet_names
    excel_file_name = os.path.splitext(os.path.basename(excel_path))[0]

    for sheat_name in sheats_names:
        sheat=excel_Sheats[sheat_name]
        anonymizer = Anonymizer(excel_path,sheat_Name=sheat,data_name=excel_file_name)
        anonymizer.anonymize()
        exit(1)
    

if __name__ == '__main__':
    main('data/breast/breast.xlsx')