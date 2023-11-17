from algorithms.datafly import datafly_anonymize
from .mondrian import classic_mondrian_anonymize
from .basic_mondrian import basic_mondrian_anonymize, read_tree, mondrian_ldiv_anonymize
from .clustering_based import cluster_based_anonymize
from .top_down_greedy import tdg_anonymize
from utils.types import AnonMethod
from diffprivlib.mechanisms import Laplace
import pandas as pd
import numpy as np

def k_anonymize(anon_params):

    if anon_params["name"] == AnonMethod.CLASSIC_MONDRIAN:
        return classic_mondrian_anonymize(
            anon_params["value"], 
            anon_params["data"], 
            anon_params["qi_index"], 
            anon_params['mapping_dict'],
            anon_params['is_cat'],
            relax=False)

    if anon_params["name"] == AnonMethod.BASIC_MONDRIAN:
        return basic_mondrian_anonymize(
            anon_params["value"], 
            anon_params["att_trees"], 
            anon_params["data"], 
            anon_params["qi_index"], 
            anon_params["sa_index"])

    if anon_params["name"] == AnonMethod.MONDRIAN_LDIV:
        return mondrian_ldiv_anonymize(
            anon_params["value"], 
            anon_params["att_trees"], 
            anon_params["data"], 
            anon_params["qi_index"], 
            anon_params["sa_index"])

    if anon_params["name"] == AnonMethod.CLUSTER:
        return cluster_based_anonymize(
            anon_params["value"], 
            anon_params["att_trees"], 
            anon_params["data"], 
            anon_params["qi_index"], 
            anon_params["sa_index"], 
            type_alg='kmember')

    if anon_params["name"] == AnonMethod.TOPDOWN:
        return tdg_anonymize(
            anon_params["value"], 
            anon_params["att_trees"], 
            anon_params["data"], 
            anon_params["qi_index"], 
            anon_params["sa_index"])

    if anon_params["name"] == AnonMethod.DATAFLY:
        return datafly_anonymize(
            anon_params["value"], 
            anon_params["csv_path"], 
            anon_params["qi_names"], 
            anon_params["data_name"], 
            anon_params["dgh_folder"],
            anon_params['res_folder'])
        

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
                mechanism = Laplace(epsilon=epsilon, sensitivity=max_change)
                noisy_values = data[column].apply(lambda x: mechanism.randomise(x))

                if min_value is not None:
                    noisy_values = noisy_values.clip(lower=min_value)

                if round_values:
                    noisy_values = noisy_values.apply(lambda x: int(x) if not pd.isna(x) and not np.isinf(x) else x)

                data[column] = noisy_values