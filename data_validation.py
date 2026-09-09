import numpy as np
import pandas as pd
import json
import re

class DfDataValidation:

    def __init__(self,):
        pass

    def nan_validation(self, df):
        df = df.replace('null', np.nan)
        df = df.replace({np.nan: None})
        return df

    def aff_id(self, df):
        df['aff_id'] = df['aff_id'].astype('Int64')  #
        return df



    def athena_hive_struct_to_json(self,value):
        """From Athena HIVE to pg json format """

        if value is None or value == '':
            return None
        if isinstance(value, dict):
            return json.dumps(value)  # serialize

        try:
            # parse json
            return json.dumps(json.loads(value))
        except (json.JSONDecodeError, TypeError):
            pass

        try:
            # Hive format: {key=value, key2=value2}
            value = value.strip()
            if value.startswith('{') and value.endswith('}'):
                value = value[1:-1]

            # Парсим key=value пары
            result = {}
            # Простой парсер для плоских структур
            pairs = re.split(r',\s*(?=[a-zA-Z_]+=)', value)
            for pair in pairs:
                if '=' in pair:
                    k, v = pair.split('=', 1)
                    result[k.strip()] = v.strip()

            return json.dumps(result)
        except Exception:
            return None

    def jsonb_to_json(self, df, cols):
        for col in cols:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: json.dumps(x.tolist() if hasattr(x, 'tolist') else x, default=str)
                    if x is not None else None
                )
        return df

