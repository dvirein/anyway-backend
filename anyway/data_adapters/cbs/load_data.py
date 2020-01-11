import logging

import pandas as pd

from anyway.data_adapters.cbs.config import convert_fields, value_languages
from anyway.data_adapters.cbs.codes import get_col_code_map_json
from anyway.data_adapters.cbs.codes import get_data_code_map_json
from anyway.data_adapters.cbs.parse_data import parse_by_mapping, format_df, get_dt

# CBS_DATA_PATH = r"d:\Users\haim\Documents\anyway_data_samples\H20051161AccData_short.csv"
# CBS_FIELDS_CODES_DATA_PATH = r"d:\Users\haim\Documents\anyway_data_samples\codes\Sadot_14Nov_1539_Fields.csv"
# CBS_CODES_DATA_PATH = r"d:\Users\haim\Documents\anyway_data_samples\codes\Dictionary.csv"
# OUTPUT_CBS_DATA_PATH = r"d:\Users\haim\Documents\anyway_data_samples\out_H20051161AccData_short.csv"
# DT = '200511'

CBS_DATA_PATH = r"d:\Users\haim\Documents\anyway_data_samples\H20181161\klali_07Apr_1718_AccData.csv"
CBS_FIELDS_CODES_DATA_PATH = r"d:\Users\haim\Documents\anyway_data_samples\H20181161\Sadot_07Apr_1718_Fields.csv"
CBS_CODES_DATA_PATH = r"d:\Users\haim\Documents\anyway_data_samples\H20181161\milon_07Apr_1718_Dictionary.csv"
OUTPUT_CBS_DATA_PATH = r"d:\Users\haim\Documents\anyway_data_samples\H20181161\output\klali_07Apr_1718_AccData"
DT = get_dt(CBS_DATA_PATH)


def read_data(excel_path, rows=None):
    df = pd.read_csv(excel_path, encoding='ISO-8859-8', nrows=rows)
    logging.info(excel_path + " has been loaded")
    return df


# extract
cbs_raw_df = read_data(CBS_DATA_PATH, 20)
cbs_data_mapping_df = read_data(CBS_CODES_DATA_PATH)
cbs_column_mapping_df = read_data(CBS_FIELDS_CODES_DATA_PATH)

# transform
col_names_map = get_col_code_map_json(cbs_column_mapping_df)
field_map = get_data_code_map_json(cbs_data_mapping_df, col_names_map, value_languages)
parsed_df = parse_by_mapping(cbs_raw_df, col_names_map, field_map, 'hebrew')
format_df = format_df(parsed_df, convert_fields, DT)

# load
format_df.to_csv(OUTPUT_CBS_DATA_PATH, encoding='ISO-8859-8', index=False)
