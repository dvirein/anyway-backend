import logging

import pandas as pd

from anyway.data_adapters.cbs.config import convert_fields, value_languages
from anyway.data_adapters.cbs.codes import get_col_code_map_json, get_non_urban_code_map_json, update_nested_mapping, \
    get_street_map_json
from anyway.data_adapters.cbs.codes import get_data_code_map_json
from anyway.data_adapters.cbs.parse_data import parse_by_mapping, format_df, get_dt, generate_id_column, \
    parse_by_mapping_street

# CBS_DATA_PATH = r"d:\Users\haim\Documents\anyway_data_samples\H20051161AccData_short.csv"
# CBS_FIELDS_CODES_DATA_PATH = r"d:\Users\haim\Documents\anyway_data_samples\codes\Sadot_14Nov_1539_Fields.csv"
# CBS_CODES_DATA_PATH = r"d:\Users\haim\Documents\anyway_data_samples\codes\Dictionary.csv"
# OUTPUT_CBS_DATA_PATH = r"d:\Users\haim\Documents\anyway_data_samples\out_H20051161AccData_short.csv"
# DT = '200511'

CBS_DATA_PATH = r"d:\Users\haim\Documents\anyway_data_samples\H20181161\klali_07Apr_1718_AccData.csv"
CBS_FIELDS_CODES_PATH = r"d:\Users\haim\Documents\anyway_data_samples\H20181161\Sadot_07Apr_1718_Fields.csv"
CBS_CODES_PATH = r"d:\Users\haim\Documents\anyway_data_samples\H20181161\milon_07Apr_1718_Dictionary.csv"

NON_URBAN_CODES_PATH = r"d:\Users\haim\Documents\anyway_data_samples\H20181161\Zmatim_lo_ironiim_07Apr_1718_IntersectNonUrban.csv"
CBS_STREET_CODES_PATH = r"d:\Users\haim\Documents\anyway_data_samples\H20181161\Rechovot_07Apr_1718_DicStreets.csv"
OUTPUT_CBS_DATA_PATH = r"d:\Users\haim\Documents\anyway_data_samples\H20181161\output\klali_07Apr_1718_AccData.csv"
DT = get_dt(CBS_DATA_PATH)


def read_data(excel_path, rows=None):
    df = pd.read_csv(excel_path, encoding='ISO-8859-8', nrows=rows)
    logging.info(excel_path + " has been loaded")
    return df


# extract
cbs_raw_df = read_data(CBS_DATA_PATH, 100)
cbs_data_mapping_df = read_data(CBS_CODES_PATH)
# cbs_data_mapping_df = cbs_data_mapping_df .drop_duplicates(COL_CODE_JUNCTION)
# cbs_raw_df = cbs_raw_df[cbs_raw_df.ZOMET_LO_IRONI.notnull()]


cbs_column_mapping_df = read_data(CBS_FIELDS_CODES_PATH)
cbs_street_mapping_df = read_data(CBS_STREET_CODES_PATH)
cbs_non_urban_junction_mapping_df = read_data(NON_URBAN_CODES_PATH)

# transform
col_names_map = get_col_code_map_json(cbs_column_mapping_df)
non_urban_junction_map = get_non_urban_code_map_json(cbs_non_urban_junction_mapping_df)
field_map = get_data_code_map_json(cbs_data_mapping_df, col_names_map, value_languages)
update_nested_mapping('94', non_urban_junction_map, field_map, value_languages)
parsed_df = parse_by_mapping(cbs_raw_df, col_names_map, field_map, 'hebrew')
parsed_df['REHOV1'] = parsed_df['REHOV1'].astype('Int64')
parsed_df['REHOV2'] = parsed_df['REHOV2'].astype('Int64')
parsed_df = generate_id_column(parsed_df, 'SEMEL_YISHUV', 'REHOV1', 'street1')
parsed_df = generate_id_column(parsed_df, 'SEMEL_YISHUV', 'REHOV2', 'street2')
streets_names_map = get_street_map_json(cbs_street_mapping_df, value_languages)
parsed_df = parse_by_mapping_street(parsed_df, streets_names_map, 'hebrew')

format_df = format_df(parsed_df, convert_fields, DT)

# load
format_df.to_csv(OUTPUT_CBS_DATA_PATH, encoding='ISO-8859-8', index=False)
