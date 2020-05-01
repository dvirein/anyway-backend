from anyway.data_adapters.cbs.codes import generate_col_name_code_map, generate_non_urban_code_map, \
    generate_data_code_map, \
    update_nested_map, generate_street_code_map
from anyway.data_adapters.cbs.config.fields_mapping import cbs_general_convert_fields, \
    cbs_involved_convert_fields, cbs_vehicles_convert_fields
from anyway.data_adapters.cbs.config.paths import CBS_GENERAL_DATA_PATH
from anyway.data_adapters.cbs.config.translate import value_languages
from anyway.data_adapters.cbs.utils.parse_utils import parse_by_mapping, generate_id_column, parse_by_mapping_street, \
    format_df, get_dt


# TODO: create object of CBSTable
# class CBSTable:
#     def __init__(self, raw_df, col_name_map_df, data_map_df):
#         self.col_name_map_df = col_name_map_df
#         self.data_map_df = data_map_df
#         self.raw_df = raw_df

# TODO: move the genarate to global, from all the mapping functions
# col_names_map = generate_col_name_code_map(col_name_map_df)
# field_map = generate_data_code_map(data_map_df, col_names_map, value_languages)


def parse_general_cbs_data(raw_df, col_name_map_df, data_map_df, non_urban_junction_map_df, street_map_df):
    col_names_map = generate_col_name_code_map(col_name_map_df)
    field_map = generate_data_code_map(data_map_df, col_names_map, value_languages)
    non_urban_junction_map = generate_non_urban_code_map(non_urban_junction_map_df)
    update_nested_map('94', non_urban_junction_map, field_map, value_languages)
    parsed_df = parse_by_mapping(raw_df, col_names_map, field_map, 'hebrew')
    parsed_df['REHOV1'] = parsed_df['REHOV1'].astype('Int64')
    parsed_df['REHOV2'] = parsed_df['REHOV2'].astype('Int64')
    parsed_df = generate_id_column(parsed_df, 'SEMEL_YISHUV', 'REHOV1', 'street1')
    parsed_df = generate_id_column(parsed_df, 'SEMEL_YISHUV', 'REHOV2', 'street2')
    streets_names_map = generate_street_code_map(street_map_df, value_languages)
    parsed_df = parse_by_mapping_street(parsed_df, streets_names_map, 'hebrew')
    dt = get_dt(CBS_GENERAL_DATA_PATH)
    return format_df(parsed_df, cbs_general_convert_fields, dt)


# TODO: combine both of those functions below
def parse_involved_cbs_data(raw_df, col_name_map_df, data_map_df):
    col_names_map = generate_col_name_code_map(col_name_map_df)
    field_map = generate_data_code_map(data_map_df, col_names_map, value_languages)
    parsed_df = parse_by_mapping(raw_df, col_names_map, field_map, 'hebrew')
    dt = get_dt(CBS_GENERAL_DATA_PATH)
    return format_df(parsed_df, cbs_involved_convert_fields, dt)


def parse_vehicles_cbs_data(raw_df, col_name_map_df, data_map_df):
    col_names_map = generate_col_name_code_map(col_name_map_df)
    field_map = generate_data_code_map(data_map_df, col_names_map, value_languages)
    parsed_df = parse_by_mapping(raw_df, col_names_map, field_map, 'hebrew')
    dt = get_dt(CBS_GENERAL_DATA_PATH)
    return format_df(parsed_df, cbs_vehicles_convert_fields, dt)
