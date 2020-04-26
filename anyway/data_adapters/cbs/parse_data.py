from anyway.data_adapters.cbs.codes import get_col_code_map_json, get_non_urban_code_map_json, get_data_code_map_json, \
    update_nested_mapping, get_street_map_json
from anyway.data_adapters.cbs.config.config import value_languages, cbs_general_convert_fields, \
    cbs_involved_convert_fields, cbs_vehicles_convert_fields
from anyway.data_adapters.cbs.config.paths import CBS_GENERAL_DATA_PATH
from anyway.data_adapters.cbs.parse_utils import parse_by_mapping, generate_id_column, parse_by_mapping_street, \
    format_df, get_dt


def parse_general_cbs_data(raw_df, col_name_map_df, data_map_df, non_urban_junction_map_df, street_map_df):
    col_names_map = get_col_code_map_json(col_name_map_df)
    non_urban_junction_map = get_non_urban_code_map_json(non_urban_junction_map_df)
    field_map = get_data_code_map_json(data_map_df, col_names_map, value_languages)
    update_nested_mapping('94', non_urban_junction_map, field_map, value_languages)
    parsed_df = parse_by_mapping(raw_df, col_names_map, field_map, 'hebrew')
    parsed_df['REHOV1'] = parsed_df['REHOV1'].astype('Int64')
    parsed_df['REHOV2'] = parsed_df['REHOV2'].astype('Int64')
    parsed_df = generate_id_column(parsed_df, 'SEMEL_YISHUV', 'REHOV1', 'street1')
    parsed_df = generate_id_column(parsed_df, 'SEMEL_YISHUV', 'REHOV2', 'street2')
    streets_names_map = get_street_map_json(street_map_df, value_languages)
    parsed_df = parse_by_mapping_street(parsed_df, streets_names_map, 'hebrew')
    dt = get_dt(CBS_GENERAL_DATA_PATH)
    return format_df(parsed_df, cbs_general_convert_fields, dt)


def parse_involved_cbs_data(raw_df, col_name_map_df, data_map_df):
    col_names_map = get_col_code_map_json(col_name_map_df)
    field_map = get_data_code_map_json(data_map_df, col_names_map, value_languages)
    parsed_df = parse_by_mapping(raw_df, col_names_map, field_map, 'hebrew')
    dt = get_dt(CBS_GENERAL_DATA_PATH)
    return format_df(parsed_df, cbs_involved_convert_fields, dt)


def parse_vehicles_cbs_data(raw_df, col_name_map_df, data_map_df):
    col_names_map = get_col_code_map_json(col_name_map_df)
    field_map = get_data_code_map_json(data_map_df, col_names_map, value_languages)
    parsed_df = parse_by_mapping(raw_df, col_names_map, field_map, 'hebrew')
    dt = get_dt(CBS_GENERAL_DATA_PATH)
    return format_df(parsed_df, cbs_vehicles_convert_fields, dt)
