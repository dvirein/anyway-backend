from anyway.data_adapters.cbs.codes import generate_col_name_code_map, generate_non_urban_code_map, \
    generate_data_code_map, update_nested_map, generate_street_code_map
from anyway.data_adapters.cbs.config.translate import value_languages
from anyway.data_adapters.cbs.utils.parse_utils import parse_by_mapping, generate_id_column, parse_by_mapping_street, \
    format_df, get_dt


def parse_general_cbs_data(cbs_table, table_config, non_urban_junction_map_df, street_map_df):
    col_names_map = generate_col_name_code_map(cbs_table.column_mapping_df)
    field_map = generate_data_code_map(cbs_table.data_mapping_df, col_names_map, value_languages)
    non_urban_junction_map = generate_non_urban_code_map(non_urban_junction_map_df)
    update_nested_map('94', non_urban_junction_map, field_map, value_languages)
    parsed_df = parse_by_mapping(cbs_table.raw_df, col_names_map, field_map, 'hebrew')
    parsed_df['REHOV1'] = parsed_df['REHOV1'].astype('Int64')
    parsed_df['REHOV2'] = parsed_df['REHOV2'].astype('Int64')
    parsed_df = generate_id_column(parsed_df, 'SEMEL_YISHUV', 'REHOV1', 'street1')
    parsed_df = generate_id_column(parsed_df, 'SEMEL_YISHUV', 'REHOV2', 'street2')
    streets_names_map = generate_street_code_map(street_map_df, value_languages)
    parsed_df = parse_by_mapping_street(parsed_df, streets_names_map, 'hebrew')
    dt = get_dt(table_config['raw_input_path'])
    return format_df(parsed_df, table_config['fields_rename'], dt)


def parse_cbs_data(cbs_table, table_config):
    col_names_map = generate_col_name_code_map(cbs_table.column_mapping_df)
    field_map = generate_data_code_map(cbs_table.data_mapping_df, col_names_map, value_languages)
    parsed_df = parse_by_mapping(cbs_table.raw_df, col_names_map, field_map, 'hebrew')
    dt = get_dt(table_config['raw_input_path'])
    return format_df(parsed_df, table_config['fields_rename'], dt)
