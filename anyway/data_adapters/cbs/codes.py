from anyway.data_adapters.cbs.utils.translate_utils import get_all_lang_translations, field_nested_translating

COL_CODE_FIELDS_DATA = 'MS_TAVLA'
COL_NAME_COL_DATA = 'SADE_ENG'
COL_CODE_COL_DATA = 'SEMEL_MILON'
COL_NAME_COL_JUNCTION = 'SHEM_ZOMET'
COL_CODE_JUNCTION = 'zomet'


def generate_data_code_map(df, col_code_map, translate_languages):
    df = df.loc[df[COL_CODE_FIELDS_DATA] != 0]
    col_codes = list(set(col_code_map.values()))
    mapping_dict = {}
    for col_code in col_codes:
        fields_mapping = {}
        for field in df.loc[df[COL_CODE_FIELDS_DATA] == int(col_code)].values.tolist():
            fields_mapping[str(field[1])] = get_all_lang_translations(field[2], translate_languages)
            mapping_dict[col_code] = fields_mapping
    return mapping_dict


def generate_col_name_code_map(column_mapping_df):
    column_mapping_df = column_mapping_df[[COL_NAME_COL_DATA, COL_CODE_COL_DATA]]
    column_mapping_df = column_mapping_df[column_mapping_df.SEMEL_MILON.notnull()]
    column_mapping_json = {}
    for field in column_mapping_df.values.tolist():
        column_mapping_json[field[0]] = str(int(field[1]))
    return column_mapping_json


def generate_non_urban_code_map(mapping_df):
    mapping_df = mapping_df[[COL_NAME_COL_JUNCTION, COL_CODE_JUNCTION]]
    mapping_df = mapping_df.drop_duplicates(COL_CODE_JUNCTION)
    column_mapping_json = {}
    for field in mapping_df.values.tolist():
        column_mapping_json[str(field[1])] = str(field[0])
    return column_mapping_json


def generate_street_code_map(street_mapping_df, translate_languages):
    street_mapping_json = {}
    for field in street_mapping_df.values.tolist():
        street_name = field[2]
        language = get_all_lang_translations(street_name, translate_languages)
        city_code = int(field[0])
        street_code = int(field[1])
        if city_code in street_mapping_json:
            street_mapping_json[city_code][street_code] = language
        else:
            street_mapping_json[city_code] = {street_code: language}
    return street_mapping_json


def update_nested_map(key_code, value, mapping_dict, translate_languages):
    mapping_dict[key_code] = field_nested_translating(value, translate_languages)
    return mapping_dict
