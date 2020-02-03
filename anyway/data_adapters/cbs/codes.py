COL_CODE_FIELDS_DATA = 'MS_TAVLA'
COL_NAME_COL_DATA = 'SADE_ENG'
COL_CODE_COL_DATA = 'SEMEL_MILON'

COL_NAME_COL_JUNCTION = 'SHEM_ZOMET'
COL_CODE_JUNCTION = 'zomet'


def get_data_code_map_json(df, col_code_map, translate_languages):
    df = df.loc[df[COL_CODE_FIELDS_DATA] != 0]
    col_codes = list(set(col_code_map.values()))
    mapping_dict = {}
    for col_code in col_codes:
        fields_mapping = {}
        # if col_kod in df['MS_TAVLA'].values.tolist():
        for field in df.loc[df[COL_CODE_FIELDS_DATA] == int(col_code)].values.tolist():
            language = {'hebrew': field[2]}
            for lang in translate_languages:
                language[lang] = translate(field, lang)
            fields_mapping[str(field[1])] = language
            mapping_dict[col_code] = fields_mapping
    return mapping_dict


def update_nested_mapping(key_code, value, mapping_dict, translate_languages):
    mapping_dict[key_code] = field_nested_translating(value, translate_languages)
    return mapping_dict


def field_nested_translating(value, translate_languages):
    for code in value.keys():
        language = {'hebrew': value[code]}
        for lang in translate_languages:
            language[lang] = translate(value[code], lang)
        value[code] = language
    return value


def get_col_code_map_json(column_mapping_df):
    column_mapping_df = column_mapping_df[[COL_NAME_COL_DATA, COL_CODE_COL_DATA]]
    column_mapping_df = column_mapping_df[column_mapping_df.SEMEL_MILON.notnull()]
    column_mapping_json = {}
    for field in column_mapping_df.values.tolist():
        column_mapping_json[field[0]] = str(int(field[1]))
    return column_mapping_json


def get_non_urban_code_map_json(mapping_df):
    mapping_df = mapping_df[[COL_NAME_COL_JUNCTION, COL_CODE_JUNCTION]]
    mapping_df = mapping_df.drop_duplicates(COL_CODE_JUNCTION)
    column_mapping_json = {}
    for field in mapping_df.values.tolist():
        column_mapping_json[str(field[1])] = str(field[0])
    return column_mapping_json


def get_street_map_json(street_mapping_df):
    #TODO: pre step - create an concat ishuv+semel_rehov
    #TODO: read thhe mapping street as concat ishuv+semel_rehov
    #TODO: add json values to SHEM_RECHOV
    street_mapping_json = {}
    for field in street_mapping_df.values.tolist():
        street_mapping_json[field[0]] = str(int(field[1]))


def translate(text, desired_language, current_language='hebrew'):
    translation = priority_translate(text, desired_language, current_language)
    if translation:
        return translation
    return api_translate(text, desired_language, current_language)


def priority_translate(text, desired_language, current_language):
    return ""  # hard_translate.get(current_language, {}).get(desired_language, {}).get(text)


def api_translate(text, desired_language, current_language='he'):
    return ""
