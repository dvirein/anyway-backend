import logging
import re


def parse_by_mapping(df, col_map, value_map, lang):
    for column in list(df.columns.values):
        idx = 0
        if column in col_map.keys():
            col_code = col_map[column]
            if col_code in value_map.keys():
                df[column] = df[column].fillna(-9998).astype(int)
                df[column] = df[column].astype(str)
                for row in df[column]:
                    df.at[idx, column] = value_map.get(col_map[column], {}).get(row, {}).get(lang)
                    idx += 1
    return df


def parse_by_mapping_street(df, value_map):  # lang
    #TODO: check for bug on converting mapping
    street_cols = ['street1', 'street2']
    for column in street_cols:
        idx = 0
        # col_code = col_map[column]
        # if col_code in value_map.keys():
        #     df[column] = df[column].fillna(-9998).astype(int)
        #     df[column] = df[column].astype(str)
        for row in df[column]:
            if re.match('\d+_\d+', row) is not None:
                city_code = int(re.search('(\d+)_\d+', row).group(1))
                street_code = int(re.search('\d+_(\d+)', row).group(1))
                df.at[idx, column] = value_map.get(city_code, {}).get(street_code, {})  # .get(lang)
                idx += 1
    return df


def format_df(df, convert_fields, dt):
    df.columns = map(str.upper, df.columns)
    df = df.rename(columns=convert_fields)
    df = _drop_unnecessary_columns(df, convert_fields.values())
    df['dt'] = dt
    return df


def get_dt(path):
    dt_search = re.search('.*\\\H(\d{6})\d+\\\.*\.csv', path)
    return dt_search.group(1)


def generate_id_column(df, col1, col2, new_column_name):
    # todo: replace to '' values wich doesnt have a full 'city_street' code, add flag for that
    df[new_column_name] = _str_number(df[col1]) + '_' + _str_number(df[col2])
    df.to_csv(r"d:\Users\haim\Documents\anyway_data_samples\H20181161\output\test\generate_id_column.csv",
              encoding='ISO-8859-8', index=False)
    return df


def remove_partial_concat(df):
    df.replace({'A': r'^ba.$'}, {'A': 'new'}, regex=True)


def _drop_unnecessary_columns(df, allowed_columns):
    drop_columns = [column for column in df.columns.values if column not in allowed_columns]
    if drop_columns:
        logging.warning(f"Unknown columns has been found: {drop_columns} consider add to mapping fields names")
    df = df.drop(columns=drop_columns)
    return df


def _str_number(column):
    column = column.astype(str)
    return column.replace('nan', '')
