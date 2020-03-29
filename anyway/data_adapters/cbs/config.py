value_languages = ['english', 'arabic', 'russian']

convert_fields = {
    # "SUG_TIK_MISHTARA": "file_type_police",
    "SUG_TIK": "file_type",
    "ISHUV": "settlement",
    "REHOV1": "street1_code",
    "REHOV2": "street2_code",
    "STREET1": "street1",
    "STREET2": "street2",
    "KVISH1": "road1",
    "KVISH2": "road2",
    "ZOMET": "junction",
    "SHEM_ZOMET": "junction_name",
    "SEMEL_RECHOV": "street_sign",
    "SHEM_RECHOV": "street_name",
    "MS_TAVLA": "table_number",
    "KOD": "code",
    "NAME": "name",
    "SEMEL": "sign",
    "ZOMET_IRONI": "urban_intersection",
    "ZOMET_LO_IRONI": "non_urban_intersection",
    "SHNAT_TEUNA": "accident_year",
    "HODESH_TEUNA": "accident_month",
    "YOM_BE_HODESH": "accident_day",
    "SHAA": "accident_hour",
    "X": "x",
    "Y": "y",
    "SUG_TEUNA": "accident_type",  # part of the desciption
    "HUMRAT_TEUNA": "accident_severity",  # part of the desciption
    "STATUS_IGUN": "location_accuracy",  # part of the desciption
    "PK_TEUNA_FIKT": "id",
    "BAYIT": "house_number",
    "SUG_DEREH": "road_type",  # part of the desciption
    "SUG_YOM": "day_type",  # part of the desciption
    "ZURAT_DEREH": "road_shape",  # part of the desciption
    "YEHIDA": "police_unit",  # part of the desciption
    "HAD_MASLUL": "one_lane",
    "RAV_MASLUL": "multi_lane",
    "MEHIRUT_MUTERET": "speed_limit",
    "TKINUT": "road_intactness",
    "ROHAV": "road_width",
    "SIMUN_TIMRUR": "road_sign",
    "TEURA": "road_light",
    "BAKARA": "road_control",
    "MEZEG_AVIR": "weather",
    "PNE_KVISH": "road_surface",
    "SUG_EZEM": "road_object",
    "MERHAK_EZEM": "object_distance",
    "LO_HAZA": "didnt_cross",
    "OFEN_HAZIYA": "cross_mode",
    "MEKOM_HAZIYA": "cross_location",
    "KIVUN_HAZIYA": "cross_direction",
    "KM": "km",
    "SEMEL_YISHUV": "yishuv_symbol",
    "THUM_GEOGRAFI": "geo_area",
    "YOM_LAYLA": "day_night",
    "YOM_BASHAVUA": "day_in_week",
    "RAMZOR": "traffic_light",
    "MAHOZ": "region",
    "NAFA": "district",
    "EZOR_TIVI": "natural_area",
    "MAAMAD_MINIZIPALI": "municipal_status",
    "ZURAT_ISHUV": "yishuv_shape"
}

involved_csv_convert_fields = {
    "SUG_MEORAV": "involved_type",
    "SHNAT_HOZAA": "license_acquiring_date",
    "KVUZA_GIL": "age_group",
    "MIN": "sex",
    "SUG_REHEV_NASA_LMS": "vehicle_type_involved",
    "EMZAE_BETIHUT": "safety_measures",
    "SEMEL_YISHUV_MEGURIM": "involve_yishuv_symbol",
    "HUMRAT_PGIA": "injury_severity",
    "SUG_NIFGA_LMS": "injured_type",
    "PEULAT_NIFGA_LMS": "injured_position",
    "KVUTZAT_OHLUSIYA_LMS": "population_type",
    "MAHOZ_MEGURIM": "home_region",
    "NAFA_MEGURIM": "home_district",
    "EZOR_TIVI_MEGURIM": "home_natural_area",
    "MAAMAD_MINIZIPALI_MEGURIM": "home_municipal_status",
    "ZURAT_ISHUV_MEGURIM": "home_yishuv_shape",
    "SUG_TIK": "file_type",
    "PAZUAUSHPAZ_LMS": "hospital_time",
    "ISS_LMS": "medical_type",
    "YAADSHIHRUR_PUF_LMS": "release_dest",
    "SHIMUSHBEAVIZAREYBETIHUT_LMS": "safety_measures_use",
    "PTIRAMEUHERET_LMS": "late_deceased",
    "MISPAR_REHEV_FIKT": "car_id",
    "ZEHUT_FIKT": "involve_id",
    "MAXAIS_LMS": "injury_severity_mais"
}

vehicles_data_convert_fields = {
    "NEFAH": "engine_volume",
    "SHNAT_YITZUR": "manufacturing_year",
    "KIVUNE_NESIA": "driving_directions",
    "MATZAV_REHEV": "vehicle_status",
    "SHIYUH_REHEV_LMS": "vehicle_attribution",
    "SUG_REHEV_LMS": "vehicle_type_vehicles",
    "MEKOMOT_YESHIVA_LMS": "seats",
    "MISHKAL_KOLEL_LMS": "total_weight",
    "NEZEK": "vehicle_damage"
}
