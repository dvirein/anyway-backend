from anyway.data_adapters.cbs.config.translate import hard_word_mapping


def get_all_lang_translations(text, translate_languages, current_language='hebrew'):
    language = {current_language: text}
    for lang in translate_languages:
        language[lang] = _translate(text, lang, current_language)
    return language


def field_nested_translating(value, translate_languages):
    for code in value.keys():
        value[code] = get_all_lang_translations(value[code], translate_languages)
    return value


# TODO: add mode, translate | translitirate | off. (add a mapping for each field what mode should it be b4 calling the function)
def _translate(text, desired_language, current_language):
    translation = _priority_translate(text, desired_language, current_language)
    if translation:
        return translation
    return _api_translate(text, desired_language, current_language)


def _priority_translate(text, desired_language, current_language):
    return hard_word_mapping.get(current_language, {}).get(desired_language, {}).get(text)


# TODO: connect to google, (think of a way to not write user+password in code, maybe outside text file with path in config? read of common methods)
def _api_translate(text, desired_language, current_language):
    return ""



# TODO: connect to google
def _api_transliterate(text, desired_language, current_language):
    return ""
