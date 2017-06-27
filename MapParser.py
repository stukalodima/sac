class MapParser:

    @staticmethod
    def get_array_words(string, key_word, separators, this_separator, find_digit):
        replace_char = ("'", "\\'", "&quot;", '"')
        words_array = string.split(this_separator)
        result = []
        for word in words_array:
            is_with_out_separator = True
            check_word = word.strip()
            if check_word == '' or (len(check_word) <= 3 and not find_digit):
                continue
            if find_digit:
                if check_word.isalpha():
                    continue
            else:
                if check_word.isdigit():
                    continue
            for sep in separators:
                if check_word.find(sep) != -1:
                    is_with_out_separator = False
                    tpm_result = MapParser.get_array_words(check_word, key_word, separators, sep, find_digit)
                    for tmp_word in tpm_result:
                        if tmp_word != "" and tmp_word not in result and tmp_word not in key_word:
                            result.append(tmp_word)
            if is_with_out_separator and check_word not in result and check_word not in key_word:
                for el_char in replace_char:
                    check_word = check_word.replace(el_char, "")
                result.append(check_word)
                print(check_word)
        return result

    @staticmethod
    def get_word_array(string, key_word, separators, find_digit):
        result_array = MapParser.get_array_words(string.lower(), key_word, separators, " ", find_digit)
        return result_array
