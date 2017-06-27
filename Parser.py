class Parser:

    @staticmethod
    def get_array_words(string, separators, this_separator):
        key_word = dict()
        words_array = string.split(this_separator)
        result = []
        for word in words_array:
            is_with_out_separator = True
            check_word = word.strip()
            for sep in separators:
                if check_word.find(sep) != -1:
                    is_with_out_separator = False
                    tpm_result = Parser.get_array_words(check_word, separators, sep)
                    for tmp_word in tpm_result:
                        if tmp_word != "" and tmp_word not in result and tmp_word not in key_word:
                            result.append(tmp_word)
            if is_with_out_separator and check_word not in result and check_word not in key_word:
                result.append(check_word)
        return result

    @staticmethod
    def get_word_array(first_string, second_string, separators):
        result_array = Parser.get_array_words(first_string.lower(), separators, " ")
        if len(result_array) == 0:
            result_array = Parser.get_array_words(second_string.lower(), separators, " ")
            print(second_string)
        else:
            print(first_string)
        return result_array
