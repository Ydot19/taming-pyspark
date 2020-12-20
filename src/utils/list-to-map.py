def creat_map_from_str_list(ls: list[str], delimiter: str, key_index: int, value_index: int) -> dict:
    """
    Transforms a list of strings to a map
    :param ls:
    :param delimiter:
    :param key_index:
    :param value_index:
    :return: dict
    """
    ret = dict()

    for item in ls:
        item_list = item.split(delimiter)
        ret[item_list[key_index]] = item_list[value_index]

    return ret

