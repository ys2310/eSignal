# 2016-04-25
def list_not_contains_str(_list, _execlude_str_list):
    for str in _execlude_str_list:
        _list = list(filter(lambda x : str not in x, _list))
    return _list
