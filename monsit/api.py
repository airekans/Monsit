
_register_stat_funcs = {}


def register_stat_func(stat_id, func):
    _register_stat_funcs[stat_id] = func


def get_registered_stat_funcs():
    return _register_stat_funcs

