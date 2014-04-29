
def parse_cpu_stat(stat_field):
    assert len(stat_field) > 7
    stat_info = {'user': int(stat_field[0]),
                 'nice': int(stat_field[1]),
                 'sys': int(stat_field[2]),
                 'idle': int(stat_field[3]),
                 'iowait': int(stat_field[4]),
                 'total': sum([int(e) for e in stat_field])}
    return stat_info


def get_cpu_stat():
    stat_file = open('/proc/stat')
    stat_infos = dict()
    for line in stat_file:
        fields = line.split()
        if len(fields) > 0 and fields[0].startswith('cpu'):
            stat_infos[fields[0]] = parse_cpu_stat(fields[1:])

    return stat_infos
