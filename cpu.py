
def get_cpu_stat():
    stat_file = open('/proc/stat')
    stat_infos = dict()
    for line in stat_file:
        fields = line.split()
        if len(fields) > 0:
            stat_infos[fields[0]] = fields[1:]

    return stat_infos
