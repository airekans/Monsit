import psutil


def get_partitions(is_all=True):
    return psutil.disk_partitions(is_all)


def get_usage(path):
    return psutil.disk_usage(path)


def get_io_counters():
    return psutil.disk_io_counters(perdisk=True)

