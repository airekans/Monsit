import psutil


def get_mem_stat():
    return psutil.virtual_memory(), psutil.swap_memory()
