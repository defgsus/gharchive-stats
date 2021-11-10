import os

MEM_SCALE = {
    "kb": 1024,
    "mb": 1024 * 1024,
    "gb": 1024 * 1024 * 1024,
}


def process_memory() -> int:
    """
    Returns the current memory usage of the process in bytes,
    or ``0`` if not available/working
    :return: int
    """
    try:
        with open(f"/proc/{os.getpid()}/status") as f:
            status = f.read()
    except:
        return 0

    try:
        mem_str = status[status.index("VmSize:")+7:].splitlines()[0].split()
        return int(mem_str[0]) * MEM_SCALE[mem_str[1].lower()]
    except (IndexError, KeyError):
        return 0
