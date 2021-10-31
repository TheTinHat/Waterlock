from hashlib import blake2b


def sanitize(f_path):
    f_path = f_path.replace("\\","/").split('/')
    f_path = [x for x in f_path if x != '']
    f_path = '/'.join(f_path)
    return f_path

def hash(f_path):
    hash = blake2b() 
    with open(f_path, 'rb') as f:
        fb = f.read(32768) 
        while len(fb) > 0:
            hash.update(fb) 
            fb = f.read(32768) 
    return str(hash.hexdigest())


def sizeof(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"