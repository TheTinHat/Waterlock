from hashlib import blake2b
from pathlib import Path, WindowsPath, PosixPath


def hash_this(file):
    blake = blake2b()
    with open(file, 'rb') as f:
        chunk = f.read(32768)
        while len(chunk) > 0:
            blake.update(chunk)
            chunk = f.read(32768)
    return str(blake.hexdigest())


def make_posix(path: Path):
    if isinstance(path, str):
        path = Path(path)
    if isinstance(path, WindowsPath):
        path = path.as_posix()
        return path
    elif isinstance(path, PosixPath):
        return str(path)