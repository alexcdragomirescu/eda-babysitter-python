import os
import gzip
import shutil
import contextlib
import time


def remove_old_files(path, days):
    now = time.time()

    for root, dirs, files in os.walk(os.path.abspath(path)):
        for f in files:
            if os.path.getmtime(os.path.join(root, f)) < now - days * 86400:
                if os.path.isfile(os.path.join(root, f)):
                    os.remove(os.path.join(os.path.join(root, f)))

def remove_files(path):
    for root, dirs, files in os.walk(os.path.abspath(path)):
        for f in files:
            os.remove(os.path.join(root, f))
        os.rmdir(path)

def ungz(f, dest):
    with contextlib.closing(gzip.open(f, 'rb')) as f_in:
        with contextlib.closing(open(dest, 'wb')) as f_out:
            shutil.copyfileobj(f_in, f_out)

def uncompress(f, dest):
    extension = os.path.splitext(f)[1]
    exec_map = {
        '.gz': lambda: ungz(f, dest)
    }
    exec_map[extension]()
