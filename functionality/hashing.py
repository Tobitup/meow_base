"""
This file contains functions for taking hashes of data and files.

Author(s): David Marchant
"""

from hashlib import sha256
from os import listdir
from os.path import isfile, exists


from meow_base.core.vars import HASH_BUFFER_SIZE, SHA256
from meow_base.functionality.validation import check_type, \
    valid_existing_file_path, valid_existing_dir_path

def _get_file_sha256(file_path:str)->str:
    sha256_hash = sha256()
    
    with open(file_path, 'rb') as file_to_hash:
        while True:
            buffer = file_to_hash.read(HASH_BUFFER_SIZE)
            if not buffer:
                break
            sha256_hash.update(buffer)
    
    return sha256_hash.hexdigest()

# TODO update this to be a bit more robust
def _get_dir_sha256(dir_path:str)->str:
    sha256_hash = sha256()

    buffer = str(listdir(dir_path)).encode()
    sha256_hash.update(buffer)

    return sha256_hash.hexdigest()

def get_file_hash(file_path:str, hash:str, hint:str="")->str:
    check_type(hash, str, hint=hint)

    valid_existing_file_path(file_path)

    valid_hashes = {
        SHA256: _get_file_sha256
    }
    if hash not in valid_hashes:
        raise KeyError(f"Cannot use hash '{hash}'. Valid are "
            f"'{list(valid_hashes.keys())}")

    return valid_hashes[hash](file_path)

# TODO inspect this a bit more fully 
def get_dir_hash(dir_path:str, hash:str, hint:str="")->str:
    check_type(hash, str, hint=hint)

    valid_existing_dir_path(dir_path)

    valid_hashes = {
        SHA256: _get_dir_sha256
    }
    if hash not in valid_hashes:
        raise KeyError(f"Cannot use hash '{hash}'. Valid are "
            f"'{list(valid_hashes.keys())}")

    return valid_hashes[hash](dir_path)

def get_hash(path:str, hash:str, hint:str="")->str:
    if not exists(path):
        return None
    if isfile(path):
        return get_file_hash(path, hash, hint=hint)
    else:
        return get_dir_hash(path, hash, hint=hint)
