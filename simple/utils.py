import codecs
import hashlib
from typing import Tuple, List
import os


def calc_hash(body: str):
    hasher = hashlib.md5()
    hasher.update(body.encode('utf-8'))
    return hasher.hexdigest()


async def save_files_to_folder(
    new_files: List[Tuple[str, str]], 
    folder: str
) -> None:
    '''
    1. get old_files
    2. compare new_files and old_files
        if new_file.content == old_file.content: rename old_file
        elif: new_file.fname exists:
            rename new_file.fname to new_file.fname_old
            save new_file.content to new_file.fname
        else: save new_file.content to new_file.fname
    '''

    old_files = {}

    for old_file_name in os.listdir(folder):
        content = codecs.open(f"{folder}/{old_file_name}", "r", "utf-8").read()
        old_files[old_file_name] = content.strip()
    
    for new_file in new_files:
        flag = False
        new_file_name = new_file[0]
        # print(f"new_file_name: {new_file_name}")
        new_file_content = new_file[1]
        for old_file_name, old_file_content in old_files.items():
            # print(f"cmp: {new_file_name} == {old_file_name}: {new_file_content == old_file_content}")
            if new_file_content == old_file_content:
                if new_file_name != old_file_name:
                    os.rename(f"{folder}/{old_file_name}", f"{folder}/{new_file_name}")
                    flag = True
                    break
                else:
                    flag = True
                    break
        if not flag: # there's no old_file with the same content
            k = ''
            while os.path.exists(f"{folder}/{new_file_name}{k}"):
                k += '_'
            with codecs.open(f"{folder}/{new_file_name}{k}", "w", "utf-8") as f:
                print(1)
                f.write(new_file_content)
                # flag = True

