import os
import sys
import shutil

class Directory_Remover():
    def __init__(self, dir_path):
        # accept path of the directory to be removed/deleted
        self.mydir_path = dir_path
    def remdir(self):
        # Try deletion
        try:
            # this deletes folder along with contents without any warning
            shutil.rmtree(self.mydir_path)
        except OSError as e:
            print("Error: %s - %s." % (e.filename, e.strerror))
        except Exception as e:
            print(e)