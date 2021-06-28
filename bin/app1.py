


import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))




if __name__=="__main__":
    from src import core_file_processor as filecon
    argv_parser = filecon.FileProcessor(sys.argv) #实例化对象
    argv_parser.execute()