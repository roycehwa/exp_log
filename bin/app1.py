


import os
import sys
import argparse

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))




if __name__=="__main__":
    from src import core_file_processor as filecon
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=["clean", "format", "upload", "convert", "search"],
                        help="commend: clean,format,upload,convert,search")
    parser.add_argument("--file", help="The path of file.")
    parser.add_argument("mode", choices=['bank', 'expense'], help="Attribute of data, bank or expense")
    args = parser.parse_args()

    print(args)

    argv_parser = filecon.FileProcessor(args) #实例化对象
    argv_parser.execute()



