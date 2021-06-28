
import os

path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
clean_kw = os.path.join(path,"db","clean_kw.json")

category = os.path.join(path,"db", 'category_.json')
"""用于存储vendor以及item与消费类型关系的文件，格式为
{"vendor":{"v1":"t1","v2":t2....},items:{"i1":"t1","i2":"t2"...}}"""