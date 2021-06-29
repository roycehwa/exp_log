import os
import re
import json
import datetime
import time
from src.db_processor import DB,DBConnect

from db import boilerplate as bp
from config import config

class FileProcessor(object):
    def __init__(self,sys_argv):
        self.sys_argv = sys_argv
        self.verfiy_argv()
        self.boilerplate = bp.strings
        self.update_pool = {}
        self.vd_item_update = config.clean_kw
        with open(config.category,mode='r',encoding='utf-8') as cat:
            self.category = json.load(cat)

    def verfiy_argv(self):
        if len(self.sys_argv)< 3:
            self.help_msg()
        cmd = self.sys_argv[1]
        if not hasattr(self,cmd):
            self.help_msg()

    def help_msg(self):
        msg = """
        [clean|format|upload] [file_name] [-e|-b] 
        clean       clean data
        format      format data
        upload      upload data to sql
        -e          target data is expenditure
        -b          target data is balance
        """
        #TODO: allinone 用于从原始数据到完整结构数据的连续操作
        exit(msg)

    def execute(self):
        file_name = self.sys_argv[2]
        print(file_name)
        if not os.path.exists(file_name):
            print("文件{}不存在,请核查后操作。".format(file_name))
            return
        elif file_name.split('.')[-1]!='csv':
            print("请传入CSV文件.")
            return
        else:
            cmd = self.sys_argv[1]
            param = self.sys_argv[3]
            func = getattr(self,cmd)
            file_object = self.sys_argv[2]
            func(file_object,param)

    def format(self,file_object,param):
        print("格式化文件...")

        if param == "-e":
            account = input("请输入支出账户:  ")
            # TODO:判断account是否合法
            header_ind = ["time","vendor","items","amount","in_or_out"]
            with open(file_object,mode="r",encoding='utf-8') as file:
                lines = file.readlines()
                length = len(lines)
                header = lines[0]
                headers = header.split(',')
                head_number = [headers.index(field) for field in header_ind]
                tag = datetime.datetime.now().strftime("%Y%m%d")
                file_new = "{}{}".format(tag,file_object)
                with open(file_new,mode='a',encoding='utf-8') as clean_file:
                    for i in range(1,length):
                        line = lines[i]
                        content = line.split(',')
                        clean_content = [content[hn].strip() for hn in head_number]
                        clean_content.append("__")
                        clean_content.append(account.strip())
                        clean_content.append("\n")
                        newline = ",".join(clean_content)
                        clean_file.write(newline)

            print("数据格式化完成，存储于文件{}中。".format(file_new))

        elif param =='-b':
            header_ind = ["date", "time", "transaction", "amount", "balance", "note"]
            bank_list = {"1": "招商银行-2944RMB", "2": "招商银行-2944USD",
                         "3": "招商银行-0852RMB", "4": "招商银行-0852USD",
                         "5": "建设银行", "6": "HSBC7434RMB",
                         "7": "HSBC7434USD", "8": "HSBCHK9888USD",
                         "9": "EASTWEST1114USD", "10": "工商银行4282RMB"}

            for item in bank_list.items():
                print("{} - {}".format(*item))

            bank = input("请输入银行编号: ")
            currency = "USD" if input("RMB 或者 USD (U - USD, 其他 RMB)").upper() == "U" else "RMB"

            with open(file_object, mode="r", encoding='utf-8-sig') as file:
                lines = file.readlines()
                # 从源文件中找到date...等栏目的序号，组成列表，采用列表推导式从读取重构新文件
                length = len(lines)
                header = lines[0]
                hea = header.split(',')
                headers = [i.strip().strip("\n") for i in hea]
                head_number = []  # 此为列数的序列，纪录源文件中相应的列具体位置
                for field in header_ind:
                    head_number.append(headers.index(field))
                tag = datetime.datetime.now().strftime("%Y%m%d")
                file_new = "{}{}".format(tag, file_object)
                with open(file_new, mode='a', encoding='utf-8') as clean_file:
                    for i in range(1, length):
                        clean_content = []
                        line = lines[i]
                        content = line.split(',')
                        for i in range(0, len(content)):
                            content[i] = content[i].strip("\"").strip("\t").strip("\n")
                        clean_content.append(bank)
                        clean_content.append(currency)
                        clean_content.append(
                            "{}/{}/{}".format(content[head_number[0]][:4], content[head_number[0]][4:6],
                                              content[head_number[0]][-2:]))
                        #格式化日期为YYYY/MM/DD
                        [clean_content.append(content[head_num]) for head_num in head_number[1:]]
                        clean_content.append("\n")
                        newline = ",".join(clean_content)
                        clean_file.write(newline)

            print("data format completed, saved in {}".format(file_new))

        else:
            print("Invalid params (-b or -e)")

    def clean(self,file_object,param):
        print("Cleaning")
        self.update_pool = { "vendor":{},"items": {}}

        if param == "-e":
            clean_file = "clean_{}".format(file_object)
            with open(clean_file,mode='a',encoding='utf-8') as clean_chart:
                with open(file_object,mode='r',encoding='utf-8') as chart:
                    un_cat = 0
                    for line in chart:
                        fields = line.split(',')
                        if fields[4].strip() != "支出":
                            continue
                        else:
                            vendor,item = fields[1:3]
                            fields[1]=self.simp_v(vendor)  # 简化vendor名称
                            fields[2]=self.simp_i(item)
                            fields[-3]= self.categorize(fields[1],fields[2])# 根据简化名称确定门类
                            if fields[-3] == "0" : un_cat += 1
                            clean_content = ",".join(fields)
                        clean_chart.write(clean_content)
                    print("{} 行没有被分类，请自行分类。".format(un_cat))

            with open(self.vd_item_update,"w+",encoding='utf-8') as f:
                try:
                    vd_pool = json.load(f)
                except Exception:
                    vd_pool = {}
                vd_pool.update(self.update_pool)
                json.dump(vd_pool,f)

        if param == "-b":
            """balance表格"""
            clean_file = "clean_{}".format(file_object)
            with open(clean_file,mode='a',encoding='utf-8') as clean_chart:
                with open(file_object,mode='r',encoding='utf-8') as chart:
                    for line in chart:
                        fields = line.split(',')
                        time = " ".join(fields[2:4])
                        fields[2] = time
                        fields.pop(3)
                        trans = fields[3].strip()
                        fields[3] = trans
                        if not fields[-1]:
                            fields[-1] = "-----"
                        clean_content = ",".join(fields)
                        clean_chart.write(clean_content)

            with open(self.vd_item_update,"w+",encoding='utf-8') as f:
                try:
                    vd_pool = json.load(f)
                except Exception:
                    vd_pool = {}
                vd_pool.update(self.update_pool)
                json.dump(vd_pool,f)

    def simp_v(self,vendor):
        rpl = vendor.strip()
        detect = re.findall(self.boilerplate["vs1"],vendor)
        detect2 = re.findall(self.boilerplate["vs2"],vendor)
        if detect!=[]:
            rpl = detect[0]
        elif detect2!=[]:
            rpl = detect2[0]
        elif not self.update_pool["vendor"].get(rpl):
            self.update_pool["vendor"][rpl] = 1
        else:
            self.update_pool["vendor"][rpl] += 1
        return rpl

    def simp_i(self,item):
        #去除纪录中的序列码和固定短语
        deni = re.sub("[Y|\d|-|_|(|)]{9,}","",item.strip())
        den2 = re.sub(":.*?购买","",deni).strip()
        detect = re.findall(self.boilerplate["is1"],den2)
        if detect:
            rpl = detect[0]
            return rpl
        elif re.findall(self.boilerplate["is2"],den2) != []:
            return re.findall(self.boilerplate["is2"], den2)[0]
        elif not self.update_pool["items"].get(den2):
            self.update_pool["items"][den2] = 1
        else:
            self.update_pool["items"][den2] += 1
        return den2

    def categorize(self,vendor,item):
        if vendor in self.category["vendor"].keys():
            return self.category["vendor"].get(vendor)
        elif item in self.category["items"].keys():
            return self.category["items"].get(item)
        else:
            return "0"

    def update_cateory(self,file_object):
        pass

    def upload(self,file_object,param):
        """打开数据源文件，读取每一行，拆分成为列表items,组成完整列表table传入UpLoader对象"""
        with open(file_object,mode='r',encoding='utf-8') as source:
            table = []
            for line in source:
                items = line.split(',')[:-1]
                table.append(tuple(items))

        if param == '-e':
            handle = DBHandler(table,"expense")
            handle.upload()

        elif param == '-b':
            handle = DBHandler(table,"balance")
            handle.upload()

class DBHandler(object):

    def __init__(self,source,type):
        self.source = source
        self.table,self.field,self.procedure = config.db_dict.get(type).values()

    def upload(self):
        sql = "insert into {} {} values(%s,%s,%s,%s,%s,%s,%s)".format(self.table,self.field)
        print(sql)
        with DBConnect() as conn_obj:
            rows = conn_obj.execute_many(sql,self.source)
            print("{} lines are uploaded.".format(rows))
            conn_obj.commit()
            conn_obj.cursor.callproc(self.procedure)
        return
