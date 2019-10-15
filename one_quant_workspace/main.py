import os
import sys
import importlib
from one_quant_data import DataEngine
import pandas as pd
import argparse
import datetime
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from one_quant_workspace.template import template
import IPython
import progressbar

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)


class Task():
    def __init__(self,config_file='./config.json'):
        self.config_file = config_file
        self.engine = DataEngine(self.config_file)
        print('hello world!')
        self.stock_codes =  list(self.engine.stock_basic().ts_code)
        self.available_date=None
        self.pool={}
    def preload_data(self,data_type):
        if data_type == 'stock_trade':
            self.available_date = pd.DataFrame({'trade_date':self.engine.get_trade_dates('19900101')})
            pbar = progressbar.ProgressBar().start()
            total = len(self.stock_codes)
            success=0
            neglect=0
            for i in range(total):
                pbar.update(int((i / (total - 1)) * 100))
                code = self.stock_codes[i]
                df = self.engine.pro_bar(code,adj='qfq')
                if df is None:
                    neglect+=1
                    continue
                df = self.available_date.merge(df,on=['trade_date'],how='left')
                df.sort_values(by='trade_date',ascending=True,inplace=True)
                df.reset_index(inplace=True)
                nan_list = list(df[df.close.isnull()].index)
                if 0 in nan_list:
                    neglect+=1
                    continue
                for index in nan_list:
            #print(nan_list)
            #print(index)
            #print(df.head(1).index)
            #print(df.head(1).close.isnull().iloc[0])
            #print(df.head(1).close.isnull())
            #print(df.head(1))
                    df.loc[index,'open'] = df.loc[index-1,'close']
                    df.loc[index,'close'] = df.loc[index-1,'close']
                    df.loc[index,'high'] = df.loc[index-1,'close']
                    df.loc[index,'low'] = df.loc[index-1,'close']
                df.fillna(0)
            #self.pool[code]={"quotes":df,"code":code}
                #df = close_curve(df,'close')
                self.pool[code]=df
                success+=1
            pbar.finish()
            print(self.pool)
            print('fetch:{},neglect:{}'.format(success,neglect))
            

            


class TaskEngine():
    def __init__(self,config_file):
        self.conn = None
        self.log = "/var/log/one_quant_task.log"
        #self.data_engine = DataEngine(config_file)
        config_json = json.load(open(config_file))
        assert config_json.get('task_engine')
        config = config_json['task_engine']
        saver = config.get('saver')
        if saver is not None:
            db = saver.get('database')
            if db is not None:
                assert db.get('type') == 'mysql'
                user=db.get('user')
                password=db.get('password')
                host =db.get('host')
                port =db.get('port')
                schema =db.get('schema')
                self.conn = create_engine('mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'.format(user,password,host,port,schema))
            log = saver.get('logfile')
            if log is not None:
                self.log = log.get('path')
        tasks_config = config.get('tasks')
        tasks_config = list(filter(lambda x:x.get('enable')==True,tasks_config))
        self.tasks = list(map(lambda x:Task(x),tasks_config))
    def run_batch(self):
        total = len(self.tasks)
        print('start to run {} tasks!'.format(total))

        return

def main_entry():
    parser = argparse.ArgumentParser()
    #parser.add_argument('-d','--data',type=int,default=0,help='create stock num to train model')
    parser.add_argument('-c','--config',help='load config file')
    parser.add_argument('-b','--batch',help='run task in batch',action='store_true', default=False)
    parser.add_argument('-o','--once',help='run task once',action='store_true', default=False)
    parser.add_argument('-r','--run',help='run task once')
    parser.add_argument('-i','--interact',help='run in interact mode',action='store_true',default=False)
    parser.add_argument('-v','--verbose',help='run in verbose mode',action='store_true',default=False)
    args = parser.parse_args()
    print(args)
    if args.run:
        filepath = args.run
        
        dirpath,filename = os.path.split(os.path.abspath(filepath)) 
        sys.path.append(dirpath)
        mod=importlib.import_module(filename.split('.')[0])
        MyTask = getattr(mod,'MyTask')
        task = MyTask()
         
        res = task.action()
        task.save(res)
        if args.interact:
            IPython.embed()

if __name__=='__main__':
    #main_entry()
    task = Task()
    task.preload_data('stock_trade')
    IPython.embed()
    '''
    #stock_engine.load_stock_info()
    parser = argparse.ArgumentParser()
    #parser.add_argument('-d','--data',type=int,default=0,help='create stock num to train model')
    parser.add_argument('-c','--config',help='load config file')
    parser.add_argument('-b','--batch',help='run task in batch',action='store_true', default=False)
    parser.add_argument('-r','--run',help='run task once')
    args = parser.parse_args()
    print('hello')
    config_file = args.config if args.config is not None else './config.json'
    if args.run:
        filepath = args.run
        exec(filepath) 
        pass
    elif args.batch:
        task_engine = TaskEngine(config_file)
        task_engine.run_batch()
    '''

