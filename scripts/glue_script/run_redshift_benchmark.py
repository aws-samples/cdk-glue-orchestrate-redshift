#!/usr/bin/python
# This is the Glue python-shell script to perform benchmark test for Redshift: sequential test and concurrent test
# Author : Liulu He
# Version: 1.0
# -*- coding: UTF-8 -*-

"""
   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 
   Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
   with the License. A copy of the License is located at
 
       http://www.apache.org/licenses/LICENSE-2.0
 
   or in the 'license' file accompanying this file. This file is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES
   OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions
   and limitations under the License.
 """

import pandas as pd
import datetime
import os, threading
import random, time
from queue import Queue
import logging
import threading
import sys, getopt
import boto3
from awsglue.utils import getResolvedOptions
import subprocess

# Install required packages
subprocess.run(["pip3 install psycopg2 -t /tmp/"], shell=True)
subprocess.run(["pip3 install DBUtils -t /tmp/"], shell=True)
import psycopg2
from dbutils.pooled_db import PooledDB

class RS_Benchmark_Operator(object):
    def __init__(self,host,user,password,sql_file_root,port='5439',dbname='dev',num_sql_files=10, result_cache='off',num_runs=1,parallel_level=10):
        self.dbname=dbname
        self.host=host
        self.port=port
        self.user=user
        self.password=password
        self.sql_file_root = sql_file_root
        self.num_sql_files=num_sql_files
        self.num_runs=num_runs
        self.result_cache = result_cache
        self.parallel_level = parallel_level
        self._pool=None

        # Set up logging
        self.root = logging.getLogger()
        self.root.setLevel(logging.INFO)

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.root.addHandler(handler)
        self.root.info("Start custom logging...")
        self.rs_con_pool()
        self.result_summary=pd.DataFrame(columns=['testName','query','round','testType','startTime','endTime','elapseSeconds'])
        self.test_name = 'Benchmark-%s'%datetime.datetime.now()

        
       
    def rs_con_pool(self):
        try:
            self.root.info('Begin to create {0} postgresql pool on：{1}.\n'.format(self.host, datetime.datetime.now()))
    
            pool = PooledDB(
                creator=psycopg2,  # 使用链接数据库的模块mincached
                maxconnections=450,  # 连接池允许的最大连接数，0和None表示不限制连接数
                mincached=10,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
                maxcached=4,  # 链接池中最多闲置的链接，0和None不限制
                blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
                maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
                setsession=[],  # 开始会话前执行的命令列表。
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.dbname)
            self._pool=pool
            self.root.info('SUCCESS: create postgresql connection pool success.\n')
        except Exception as e:
            self.root.error('ERROR: create postgresql pool failed：%s\n'%e)
            raise e
    def rs_select(self, sql, path,type,runs):
         # 执行查询
         try:
             conn = self._pool.connection()
             cursor = conn.cursor()
             cursor.execute("set enable_result_cache_for_session to %s;" %self.result_cache)
             startTime = datetime.datetime.now()
             cursor.execute(sql)
             result = cursor.fetchall()
             endTime = datetime.datetime.now()
             costSeconds = (endTime - startTime).seconds
             costMicroSeconds = (endTime - startTime).microseconds
             costTime = round(costSeconds+costMicroSeconds/1000000,2)

             # Record the result to dataframe and log 
             # columns=['query','round','testType','startTime','endTime','elapseSeconds']
             self.result_summary=self.result_summary.append(pd.Series([self.test_name,path,runs,type,startTime,endTime,costTime],index=['testName','query','round','testType','startTime','endTime','elapseSeconds']),ignore_index=True)
             self.root.info('QUERY SUCCESS: script %s success and take %s seconds.'%(path,costTime))
         except Exception as e:
             self.root.error('ERROR: execute  {0} causes error'.format(path))
             self.result_summary=self.result_summary.append(pd.Series([self.test_name,path,runs,type,'error','error',0.0],index=['testName','query','round','testType','startTime','endTime','elapseSeconds']),ignore_index=True)
         finally:
             cursor.close()
             conn.close()
         
    def close_pool(self):
        if self._pool != None:
            self._pool.close()
    def get_name(self):
        return self.test_name

    def get_result(self):
        self.root.info('Final result: \n %s'%self.result_summary.to_string())
        return self.result_summary
    
    def run_parallel_task(self):
        
        q = Queue(maxsize=self.parallel_level)  # 设定最大队列数和线程数
        for i in range(1,self.num_runs+1):
            st = time.time()
            self.root.info("Start %s round of concurrent query test"%i)
            for j in range(1,self.num_sql_files+1):
                queryPath = self.sql_file_root+"query"+str(j) +".sql"

                # open querySQL x
                queryFile = open(queryPath,'r')
                querySQL = queryFile.read()
                # Thread run sql
                t = threading.Thread(target=self.rs_select,args=(querySQL,queryPath,'concurrent',i))
                q.put(t)
                if q.full() == True:
                    thread_list = []
                    while q.empty() == False:
                        t = q.get()
                        thread_list.append(t)
                        t.start()
                    for t in thread_list:
                        t.join()
            et=time.time()
            tt=round(et - st, 3)
            self.root.info("ROUND SUCCESS: %s round concurrent test and took %s seconds in total"%(i, tt))
            self.result_summary=self.result_summary.append(pd.Series([self.test_name,'total',i,'concurrent',st,et,tt],index=['testName','query','round','testType','startTime','endTime','elapseSeconds']),ignore_index=True)
    

    
    def run_sequential_task(self):
        for i in range(1,self.num_runs+1):
            st = time.time()
            self.root.info("Start %s round sequential query test"%i)
            for j in range(1,self.num_sql_files+1):
                queryPath = self.sql_file_root+"query"+str(j) +".sql"
                self.root.info("Start %s run" %queryPath)

                # open querySQL x
                queryFile = open(queryPath,'r')
                querySQL = queryFile.read()
                # Thread run sql
                self.rs_select(querySQL,queryPath,'sequential',i)
            et=time.time()
            tt=round(et - st, 3)
            self.root.info("ROUND SUCCESS: %s round concurrent test and took %s seconds in total"%(i, tt))
            self.result_summary=self.result_summary.append(pd.Series([self.test_name,'total',i,'sequential',st,et,tt],index=['testName','query','round','testType','startTime','endTime','elapseSeconds']),ignore_index=True)
    

if __name__ == '__main__':
    #logging.basicConfig(format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',level=logging.DEBUG)
       
    host=None
    user=None
    password=None
    port=None
    dbname=None
    num_files=10         #only provide 10 queries to run, can bring more but need to change this number
    result_cache='off'
    num_runs=None
    # query path without number. My example path is /home/ec2-user/environment/python_project/rs_sql_file/query10.sql
    sql_file_path = 'redshift_script/tpcds_queries' 
    mode = 'sequential'
    parallel_level = 10 # number of queries runs in parallel during concurrent query task 
    ############################# Use pass-in parameter #######################################################################
    args = getResolvedOptions(sys.argv,['dbname',
                                    'host',
                                    'port',
                                    'username',
                                    'mode',
                                    'password',
                                    'sql_script_bucket',
                                    'sql_script_key',
                                    'num_runs',
                                    'parallel_level',
                                    'num_files'])
    host=args['host']
    user=args['username']
    password=args['password']
    port=args['port']
    dbname=args['dbname']
    mode=args['mode']
    sql_script_bucket=args['sql_script_bucket']
    sql_script_key=args['sql_script_key']
    num_runs=int(args['num_runs'])
    parallel_level=int(args['parallel_level'])
    num_files=int(args['num_files'])

    # Download SQL files from S3 to local and store in local path same as S3 path
    import boto3
    s3 = boto3.resource('s3')
    
    bucket = s3.Bucket(sql_script_bucket) 
    for obj in bucket.objects.filter(Prefix = sql_script_key):
        if not os.path.exists(os.path.dirname(obj.key)):
            os.makedirs(os.path.dirname(obj.key))
        bucket.download_file(obj.key, obj.key) # save to same path

    sql_file_path = args['sql_script_key']

    ###########################################################################################################################

    ############################# Use command to get input #############################################################################
    """
    opts,args = getopt.getopt(sys.argv[1:],'-h:-po:-u:-p:-d:-path -f:-c:-n:',['host=','port=','user=','password','dbname=','sql_file_path=','num_files=','result_cache=','num_runs='])
    
    for opt_name,opt_value in opts:
        if opt_name in ('-h','--host'):
            host=opt_value
            print("[*] host url is ",host)
        if opt_name in ('-m','--mode'):
            mode=opt_value
            print("[*] mode is ",mode)
        
        if opt_name in ('-u','--user'):
            user=opt_value
            print("[*] username is ",user)
       
        if opt_name in ('-p','--password'):
            password = opt_value
        
        if opt_name in ('-po','--port'):
            port = opt_value
            print("[*] db port is ",dbname)
            
        if opt_name in ('-d','--dbname'):
            dbname = opt_value
            print("[*] db name is ",dbname)
            
        if opt_name in ('-f','--num_files'):
            num_files = opt_value
            print("[*] number of files ",num_files)
            
        if opt_name in ('-c','--result_cache'):
            result_cache = opt_value
            print("[*] result cache is ",result_cache)
        if opt_name in ('-n','--num_runs'):
            num_runs = opt_value
            print("[*] number of runs ",num_runs)
        if opt_name in ('-path','--sql_file_path'):
            sql_file_path = opt_value
            print("[*] sql files path is ",sql_file_path)
    ###########################################################################################
    """   
    db=RS_Benchmark_Operator(host,user,password,sql_file_path,port,dbname,num_runs=num_runs,parallel_level=parallel_level,num_sql_files=num_files)

    if mode =='concurrent':
        # Start concurrent query task
        db.run_parallel_task()
    elif mode =='sequential':
        # Start sequantial query task
        db.run_sequential_task()
    else:
        raise Exception('Mode %s is not acceptable. Should be concurrent or sequential'%mode)
    
    db.close_pool()

    db.get_result().to_csv(sql_file_path+db.get_name(), index=False)
    bucket.upload_file(sql_file_path+db.get_name(), 'report/'+db.get_name())