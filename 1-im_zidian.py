#!/usr/bin/env python
#coding=utf-8 

import logging
import traceback
import jieba
#import word2vec
import codecs
import gensim
from gensim.models import word2vec
from impala.dbapi import connect
from impala.util import as_pandas
import pandas as pd
import datetime
import time
import sys
import os
try:
    reload(sys)
    sys.setdefaultencoding('utf-8')
except:
    pass

yesterday = (datetime.date.today()+ datetime.timedelta(days=-1)).strftime('%Y%m%d')

SQL = 'SELECT * FROM diagnosis_text_in where pt=\''+ yesterday +'\' and length(main_suit)>=4 and dept_name=\'儿科\'' 
print SQL

tag = 'SELECT * FROM mid_dim_tag'

jieba.set_dictionary('/home/rd/dict/dict_my.txt')

jieba.enable_parallel(16)
stopkey=[line.strip().decode('utf-8') for line in open('/home/rd/dict/stop_word.txt').readlines()]  

conn = connect(host='10.129.64.165', port=10000,database='jkgj_log',auth_mechanism='GSSAPI',kerberos_service_name='hive')
cursor = conn.cursor()

cursor.execute(SQL)
results = cursor.fetchall()

cursor1 = conn.cursor()
cursor1.execute(tag)

tags = cursor1.fetchall()

for i in tags:
    jieba.add_word(str(i[2]))
    jieba.add_word(str(i[1]))
#print cursor.description  # prints the result set's schema

#results = cursor.fetchall()
#for i in results:
#   for t in i:
#      print t,
#   print ""

#df = as_pandas(cursor)



#cursor.execute('drop table if exists jkgj_log.cy_tmp_text_dialog_out_1120')

files = '/home/rd/python/output/dialog_keyword/dialog_keyword_child'+yesterday+'.txt'


t1 = time.time()
fout1 = open(files,'w')
try:
    for i in results:
        jiebas= jieba.cut_for_search(i[5]+i[6]+i[7])
        wordList = list(set(jiebas)-set(stopkey))
        for word in wordList:
           word = word.strip().decode('utf8')
           print >> fout1,word
            #print i[3],i[5]
    print success
    #SQL = "use jkgj_log;LOAD DATA LOCAL INPATH \'"+files+"\' OVERWRITE INTO TABLE mid_consult_words PARTITION (pt=\'"+yesterday+"\')"  
    #print SQL
    #CMD='/usr/bin/hadoop fs -rm /user/hive/warehouse/jkgj_log.db/mid_consult_words/pt='+yesterday+'/*'
    #print CMD
    #os.system(CMD)
    #CMD='/usr/bin/hadoop fs -put '+files+' /user/hive/warehouse/jkgj_log.db/mid_consult_words/pt='+yesterday
    #print CMD
    #os.system(CMD)
except:
    f = open("/home/rd/python/tmp/main_suit_log.log", 'a')
    traceback.print_exc(file=f)
    f.flush()
    f.close()
    fout1.close()




