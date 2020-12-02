#example to send to Marcela
!pip install wfdb
import io
import pandas as pd
from IPython.display import display
import matplotlib.pyplot as plt
%matplotlib inline
import numpy as np
import os
import shutil
import posixpath
import wfdb
import urllib.request
import datetime
"""
'HR',
  'PULSE', 
  'ABP SYS',
  'ABP DIAS',
"""
channels = ['ABP DIAS','ABP SYS','PULSE','temp','HR']
signals,fields = wfdb.rdsamp('p042930-2190-07-28-20-30n', pn_dir='mimic3wdb/matched/p04/p042930/')
wfdb.plot_items(signal=signals, fs=fields['fs'])
display(signals)
display(fields)
print ('fs' , fields['fs']);
print ('signal length',fields['sig_len']);
print ('date' ,fields['base_date'] );        
print ('time' ,fields['base_time'] );
record_starttime = datetime.datetime.combine(fields['base_date'] ,fields['base_time'] ) ;
print ('%.3f'%(fields['fs']))
if  '%.3f'%(fields['fs']) == '1.000':
  print ('Sampled once per second')
  record_endtime = record_starttime + datetime.timedelta(seconds = (fields['sig_len']-1)) ;
elif '%.3f'%(fields['fs'])== '0.017' :
  print('Sampled once per minute')
  record_endtime = record_starttime + datetime.timedelta(minutes = (fields['sig_len']-1)) ;
else :
  print('ERROR IN SAMPLING')  
