#### IMPORTANT ! DO NOT DELETE
#To get patient records if multiple ts existis 


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
from collections import namedtuple

df_ts_records_columns = ['RECORD','TIME','HR', 'SPO2','ABPSYS','ABPDIAS','ABPMEAN','RESP'] 
df_ts_records = pd.DataFrame(columns=df_ts_records_columns); 
#subject_id= 48149; # per second multiple 
#icu_intime = datetime.datetime(2127, 5, 25, 8, 34,39) # for 48149
#icu_outtime = datetime.datetime(2127, 6, 16, 1, 15,22) # for 48149

subject_id= 55638; # per minute 

icu_intime = datetime.datetime(2106, 11, 25, 12, 37,32) # for 55638

icu_outtime = datetime.datetime(2106, 11, 27, 10, 49,33) # for 55638


print ('icu intime =', icu_intime)
print ('icu outtime', icu_outtime)
"""
subject_id= 59864;

icu_intime = datetime.datetime(2173, 5, 16, 12, 14,45)
print ('icu intime =', icu_intime)

icu_outtime = datetime.datetime(2173, 6, 8, 15, 45,23)
print ('icu outtime', icu_outtime)

#2173-05-16 12:14:45,2173-06-08 15:45:23,
"""
wdb_dir_path = 'mimic3wdb/matched/p'+ str(subject_id).zfill(6)[:2] + '/p' + str(subject_id).zfill(6) + '/';
wdb_path_toAllRecords = 'https://archive.physionet.org/physiobank/database/'+ wdb_dir_path + 'RECORDS';
wdb_records =  urllib.request.urlopen(wdb_path_toAllRecords);  
try:
  df_ts_records.drop(df_ts_records.index, inplace=True)
except:
  print('df_ts_records does not exist')
count_overlap = 0; 
for lines in wdb_records.readlines():
    record = lines.decode("utf-8"); 
    record = str(record).rstrip()
    #print (record[-1:])
    if record[-1:] == 'n':
      print(record);
      #print (wdb_dir_path);
      record = str(record).rstrip()
      
      
      #try:
      signals =''
      fields = ''
      signals,fields = wfdb.rdsamp(record, pn_dir=wdb_dir_path) ; 
        
      list_sig_name = [item.upper().replace(' ','') for item in fields['sig_name']]

      sig_exist_1 = all(x in list_sig_name for x in ['HR', 'SPO2','ABPSYS','ABPDIAS','ABPMEAN','RESP']);  #%SpO2
      sig_exist_2 = all(x in list_sig_name for x in ['HR', '%SPO2','ABPSYS','ABPDIAS','ABPMEAN','RESP']); 

      record_starttime = datetime.datetime.combine(fields['base_date'] ,fields['base_time'] ) ;
            
      if  '%.3f'%(fields['fs']) == '1.000' :
        record_endtime = record_starttime + datetime.timedelta(seconds= (fields['sig_len']-1)) ;
      elif '%.3f'%(fields['fs'])== '0.017' :
        record_endtime = record_starttime + datetime.timedelta(minutes = (fields['sig_len']-1)) ;
      else : 
        print('ERROR IN SAMPLING');
        print(record);
        print(wdb_dir_path);

      print('record START time:  ', record_starttime)
      print('record END time:  ', record_endtime)
      Range = namedtuple('Range', ['start', 'end'])
      r1 = Range(start= icu_intime, end= icu_outtime)
      r2 = Range(start= record_starttime, end = record_endtime)
      latest_start = max(r1.start, r2.start)
      earliest_end = min(r1.end, r2.end)
      delta = (earliest_end - latest_start).days + 1
       #delta >= 0 :
      print('sig_exist_1 : ', sig_exist_1)
      print('sig_exist_2 : ', sig_exist_2)
      print('delta : ', delta)
      if ( ((sig_exist_1 == True) or (sig_exist_2 == True)) and (delta >= 0)):
        ###
        try:
          df_ts_indv_record_temp.drop(df_ts_indv_record_temp.index, inplace=True)
        except:
          print('individual record for a single patient df does not exists')
          
        df_ts_indv_record_temp = pd.DataFrame(columns = df_ts_records_columns ) # individual record for a single patient #safiya
        ###

        df_row_idx = df_ts_records.shape[0] ;
        print('length of signal: ', len(signals))
        print('index of dataframe before inserting into it: ', df_row_idx)
         
        for i in fields['sig_name']:
          
          if i.upper().replace(' ','') == 'HR':
            idx_HR = fields['sig_name'].index(i);
          elif (( i.upper().replace(' ','') == 'SPO2') or (i.upper().replace(' ','') =='%SPO2')):
            idx_SPO2 = fields['sig_name'].index(i);
          elif i.upper().replace(' ','') == 'ABPSYS' :
            idx_ABPSYS = fields['sig_name'].index(i);
          elif i.upper().replace(' ','') == 'ABPDIAS' :
            idx_ABPDIAS = fields['sig_name'].index(i);
          elif i.upper().replace(' ','') == 'ABPMEAN' :
            idx_ABPMEAN = fields['sig_name'].index(i);
          elif i.upper().replace(' ','') == 'RESP' :
            idx_RESP = fields['sig_name'].index(i);
            
        
        
        if count_overlap == 0 : 
            if record_starttime > icu_intime:
              print('inserting nulls before the record start time')
              #print( (datetime.datetime.strptime((icu_intime.strftime('%Y-%m-%d %H:%M' )), '%Y-%m-%d %H:%M'))  ) #+ datetime.timedelta(seconds= int(record_starttime.strftime('%S')))  )
              #print(icu_intime.strftime('%Y-%m-%d %H:%M'))

              if '%.3f'%(fields['fs'])== '0.017' :
                minutes_to_insert_start = (datetime.datetime.strptime((record_starttime.strftime('%Y-%m-%d %H:%M' )), '%Y-%m-%d %H:%M'))- (datetime.datetime.strptime((icu_intime.strftime('%Y-%m-%d %H:%M' )), '%Y-%m-%d %H:%M'))
              elif '%.3f'%(fields['fs'])==  '1.000' :
                minutes_to_insert_start = record_starttime - icu_intime

              print('minutes_to_insert_start:  ', minutes_to_insert_start)
              duration_in_s = minutes_to_insert_start.total_seconds()
              minutes_to_insert_start = divmod(duration_in_s, 60)[0] - 1 

              try:
                df_ts_records_time_temp_start.drop(df_ts_records_time_temp_start.index,  inplace=True)
              except :
                print( 'df_ts_records_time_temp_start does not exist')
              

              df_ts_records_time_temp_start = pd.DataFrame(columns=df_ts_records_columns)

              if '%.3f'%(fields['fs'])== '0.017' :
                df_ts_records_time_temp_start['TIME'] = pd.date_range(icu_intime + datetime.timedelta(minutes=1), 
                                                              periods=minutes_to_insert_start, freq='1min'); 

              elif '%.3f'%(fields['fs'])== '1.000' :
                df_ts_records_time_temp_start['TIME'] = pd.date_range(icu_intime + datetime.timedelta(seconds=1), 
                                                              periods= (duration_in_s-1), freq='S'); 

              print ('INSERTING ONLY NULL IN START:')
              print (df_ts_records_time_temp_start)
              df_ts_indv_record_temp = df_ts_indv_record_temp.append(df_ts_records_time_temp_start, ignore_index=True);
              print('inserting nulls in start IN INDV LEVEL')
              print(df_ts_indv_record_temp)




            try:
              df_ts_records_temp.drop(df_ts_records_temp.index,  inplace=True)
            except:
              print( 'df_ts_records_temp does not exist')

            df_ts_records_temp = pd.DataFrame(columns=df_ts_records_columns)
            df_ts_records_temp['HR']= signals[:,idx_HR ] 
            df_ts_records_temp['SPO2']= signals[:,idx_SPO2 ] 
            df_ts_records_temp['ABPSYS']= signals[:,idx_ABPSYS ] 
            df_ts_records_temp['ABPDIAS']= signals[:,idx_ABPDIAS ] 
            df_ts_records_temp['ABPMEAN']= signals[:,idx_ABPMEAN ] 
            df_ts_records_temp['RESP']= signals[:,idx_RESP ] 


            if '%.3f'%(fields['fs'])== '0.017' :
              df_ts_records_temp['TIME'] = pd.date_range(record_starttime, periods=fields['sig_len'], freq='1min'); 
            elif '%.3f'%(fields['fs'])== '1.000' :
              df_ts_records_temp['TIME'] = pd.date_range(record_starttime, periods=fields['sig_len'], freq='S'); 

            df_ts_records_temp.TIME = pd.to_datetime(df_ts_records_temp.TIME)
            df_ts_indv_record_temp = df_ts_indv_record_temp.append(df_ts_records_temp, ignore_index=True); #safiya

            print('inserting nulls in start + first record data')
            print(df_ts_indv_record_temp)

            if '%.3f'%(fields['fs'])== '1.000' : #safiya
              print("AGGREGATING")
              start_idx = 0;
              df_ts_records_new = pd.DataFrame(columns=df_ts_records_columns);
              #print('length of new df  '  , df_ts_records_new.shape[0] )
              for index, rows in df_ts_indv_record_temp.iterrows():
                print('start index for first: ', start_idx)
                if start_idx >= df_ts_indv_record_temp.shape[0]:
                  exit;
                else: 
                  
                  #print(df_ts_records.iloc[start_idx: (start_idx+60), 2:8])
                  array = np.array( df_ts_indv_record_temp.iloc[start_idx: (start_idx+60), 2:8].mean(axis=0))
                  #print('printing array of average')
                  #print (array)

                  current_index = df_ts_records_new.shape[0]
                  df_ts_records_new.loc[current_index ,'HR']= array[0]
                  df_ts_records_new.loc[current_index,'SPO2']= array[1]
                  df_ts_records_new.loc[current_index,'ABPSYS']= array[2]
                  df_ts_records_new.loc[current_index,'ABPDIAS']= array[3]
                  df_ts_records_new.loc[current_index,'ABPMEAN']= array[4]
                  df_ts_records_new.loc[current_index,'RESP']= array[5]

                  #print(df_ts_records_new)
                  #print('next average')
                  start_idx = start_idx+60;
                  #print('start index :: ' , start_idx)

              print('# record time:  ',df_ts_records_new.shape[0])
              df_ts_records_new['TIME'] = pd.date_range(df_ts_indv_record_temp.loc[0,'TIME'], periods= df_ts_records_new.shape[0], freq='1min'); 
              df_ts_records_new.TIME = pd.to_datetime(df_ts_records_new.TIME)
              #print(df_ts_records_new)

              df_ts_indv_record_temp.drop(df_ts_indv_record_temp.index, inplace=True);
              #df_ts_records = pd.DataFrame(columns=df_ts_records_columns)
              df_ts_records = df_ts_records.append(df_ts_records_new, ignore_index=True);
              print('only first record  aggregated at individual record level: ')
              print(df_ts_records_new)
              print('inserting aggregated first record into  FINAL SUBJEC DATAFRAME')
              print(df_ts_records)
              df_ts_records_new.drop(df_ts_records_new.index, inplace=True)
              df_ts_records['RECORD'] = record   

            else:
              df_ts_records = df_ts_records.append(df_ts_indv_record_temp, ignore_index=True);
              df_ts_records['RECORD'] = record   

              print('inserting nulls in start + first record data into FINAL SUBJEC DATAFRAME')
              print(df_ts_records)
           
               
        else:
            if record_starttime <= icu_outtime :
              last_Record_time = df_ts_records.loc[(df_row_idx-1),'TIME']
              print('main DF last time record: ',last_Record_time )

              if '%.3f'%(fields['fs'])== '0.017' :
                minutes_to_insert = (datetime.datetime.strptime((record_starttime.strftime('%Y-%m-%d %H:%M' )), '%Y-%m-%d %H:%M')) - (datetime.datetime.strptime((last_Record_time.strftime('%Y-%m-%d %H:%M' )), '%Y-%m-%d %H:%M'))
              elif '%.3f'%(fields['fs'])== '1.000' :
                minutes_to_insert = record_starttime - last_Record_time

              duration_in_s = minutes_to_insert.total_seconds()
              minutes_to_insert = divmod(duration_in_s, 60)[0] - 1

              print ('minutes_to_insert:  ', minutes_to_insert);
              print('seconds to insert: ', duration_in_s)


              try:
                df_ts_records_time_temp.drop(df_ts_records_time_temp.index, inplace=True);
                df_ts_records_temp.drop(df_ts_records_temp.index, inplace=True);
              except:
                print ('df_ts_records_temp and df_ts_records_time_temp does not exits')

              df_ts_records_time_temp = pd.DataFrame(columns=df_ts_records_columns)
              if '%.3f'%(fields['fs'])== '0.017' :
                df_ts_records_time_temp['TIME'] = pd.date_range(last_Record_time + datetime.timedelta(minutes=1), 
                                                              periods=minutes_to_insert, freq='1min'); 
              elif  '%.3f'%(fields['fs'])== '1.000' :
                print('last record time' , last_Record_time)
                print('(duration_in_s-1)' , (duration_in_s-1))
                df_ts_records_time_temp['TIME'] = pd.date_range(last_Record_time + datetime.timedelta(seconds=1), 
                                                              periods=(duration_in_s-1), freq='S'); 
              print ('INSERTING ONLY NULL UNTILL NEXT RECORD START TIME:')
              print (df_ts_records_time_temp)

              df_ts_indv_record_temp = df_ts_indv_record_temp.append(df_ts_records_time_temp, ignore_index=True);
              print('inserting nulls UNTILL NEXT RECORD START TIME INTO INDV LEVEL')
              print(df_ts_indv_record_temp)

              df_ts_records_temp = pd.DataFrame(columns=df_ts_records_columns)
          
              df_ts_records_temp['HR']= signals[:,idx_HR ] 
              df_ts_records_temp['SPO2']= signals[:,idx_SPO2 ] 
              df_ts_records_temp['ABPSYS']= signals[:,idx_ABPSYS ] 
              df_ts_records_temp['ABPDIAS']= signals[:,idx_ABPDIAS ] 
              df_ts_records_temp['ABPMEAN']= signals[:,idx_ABPMEAN ] 
              df_ts_records_temp['RESP']= signals[:,idx_RESP ] 
              if '%.3f'%(fields['fs'])== '0.017' :
                df_ts_records_temp['TIME'] = pd.date_range(record_starttime, periods=fields['sig_len'], freq='1min'); 
              elif  '%.3f'%(fields['fs'])== '1.000' :
                df_ts_records_temp['TIME'] = pd.date_range(record_starttime, periods=fields['sig_len'], freq='S'); 

              df_ts_records_temp.TIME = pd.to_datetime(df_ts_records_temp.TIME)
            
              print('before appending: ')
            
              print( df_ts_records_temp);
              df_ts_indv_record_temp = df_ts_indv_record_temp.append(df_ts_records_temp, ignore_index=True);

              print('inserting nulls in start + SECOND record data')
              print(df_ts_indv_record_temp)

              if '%.3f'%(fields['fs'])== '1.000' : #safiya
                start_idx = 0;
                df_ts_records_new = pd.DataFrame(columns=df_ts_records_columns);
                #print('length of new df  '  , df_ts_records_new.shape[0] )
                for index, rows in df_ts_indv_record_temp.iterrows():
                  if start_idx >= df_ts_indv_record_temp.shape[0]:
                    exit;
                  else: 
                    
                    #print(df_ts_records.iloc[start_idx: (start_idx+60), 2:8])
                    array = np.array( df_ts_indv_record_temp.iloc[start_idx: (start_idx+60), 2:8].mean(axis=0))
                    #print('printing array of average')
                    #print (array)

                    current_index = df_ts_records_new.shape[0]
                    df_ts_records_new.loc[current_index ,'HR']= array[0]
                    df_ts_records_new.loc[current_index,'SPO2']= array[1]
                    df_ts_records_new.loc[current_index,'ABPSYS']= array[2]
                    df_ts_records_new.loc[current_index,'ABPDIAS']= array[3]
                    df_ts_records_new.loc[current_index,'ABPMEAN']= array[4]
                    df_ts_records_new.loc[current_index,'RESP']= array[5]

                    #print(df_ts_records_new)
                    #print('next average')
                    start_idx = start_idx+60;
                    #print('start index :: ' , start_idx)

                print('# record time:  ',df_ts_records_new.shape[0])
                df_ts_records_new['TIME'] = pd.date_range(df_ts_indv_record_temp.loc[0,'TIME'], periods= df_ts_records_new.shape[0], freq='1min'); 
                df_ts_records_new.TIME = pd.to_datetime(df_ts_records_new.TIME)
                #print(df_ts_records_new)

                df_ts_indv_record_temp.drop(df_ts_indv_record_temp.index, inplace=True);
                #df_ts_records = pd.DataFrame(columns=df_ts_records_columns)
                df_ts_records = df_ts_records.append(df_ts_records_new, ignore_index=True);
                
                print('only first record  aggregated at individual record level: ')
                print(df_ts_records_new)
                print('inserting aggregated first record into  FINAL SUBJEC DATAFRAME')
                print(df_ts_records)
                df_ts_records_new.drop(df_ts_records_new.index, inplace=True)
                df_ts_records['RECORD'] = record   

              else:
                df_ts_records = df_ts_records.append(df_ts_indv_record_temp, ignore_index=True);
                df_ts_records['RECORD'] = record   
                print('inserting nulls in start + first record data into FINAL SUBJEC DATAFRAME')
                print(df_ts_records)
           
              
        count_overlap = count_overlap +1
        print('overlap count after all insertions: ', count_overlap )
      else:
        print('Either all 6 signals not exists or there is no overlapt with recording time and ICU in time and out time')


last_record_idx = df_ts_records.shape[0] - 1
all_records_end_time = df_ts_records.loc[last_record_idx,'TIME']
      
if (all_records_end_time < icu_outtime  ):
  #print('INSERTING NULLS AT THE END')
  try:
    df_ts_records_time_temp_end.drop(df_ts_records_time_temp_end.index, inplace=True)
  except:
    print('df_ts_records_time_temp_end does not exists')
  #print('main DF last time record: ',last_Record_time )
  if '%.3f'%(fields['fs'])== '0.017' :
    minutes_to_insert_end =  (datetime.datetime.strptime((icu_outtime.strftime('%Y-%m-%d %H:%M' )), '%Y-%m-%d %H:%M')) - (datetime.datetime.strptime((all_records_end_time.strftime('%Y-%m-%d %H:%M' )), '%Y-%m-%d %H:%M')) 
  elif '%.3f'%(fields['fs'])== '1.000' :
    minutes_to_insert_end = icu_outtime - all_records_end_time;

  duration_in_s = minutes_to_insert_end.total_seconds()
  minutes_to_insert_end = divmod(duration_in_s, 60)[0] - 1
  df_ts_records_time_temp_end = pd.DataFrame(columns=df_ts_records_columns)
        
  df_ts_records_time_temp_end['TIME'] = pd.date_range(all_records_end_time + datetime.timedelta(minutes=1), 
                                                              periods=minutes_to_insert_end, freq='1min'); 
  df_ts_records = df_ts_records.append(df_ts_records_time_temp_end, ignore_index=True);
      
  df_ts_records['RECORD'] = record

print('printing final data for this patient')
print(df_ts_records)
