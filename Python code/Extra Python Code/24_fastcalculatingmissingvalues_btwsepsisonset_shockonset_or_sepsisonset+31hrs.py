# -*- coding: utf-8 -*-
"""24_FastCalculatingMissingValues_BtwSepsisOnset_ShockOnset_OR_SepsisOnset+31hrs.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1DCF-gvOcHarmGUZTPDfDghyxYOaRMJ-M
"""

# Commented out IPython magic to ensure Python compatibility.

!pip install wfdb
import io
import pandas as pd
from IPython.display import display
import matplotlib.pyplot as plt
# %matplotlib inline
import numpy as np
import os
import shutil
import posixpath
import wfdb
import urllib.request
import datetime
from collections import namedtuple

"""CHECKING FOR DATA FOR SEPSIS PATIENTS BETWEEN SEPSIS ONSET TIME AND SEPSIS ONSET + 31 HOURS AND CALCULATING % MISSING VALUES"""

# to extract sample subject ids to send to Marcela
from google.colab import files
uploaded = files.upload()

df_csvdata = pd.read_csv(io.BytesIO(uploaded['new_df_waveform_exists.csv'])) # this file consists of only sepsis patients ( with and without shock)
# Dataset is now stored in a Pandas Dataframe
print ('shape of original dataframe from CSV : ', df_csvdata.shape)
print(df_csvdata.columns)

df_csvdata = df_csvdata[df_csvdata['sepsis_onsettime'].notna() ]
print(df_csvdata.shape[0])

"""
df_csvdata = df_csvdata[df_csvdata['subject_id']==42904]
print(df_csvdata)
"""

print(df_csvdata.shape[0])

df_csvdata['Sepsis_SepsisOnset+ShockOnsetOR31h_timeoverlap_exists']='';
df_csvdata['Sepsis_SepsisOnset+ShockOnsetOR31h_overlap_duration']=''
df_csvdata['Sepsis_SepsisOnset+ShockOnsetOR31h_Number_of_overlaping_records'] =''
df_csvdata['gap_SO_ShockOnsetORSOplus31']= ''
df_csvdata['Sepsis_SepsisOnset+ShockOnsetOR31h_percentNonMissingData']=''; 


print (df_csvdata.columns)

#df_ts_records_columns = ['RECORD','TIME','HR', 'SPO2','ABPSYS','ABPDIAS','ABPMEAN','RESP'] 
df_ts_records_columns = ['SUBJECT_ID','ICUSTAY_ID','RECORD','TIME','HR', 'SPO2','ABPSYS','ABPDIAS','ABPMEAN','RESP'] 
#df_ts_records_all_patients_columns =  ['SUBJECT_ID','ICUSTAY_ID','RECORD','TIME','HR', 'SPO2','ABPSYS','ABPDIAS','ABPMEAN','RESP']  # main DF for extracting TS of ALL patients
#df_ts_records_all_patients = pd.DataFrame(columns=df_ts_records_all_patients_columns)


for idx, row in df_csvdata.iterrows():
    #print(row['subject_id']);
    try:
      df_ts_records.drop(df_ts_records.index,inplace=True)
    except:
      print('MAIN DF does not exits')

    df_ts_records = pd.DataFrame(columns=df_ts_records_columns);

    wdb_dir_path = 'mimic3wdb/matched/p'+ str(row['subject_id']).zfill(6)[:2] + '/p' + str(row['subject_id']).zfill(6) + '/';
    wdb_path_toAllRecords = 'https://archive.physionet.org/physiobank/database/'+ wdb_dir_path + 'RECORDS';
    wdb_records =  urllib.request.urlopen(wdb_path_toAllRecords);   

    count_overlaping_records_SO_SOplus31 = 0 
    overlap_duration_SO_SOplus31 = '';
    gap_SO_SOplus31 = '';

    count_overlaping_records_AF_AFplus26 = 0 
    overlap_duration_AF_AFplus26 = '';
    gap_AF_AFplus26 = ''

    for lines in wdb_records.readlines():
      record = lines.decode("utf-8"); 
      record = str(record).rstrip()
      #print (record[-1:])
      if record[-1:] == 'n':
        #print(record);
        #print (wdb_dir_path);
        consider_record =0 ;
        record = str(record).rstrip()

        try:
          signals =''
          fields = ''
          #print(wdb_dir_path)
          #print(record)
          signals,fields = wfdb.rdsamp(record, pn_dir=wdb_dir_path) ; 
          list_sig_name = [item.upper().replace(' ','') for item in fields['sig_name']]
          sig_exist_1 = all(x in list_sig_name for x in ['HR', 'SPO2','ABPSYS','ABPDIAS','ABPMEAN','RESP']);  #%SpO2
          sig_exist_2 = all(x in list_sig_name for x in ['HR', '%SPO2','ABPSYS','ABPDIAS','ABPMEAN','RESP']); 
          if ((sig_exist_1 == True) or (sig_exist_2 == True)) :
            consider_record = 1
          else:
            consider_record = 0
          record_starttime = datetime.datetime.combine(fields['base_date'] ,fields['base_time'] ) ;
          if  '%.3f'%(fields['fs']) == '1.000' :
            record_endtime = record_starttime + datetime.timedelta(seconds= (fields['sig_len']-1)) ;
          elif '%.3f'%(fields['fs'])== '0.017' :
            record_endtime = record_starttime + datetime.timedelta(minutes = (fields['sig_len']-1)) ;
          else : 
            print('ERROR IN SAMPLING');
            print(record);
            print(wdb_dir_path);
          #Caculate if we have a recording for the time of icu stay
          Range = namedtuple('Range', ['start', 'end'])

          sepsis_onsettime = '';
          sepsis_onsettime_plus_31h = '' ;
          shock_onsettime = '';


          sepsis_onsettime = datetime.datetime.strptime(row['sepsis_onsettime'],'%Y-%m-%d %H:%M:%S')

          if str(row['sepstic_shock_onsettime']) == 'nan':
            shock_onsettime = 'nan'
          else:
            shock_onsettime = datetime.datetime.strptime(row['sepstic_shock_onsettime'],'%Y-%m-%d %H:%M:%S') 

          
          sepsis_onsettime_plus_31h = (datetime.datetime.strptime(row['sepsis_onsettime'],'%Y-%m-%d %H:%M:%S')  + datetime.timedelta(hours = 31))


          if (str(shock_onsettime) != 'nan') and (shock_onsettime < sepsis_onsettime_plus_31h) :
            #print('SHOCK PATIENT')
            sepsis_onsettime_plus_31h_or_ShockOnsettime = sepsis_onsettime_plus_31h

          elif (str(shock_onsettime) != 'nan') and (shock_onsettime >= sepsis_onsettime_plus_31h) :
            #print('SHOCK PATIENT')
            sepsis_onsettime_plus_31h_or_ShockOnsettime = shock_onsettime

          elif str(shock_onsettime) == 'nan':
            #print('NON SHOCK PATIENT')
            sepsis_onsettime_plus_31h_or_ShockOnsettime = sepsis_onsettime_plus_31h
          

          r1 = Range(start= sepsis_onsettime , end= sepsis_onsettime_plus_31h_or_ShockOnsettime)

          r2 = Range(start= record_starttime, end = record_endtime)

          latest_start = max(r1.start, r2.start)
          earliest_end = min(r1.end, r2.end)
          
          delta = (earliest_end - latest_start).days + 1
          df_row_idx = df_ts_records.shape[0] ;
          
          if ( ((r1.start <= r2.end) and (r2.start <= r1.end) ) and (consider_record ==1) ):
            delta = 0 ;
            delta = ((earliest_end - latest_start).seconds )/60
            overlap_duration_SO_SOplus31 = overlap_duration_SO_SOplus31 + ',' + str(delta)
            df_csvdata.loc[idx,'Sepsis_SepsisOnset+ShockOnsetOR31h_timeoverlap_exists'] = 1;
            

          #if ((delta >= 0 ) & (consider_record ==1)) :
            ###
            try:
              df_ts_indv_record_temp.drop(df_ts_indv_record_temp.index, inplace=True)
            except:
              print('individual record for a single patient df does not exists')
              
            df_ts_indv_record_temp = pd.DataFrame(columns = df_ts_records_columns ) # individual record for a single patient #safiya
            ###
            #print('RECORD EXISTS FOR THE ICU STAYS WITH THE SIGNALS NEEDED : ', row['subject_id'])
            #df_csvdata.loc[idx,'timeoverlap'] = 1;
            #todo : adding new dataframe, exatracting required signals, computing avergage for per sminute values in case of per second sampling frequency
            for i in fields['sig_name']:
              if i.upper().replace(' ','') == 'HR':
                idx_HR='';
                idx_HR = fields['sig_name'].index(i);
              elif (( i.upper().replace(' ','') == 'SPO2') or (i.upper().replace(' ','') =='%SPO2')):
                idx_SPO2 = '';
                idx_SPO2 = fields['sig_name'].index(i);
              elif i.upper().replace(' ','') == 'ABPSYS' :
                idx_ABPSYS = '';
                idx_ABPSYS = fields['sig_name'].index(i);
              elif i.upper().replace(' ','') == 'ABPDIAS' :
                idx_ABPDIAS = '';
                idx_ABPDIAS = fields['sig_name'].index(i);
              elif i.upper().replace(' ','') == 'ABPMEAN' :
                idx_ABPMEAN = '';
                idx_ABPMEAN = fields['sig_name'].index(i);
              elif i.upper().replace(' ','') == 'RESP' :
                idx_RESP = '';
                idx_RESP = fields['sig_name'].index(i);
                
            if count_overlaping_records_SO_SOplus31 == 0 : 
              if record_starttime > sepsis_onsettime:
                #print('inserting nulls between icu intime and record start time')
                minutes_to_insert_start = record_starttime - sepsis_onsettime
                #print('minutes_to_insert_start:  ', minutes_to_insert_start)
                duration_in_s = minutes_to_insert_start.total_seconds()
                minutes_to_insert_start = divmod(duration_in_s, 60)[0] - 1 
                gap_SO_SOplus31 = gap_SO_SOplus31 + ',start:' + str(minutes_to_insert_start)

                try:
                  df_ts_records_time_temp_start.drop(df_ts_records_time_temp_start.index,  inplace=True)
                except :
                  print( 'df_ts_records_time_temp_start does not exist')

                df_ts_records_time_temp_start = pd.DataFrame(columns=df_ts_records_columns)

                if '%.3f'%(fields['fs'])== '0.017' :
                  df_ts_records_time_temp_start['TIME'] = pd.date_range(sepsis_onsettime + datetime.timedelta(minutes=1), 
                                                              periods = minutes_to_insert_start, freq='1min'); 
                elif '%.3f'%(fields['fs'])== '1.000' :
                  df_ts_records_time_temp_start['TIME'] = pd.date_range(sepsis_onsettime + datetime.timedelta(seconds=1), 
                                                              periods = (duration_in_s-1), freq='S');  
                df_ts_indv_record_temp = df_ts_indv_record_temp.append(df_ts_records_time_temp_start, ignore_index=True);

              try:
                df_ts_records_temp.drop(df_ts_records_temp.index,  inplace=True)
              except:
                print( 'df_ts_records_time_temp_start does not exist')

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
              df_ts_indv_record_temp = df_ts_indv_record_temp.append(df_ts_records_temp, ignore_index=True);
              df_ts_indv_record_temp['RECORD'] = record;

              #FOR FAST:
              df_ts_records = df_ts_records.append(df_ts_indv_record_temp, ignore_index=True);
              df_ts_indv_record_temp.drop(df_ts_indv_record_temp.index, inplace=True);
              """

              if '%.3f'%(fields['fs'])== '1.000' : #safiya
                #print('shape of persecond data before aggregation: ', df_ts_indv_record_temp.shape)
                #print('aggregating first record')
                start_idx = 0;
                try:
                  df_ts_records_new.drop(df_ts_records_new.index, inplace=True)
                except:
                  print('df_ts_records_new does not exists')
                df_ts_records_new = pd.DataFrame(columns=df_ts_records_columns);
                #print('length of new df  '  , df_ts_records_new.shape[0] )
                for index, rows in df_ts_indv_record_temp.iterrows():
                  if start_idx >= df_ts_indv_record_temp.shape[0]:
                    exit;
                  else: 
                    #print(df_ts_records.iloc[start_idx: (start_idx+60), 2:8])
                    array = np.array( df_ts_indv_record_temp.iloc[start_idx: (start_idx+60), 4:10].mean(axis=0))
                    current_index = df_ts_records_new.shape[0]
                    df_ts_records_new.loc[current_index ,'HR']= array[0]
                    df_ts_records_new.loc[current_index,'SPO2']= array[1]
                    df_ts_records_new.loc[current_index,'ABPSYS']= array[2]
                    df_ts_records_new.loc[current_index,'ABPDIAS']= array[3]
                    df_ts_records_new.loc[current_index,'ABPMEAN']= array[4]
                    df_ts_records_new.loc[current_index,'RESP']= array[5]
                    start_idx = start_idx+60;

                #print('finished aggregating first record and now inserting into main df for a patient')
                df_ts_records_new['TIME'] = pd.date_range(df_ts_indv_record_temp.loc[0,'TIME'], periods= df_ts_records_new.shape[0], freq='1min'); 
                df_ts_records_new.TIME = pd.to_datetime(df_ts_records_new.TIME)
                df_ts_records_new['RECORD'] = record
                df_ts_records = df_ts_records.append(df_ts_records_new, ignore_index=True); # appending at one subject level
                df_ts_indv_record_temp.drop(df_ts_indv_record_temp.index, inplace=True);
                df_ts_records_new.drop(df_ts_records_new.index, inplace=True)   
                #print('finished aggregating first record')

              else:
                df_ts_records = df_ts_records.append(df_ts_indv_record_temp, ignore_index=True);
                df_ts_indv_record_temp.drop(df_ts_indv_record_temp.index, inplace=True);
              """
                

            else: # when it is a second/ third/... record for one subject
              if record_starttime < sepsis_onsettime_plus_31h_or_ShockOnsettime :
                last_Record_time = df_ts_records.loc[(df_row_idx-1),'TIME']
                #print('main DF last time record: ',last_Record_time )
                minutes_to_insert = record_starttime - last_Record_time
                duration_in_s = minutes_to_insert.total_seconds()
                minutes_to_insert = divmod(duration_in_s, 60)[0] - 1
                gap_SO_SOplus31 = gap_SO_SOplus31 + ',mid:' + str(minutes_to_insert)
                #print ('minutes_to_insert:  ', minutes_to_insert);
                try:
                  df_ts_records_time_temp.drop(df_ts_records_time_temp.index, inplace= True);
                  df_ts_records_temp.drop(df_ts_records_temp.index, inplace=True);
                except:
                  print ('df_ts_records_temp and df_ts_records_time_temp does not exits')
                df_ts_records_time_temp = pd.DataFrame(columns=df_ts_records_columns)
                if '%.3f'%(fields['fs'])== '0.017' :
                  df_ts_records_time_temp['TIME'] = pd.date_range(last_Record_time + datetime.timedelta(minutes=1), 
                                                              periods=minutes_to_insert, freq='1min'); 
                elif '%.3f'%(fields['fs'])== '1.000' :
                  df_ts_records_time_temp['TIME'] = pd.date_range(last_Record_time + datetime.timedelta(seconds=1), 
                                                              periods=(duration_in_s-1), freq='S'); 
                #print ('df_ts_records_time_temp:')
                #print (df_ts_records_time_temp)
                df_ts_indv_record_temp = df_ts_indv_record_temp.append(df_ts_records_time_temp, ignore_index=True);
              
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
                df_ts_indv_record_temp = df_ts_indv_record_temp.append(df_ts_records_temp, ignore_index=True);
                df_ts_indv_record_temp['RECORD'] = record;
                
                #FOR FAST

                df_ts_records = df_ts_records.append(df_ts_indv_record_temp, ignore_index=True);
                df_ts_indv_record_temp.drop(df_ts_indv_record_temp.index, inplace=True);
                
                """
                if '%.3f'%(fields['fs'])== '1.000' : #safiya
                  #print('aggregating second record')
                  start_idx = 0;
                  try:
                    df_ts_records_new.drop(df_ts_records_new.index, inplace=True)
                  except:
                    print('df_ts_records_new does not exists')
                  df_ts_records_new = pd.DataFrame(columns=df_ts_records_columns);
                  #print('length of new df  '  , df_ts_records_new.shape[0] )
                  for index, rows in df_ts_indv_record_temp.iterrows():
                    if start_idx >= df_ts_indv_record_temp.shape[0]:
                      exit;
                    else: 
                      array = np.array( df_ts_indv_record_temp.iloc[start_idx: (start_idx+60), 4:10].mean(axis=0))
                      current_index = df_ts_records_new.shape[0]
                      df_ts_records_new.loc[current_index ,'HR']= array[0]
                      df_ts_records_new.loc[current_index,'SPO2']= array[1]
                      df_ts_records_new.loc[current_index,'ABPSYS']= array[2]
                      df_ts_records_new.loc[current_index,'ABPDIAS']= array[3]
                      df_ts_records_new.loc[current_index,'ABPMEAN']= array[4]
                      df_ts_records_new.loc[current_index,'RESP']= array[5]
                      start_idx = start_idx+60;
                  
                  #print('finished aggregating second record and now inserting into main df for a patient')
                  df_ts_records_new['TIME'] = pd.date_range(df_ts_indv_record_temp.loc[0,'TIME'], periods= df_ts_records_new.shape[0], freq='1min'); 
                  df_ts_records_new.TIME = pd.to_datetime(df_ts_records_new.TIME)
                  df_ts_records_new['RECORD'] = record;
                  #df_ts_records = pd.DataFrame(columns=df_ts_records_columns)
                  df_ts_records = df_ts_records.append(df_ts_records_new, ignore_index=True);
                  df_ts_indv_record_temp.drop(df_ts_indv_record_temp.index, inplace=True);
                  df_ts_records_new.drop(df_ts_records_new.index, inplace=True)
                  #print('finished aggregating second record')

                else:
                  df_ts_records = df_ts_records.append(df_ts_indv_record_temp, ignore_index=True);
                  df_ts_indv_record_temp.drop(df_ts_indv_record_temp.index, inplace=True);
                """     

            
            FS = '%.3f'%(fields['fs'])
            #print(FS)
            count_overlaping_records_SO_SOplus31 = count_overlaping_records_SO_SOplus31 +1
          else:            
            print('RECORD DOES NOT EXISTS FOR THE ICU STAYS WITH THE SIGNALS NEEDED : ', row['subject_id'])
              #df_csvdata.loc[idx,'timeoverlap'] = 0;

        except ValueError:
          print('Error occured while reading waveform: ', record);


    #print((datetime.datetime.strptime(row['intime'],'%Y-%m-%d %H:%M:%S') ) + datetime.timedelta(hours=24))
    try:
      last_record_idx = df_ts_records.shape[0] - 1
      all_records_end_time = df_ts_records.loc[last_record_idx,'TIME']
      
      if (all_records_end_time < sepsis_onsettime_plus_31h_or_ShockOnsettime ):
        #print('INSERTING NULLS AT THE END')
        try:
          df_ts_records_time_temp_end.drop(df_ts_records_time_temp_end.index, inplace=True)
        except:
          print('df_ts_records_time_temp_end does not exists')
        #print('main DF last time record: ',last_Record_time )
        minutes_to_insert_end = sepsis_onsettime_plus_31h_or_ShockOnsettime - all_records_end_time
        duration_in_s = minutes_to_insert_end.total_seconds()
        minutes_to_insert_end = divmod(duration_in_s, 60)[0] - 1
        gap_SO_SOplus31 = gap_SO_SOplus31 + ',end:' + str(minutes_to_insert_end)
        df_ts_records_time_temp_end = pd.DataFrame(columns=df_ts_records_columns)

        if FS == '0.017' :
          df_ts_records_time_temp_end['TIME'] = pd.date_range(all_records_end_time + datetime.timedelta(minutes=1), 
                                                              periods=minutes_to_insert_end, freq='1min'); 
        elif FS == '1.000' :
          df_ts_records_time_temp_end['TIME'] = pd.date_range(all_records_end_time + datetime.timedelta(seconds=1), 
                                                              periods=(duration_in_s-1), freq='S'); 

        df_ts_records = df_ts_records.append(df_ts_records_time_temp_end, ignore_index=True);
        #print('appended to df_ts_records')
      
      #df_ts_records['RECORD'] = record
      df_csvdata.loc[idx,'gap_SO_ShockOnsetORSOplus31'] = gap_SO_SOplus31;
      df_csvdata['Sepsis_SepsisOnset+ShockOnsetOR31h_overlap_duration']= overlap_duration_SO_SOplus31;
      df_csvdata['Sepsis_SepsisOnset+ShockOnsetOR31h_Number_of_overlaping_records'] = count_overlaping_records_SO_SOplus31;

      df_ts_records['SUBJECT_ID'] = row['subject_id']
      df_ts_records['ICUSTAY_ID'] = row['icustay_id']
      
      total_rows_sepsis_sepsisPlus31h = df_ts_records[(df_ts_records['TIME'] >= sepsis_onsettime) & (df_ts_records['TIME'] <= sepsis_onsettime_plus_31h_or_ShockOnsettime)].shape[0]

      total_rows_sepsis_sepsisPlus31h_notNAN = df_ts_records[(df_ts_records['TIME'] >=  sepsis_onsettime) & (df_ts_records['TIME'] <= sepsis_onsettime_plus_31h_or_ShockOnsettime)].dropna().shape[0]
      
      percent_notNANdata_sepsis_sepsisPlus31h = round(total_rows_sepsis_sepsisPlus31h_notNAN / total_rows_sepsis_sepsisPlus31h , 2) * 100 
      df_csvdata.loc[idx,'Sepsis_SepsisOnset+ShockOnsetOR31h_percentNonMissingData'] = percent_notNANdata_sepsis_sepsisPlus31h;


      #print(df_ts_records)

      #df_ts_records_all_patients = df_ts_records_all_patients.append(df_ts_records, ignore_index=True); # to insert into main df collecting TS of all patients
      #print('successfully inserted: ',row['subject_id'])
      
      
    except:
      print('hey nothing found ')
      #print('Error occured while reading waveform for patient: ', row['subject_id'])

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

df_csvdata['Sepsis_SepsisOnset+ShockOnsetOR31h_percentNonMissingData']=df_csvdata['Sepsis_SepsisOnset+ShockOnsetOR31h_percentNonMissingData'].apply(pd.to_numeric)

df_Sepsis_SepsisOnsetPlus31h_overlap_exists = df_csvdata[df_csvdata['Sepsis_SepsisOnset+ShockOnsetOR31h_timeoverlap_exists']==1]

print('Number of sepsis patients with overlap sepsis onset and Shock onset or sepsis onset + 31 hours : ', df_Sepsis_SepsisOnsetPlus31h_overlap_exists.shape[0])

print('Number of sepsis patients with overlap sepsis onset and Shock onset or sepsis onset + 31 hours and less than 20% missing data: ', df_Sepsis_SepsisOnsetPlus31h_overlap_exists[df_Sepsis_SepsisOnsetPlus31h_overlap_exists['Sepsis_SepsisOnset+ShockOnsetOR31h_percentNonMissingData'] >= 80].shape[0])

#to genrate a file for checking only sepsis onset time reference and NOT fluid resuciation time
df_csvdata.to_csv ('Only_AllSepsisPatients_with_MissingData_FromSepsisOnset_ToShockOnset_or_SepsisOnset+31h.csv', sep=',', index = False, header=True);


from google.colab import files
files.download('Only_AllSepsisPatients_with_MissingData_FromSepsisOnset_ToShockOnset_or_SepsisOnset+31h.csv')
