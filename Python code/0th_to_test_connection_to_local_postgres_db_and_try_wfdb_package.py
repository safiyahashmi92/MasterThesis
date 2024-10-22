# -*- coding: utf-8 -*-
"""0th_To test connection to local Postgres Db and try WFDB package.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1CCs95wCAZHQPZfIUA4B3MAjWQ36RlfYQ
"""

import psycopg2
import pandas as pd
conn = psycopg2.connect(database="mimic",user="postgres",host="192.168.2.104",password="postgres",port="5432")
cur = conn.cursor()
cur.execute("SET search_path TO " + "mimiciii")

!python --version python

query_me = """
select hadm_id
    , chartdate, charttime
    , spec_type_desc
    -- if organism is present, then this is a positive culture, otherwise it's negative
    , max(case when org_name is not null and org_name != '' then 1 else 0 end) as PositiveCulture
from microbiologyevents
group by hadm_id, chartdate, charttime, spec_type_desc
"""

query = "select * from information_schema.tables where table_name = 'abx_poe_list'"
abx_poe = pd.read_sql_query(query,conn)
abx_poe.head()

query_abx_poe = """
  select pr.hadm_id
  , pr.drug as antibiotic_name
  , pr.startdate as antibiotic_time
  , pr.enddate as antibiotic_endtime
  from prescriptions pr
  -- inner join to subselect to only antibiotic prescriptions
  inner join abx_poe_list ab
      on pr.drug = ab.drug
"""

query = """
CREATE TABLE abx_micro_poe as
with abx as
(
""" + query_abx_poe + """
)
-- get cultures for each icustay
-- note this duplicates prescriptions
-- each ICU stay in the same hospitalization will get a copy of all prescriptions for that hospitalization
, ab_tbl as
(
  select
        ie.subject_id, ie.hadm_id, ie.icustay_id
      , ie.intime, ie.outtime
      , abx.antibiotic_name
      , abx.antibiotic_time
      , abx.antibiotic_endtime
  from icustays ie
  left join abx
      on ie.hadm_id = abx.hadm_id
)
, me as
(
""" + query_me + """
)
, ab_fnl as
(
  select
      ab_tbl.icustay_id, ab_tbl.intime, ab_tbl.outtime
    , ab_tbl.antibiotic_name
    , ab_tbl.antibiotic_time
    , coalesce(me72.charttime,me72.chartdate) as last72_charttime
    , coalesce(me24.charttime,me24.chartdate) as next24_charttime
    , me72.positiveculture as last72_positiveculture
    , me72.spec_type_desc as last72_specimen
    , me24.positiveculture as next24_positiveculture
    , me24.spec_type_desc as next24_specimen
  from ab_tbl
  -- blood culture in last 72 hours
  left join me me72
    on ab_tbl.hadm_id = me72.hadm_id
    and ab_tbl.antibiotic_time is not null
    and
    (
      -- if charttime is available, use it
      (
          ab_tbl.antibiotic_time > me72.charttime
      and ab_tbl.antibiotic_time <= me72.charttime + interval '72' hour
      )
      OR
      (
      -- if charttime is not available, use chartdate
          me72.charttime is null
      and ab_tbl.antibiotic_time > me72.chartdate
      and ab_tbl.antibiotic_time < me72.chartdate + interval '96' hour -- could equally do this with a date_trunc, but that's less portable
      )
    )
  -- blood culture in subsequent 24 hours
  left join me me24
    on ab_tbl.hadm_id = me24.hadm_id
    and ab_tbl.antibiotic_time is not null
    and me24.charttime is not null
    and
    (
      -- if charttime is available, use it
      (
          ab_tbl.antibiotic_time > me24.charttime - interval '24' hour
      and ab_tbl.antibiotic_time <= me24.charttime
      )
      OR
      (
      -- if charttime is not available, use chartdate
          me24.charttime is null
      and ab_tbl.antibiotic_time > me24.chartdate
      and ab_tbl.antibiotic_time <= me24.chartdate + interval '24' hour
      )
    )
)
, ab_laststg as
(
select
  icustay_id
  , antibiotic_name
  , antibiotic_time
  , last72_charttime
  , next24_charttime

  -- time of suspected infection: either the culture time (if before antibiotic), or the antibiotic time
  , case
      when coalesce(last72_charttime,next24_charttime) is null
        then 0
      else 1 end as suspected_infection

  , coalesce(last72_charttime,next24_charttime) as suspected_infection_time

  -- the specimen that was cultured
  , case
      when last72_charttime is not null
        then last72_specimen
      when next24_charttime is not null
        then next24_specimen
    else null
  end as specimen

  -- whether the cultured specimen ended up being positive or not
  , case
      when last72_charttime is not null
        then last72_positiveculture
      when next24_charttime is not null
        then next24_positiveculture
    else null
  end as positiveculture
  
  -- used to identify the *first* occurrence of suspected infection
  , ROW_NUMBER() over 
  (
     PARTITION BY ab_fnl.icustay_id
     ORDER BY coalesce(last72_charttime, next24_charttime)
  )
      as rn
from ab_fnl
)
select
  icustay_id
  , antibiotic_name
  , antibiotic_time
  , last72_charttime
  , next24_charttime
  , suspected_infection_time
  , specimen, positiveculture
from ab_laststg
where rn=1
order by icustay_id, antibiotic_time;
"""

cur = conn.cursor()
cur.execute(query)
cur.execute('COMMIT;')
cur.close()

print('abx_micro_poe table created!')

# Commented out IPython magic to ensure Python compatibility.
!pip install wfdb
from IPython.display import display
import matplotlib.pyplot as plt
# %matplotlib inline
import numpy as np
import os
import shutil
import posixpath
import wfdb

# Demo 7 - Read the multi-segment record and plot waveforms from the MIMIC matched waveform database.
# Notice that some channels have no valid values to plot
#record = wfdb.rdsamp('mimic3wdb/31/3141595/3141595n')
#wfdb.plot_wfdb(record, title='Record p000878/3269321_0001') 
#display(record.__dict__)

# Can also read the same files hosted on Physionet
record2 = wfdb.rdrecord('3141595n', pb_dir='mimic3wdb/31/3141595/')
wfdb.plot_wfdb(record2, title='test') 
display(record2.__dict__)

signals, fields = wfdb.rdsamp('3141595n', pb_dir='mimic3wdb/31/3141595/',  sampfrom=1, sampto=20)
display(signals)
display(fields)

import urllib

import urllib.request

with urllib.request.urlopen("https://archive.physionet.org/physiobank/database/mimic3wdb/matched/RECORDS") as url:
    s = url.read()
    # I'm guessing this would output the html source code ?
    print(s)
    

data =  urllib.request.urlopen("https://archive.physionet.org/physiobank/database/mimic3wdb/matched/RECORDS");    
for lines in data.readlines():
  print (lines) ;
  print ( lines[5:11]);