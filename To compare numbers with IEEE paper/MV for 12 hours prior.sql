
create materialized view spesis_onsettime_temp as
  select son.icustay_id, count(*) as temp_count
  from sepsis3_onsettime son, chartevents ch
  where son.icustay_id =  ch.icustay_id
  and ch.charttime >= son.sepsis_onset_time - interval '12' hour
  and ch. error is distinct from 1
  and ch.itemid in ( select d.itemid from d_items d where lower(d.label) like '%temperature%' and CATEGORY='Routine Vital Signs')
group by son.icustay_id
having count(*) > 0;

create materialized view spesis_onsettime_gcs as
  select son.icustay_id, count(*) as temp_count
  from sepsis3_onsettime son, chartevents ch
  where son.icustay_id =  ch.icustay_id
  and ch.charttime >= son.sepsis_onset_time - interval '12' hour
  and ch. error is distinct from 1
  and ch.itemid in ( select d.itemid from d_items d where lower(d.label) like '%gcs%' AND category ='Neurological')
group by son.icustay_id
having count(*) > 0;

create  materialized view if not exists spesis_onsettime_resp as
  select son.icustay_id, count(*) as temp_count
  from sepsis3_onsettime son, chartevents ch
  where son.icustay_id =  ch.icustay_id
  and ch.charttime >= son.sepsis_onset_time - interval '12' hour
  and ch. error is distinct from 1
  and ch.itemid in ( select d.itemid from d_items d where lower(d.label) like '%respiration%' and CATEGORY='Routine Vital Signs')
group by son.icustay_id
having count(*) > 0;


create  materialized view if not exists spesis_onsettime_hr as
  select son.icustay_id, count(*) as temp_count
  from sepsis3_onsettime son, chartevents ch
  where son.icustay_id =  ch.icustay_id
  and ch.charttime >= son.sepsis_onset_time - interval '12' hour
  and ch. error is distinct from 1
  and ch.itemid in ( select d.itemid from d_items d where lower(d.label) like '%heart%rate%' and CATEGORY='Routine Vital Signs')
group by son.icustay_id
having count(*) > 0;



create  materialized view if not exists spesis_onsettime_oxysat as
  select son.icustay_id, count(*) as temp_count
  from sepsis3_onsettime son, chartevents ch
  where son.icustay_id =  ch.icustay_id
  and ch.charttime >= son.sepsis_onset_time - interval '12' hour
  and ch. error is distinct from 1
  and ch.itemid in ( select d.itemid from d_items d where lower(d.label) like  '%oxygen%saturation%' and CATEGORY='Routine Vital Signs')
group by son.icustay_id
having count(*) > 0;


create  materialized view if not exists spesis_onsettime_bp as
  select son.icustay_id, count(*) as temp_count
  from sepsis3_onsettime son, chartevents ch
  where son.icustay_id =  ch.icustay_id
  and ch.charttime >= son.sepsis_onset_time - interval '12' hour
  and ch. error is distinct from 1
  and ch.itemid in ( select d.itemid from d_items d where lower(d.label) like '%blood%pressure%' and CATEGORY='Routine Vital Signs')
group by son.icustay_id
having count(*) > 0;
