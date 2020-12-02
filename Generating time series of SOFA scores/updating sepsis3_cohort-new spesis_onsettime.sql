ALTER TABLE sepsis3_cohort ADD COLUMN waveform_exists int4;
Alter table  sepsis3_cohort ADD COLUMN has_sepsis int4;
Alter table  sepsis3_cohort ADD COLUMN sepsis_onsettime timestamp(0);
Alter table  sepsis3_cohort ADD COLUMN subject_id int4;


update sepsis3_cohort coh
set subject_id = (select icu.subject_id from icustays icu
  where icu.icustay_id=coh.icustay_id);
  

update sepsis3_cohort coh
set sepsis_onsettime = (select son.sepsis_onset_time from sepsis3_onsettime_new son
  where son.icustay_id=coh.icustay_id);
  
  
update sepsis3_cohort
set has_sepsis =0  ;
  
update sepsis3_cohort
set has_sepsis =1
where sepsis_onsettime is not null;