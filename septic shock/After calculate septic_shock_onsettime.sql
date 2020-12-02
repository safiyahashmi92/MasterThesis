
 Alter table  sepsis3_cohort ADD COLUMN sepstic_shock_onsettime timestamp(0);

  update sepsis3_cohort coh
set sepstic_shock_onsettime = (select son.septic_shock_onsettime from sepsis3_shock_onsettime son
  where son.icustay_id=coh.icustay_id);
  
  