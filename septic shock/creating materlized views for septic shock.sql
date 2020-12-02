------creating materlized views------

create  materialized view  mv_septic_shock_weight as
 select ch.icustay_id,
        ch.itemid,
        ch.valuenum,
        ch.charttime,
        ch.value,
        ch.valueuom
 FROM sepsis3_cohort coh, chartevents ch
  WHERE ch.icustay_id = coh.icustay_id
  and coh.suspected_of_infection_poe = 1
  and coh.excluded=0
  and coh.waveform_exists = 1
  and coh.sepsis_onsettime is not null
  and ch.error IS DISTINCT FROM 1
  and ch.itemid in (762, 763, 3723, 3580,                     -- Weight Kg
      3581, 226531 ,                                   -- Weight lb
      3582,                                     -- Weight oz
      226512, 224639 -- Metavision: Admission Weight (Kg)
);


--inputevents_mv
create  materialized view  mv_septic_shock_fluids as
 select mv.icustay_id,
        mv.itemid,
        mv.amount ,
        mv.amountuom,
        mv.storetime,
        mv.starttime,
        mv.ordercategoryname,
        mv.secondaryordercategoryname
 FROM sepsis3_cohort coh, inputevents_mv mv
  WHERE mv.icustay_id = coh.icustay_id
  and coh.suspected_of_infection_poe = 1
  and coh.excluded=0
  and coh.waveform_exists = 1
  and coh.sepsis_onsettime is not null
  and mv.amountuom = 'ml'
  and ( lower(mv.ordercategoryname) like '%crystalloid%' or lower(mv.secondaryordercategoryname) like '%crystalloid%');
-----

create materialized view  mv_septic_shock_urineout as
  select op.charttime,
         op.storetime,
         op.itemid,
         op.icustay_id,
         op.value,
         op.valueuom
  FROM sepsis3_cohort coh, outputevents op
  WHERE op.icustay_id = coh.icustay_id
  and coh.suspected_of_infection_poe = 1
  and coh.excluded=0
  and coh.waveform_exists = 1
  and coh.sepsis_onsettime is not null
  and op.itemid in (
        -- these are the most frequently occurring urine output observations in CareVue
        40055, -- "Urine Out Foley"
        43175, -- "Urine ."
        40069, -- "Urine Out Void"
        40094, -- "Urine Out Condom Cath"
        40715, -- "Urine Out Suprapubic"
        40473, -- "Urine Out IleoConduit"
        40085, -- "Urine Out Incontinent"
        40057, -- "Urine Out Rt Nephrostomy"
        40056, -- "Urine Out Lt Nephrostomy"
        40405, -- "Urine Out Other"
        40428, -- "Urine Out Straight Cath"
        40086,--	Urine Out Incontinent
        40096, -- "Urine Out Ureteral Stent #1"
        40651, -- "Urine Out Ureteral Stent #2"

        -- these are the most frequently occurring urine output observations in Metavision
        226559, -- "Foley"
        226560, -- "Void"
        227510, -- "TF Residual"
        226561, -- "Condom Cath"
        226584, -- "Ileoconduit"
        226563, -- "Suprapubic"
        226564, -- "R Nephrostomy"
        226565, -- "L Nephrostomy"
        226567, --	Straight Cath
        226557, -- "R Ureteral Stent"
        226558  -- "L Ureteral Stent"
        );

-------CVP

create  materialized view  mv_septic_shock_cvp as
 select ch.icustay_id,
        ch.itemid,
        ch.valuenum,
        ch.charttime,
        ch.value,
        ch.valueuom
 FROM sepsis3_cohort coh, chartevents ch
  WHERE ch.icustay_id = coh.icustay_id
  and coh.suspected_of_infection_poe = 1
  and coh.excluded=0
  and coh.waveform_exists = 1
  and coh.sepsis_onsettime is not null
  and ch.error IS DISTINCT FROM 1
  and ch.itemid in (716, 1103, 113, 220074) ;

-----map
create  materialized view  mv_septic_shock_map as
 select ch.icustay_id,
        ch.itemid,
        ch.valuenum,
        ch.charttime,
        ch.value,
        ch.valueuom
 FROM sepsis3_cohort coh, chartevents ch
  WHERE ch.icustay_id = coh.icustay_id
  and coh.suspected_of_infection_poe = 1
  and coh.excluded=0
  and coh.waveform_exists = 1
  and coh.sepsis_onsettime is not null
  and ch.error IS DISTINCT FROM 1
  and ch.itemid in(456,52,6702,443,220052,220181,225312);

---- lactic ( 818, 1531, 225668 )
create  materialized view  mv_septic_shock_lactate as
 select ch.icustay_id,
        ch.itemid,
        ch.valuenum,
        ch.charttime,
        ch.value,
        ch.valueuom
 FROM sepsis3_cohort coh, chartevents ch
  WHERE ch.icustay_id = coh.icustay_id
  and coh.suspected_of_infection_poe = 1
  and coh.excluded=0
  and coh.waveform_exists = 1
  and coh.sepsis_onsettime is not null
  and ch.error IS DISTINCT FROM 1
  and ch.itemid in ( 818, 1531, 225668 );
