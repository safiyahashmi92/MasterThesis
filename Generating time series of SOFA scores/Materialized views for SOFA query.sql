create materialized view  gcs_part1 as
  SELECT pvt.ICUSTAY_ID
  , pvt.charttime
  -- Easier names - note we coalesced Metavision and CareVue IDs below
  , max(case when pvt.itemid = 454 then pvt.valuenum else null end) as GCSMotor
  , max(case when pvt.itemid = 723 then pvt.valuenum else null end) as GCSVerbal
  , max(case when pvt.itemid = 184 then pvt.valuenum else null end) as GCSEyes
  -- If verbal was set to 0 in the below select, then this is an intubated patient
  , case
      when max(case when pvt.itemid = 723 then pvt.valuenum else null end) = 0
    then 1
    else 0
    end as EndoTrachFlag
  , ROW_NUMBER ()
          OVER (PARTITION BY pvt.ICUSTAY_ID ORDER BY pvt.charttime ASC) as rn
  FROM  (
  select l.ICUSTAY_ID
  -- merge the ITEMIDs so that the pivot applies to both metavision/carevue data
  , case
      when l.ITEMID in (723,223900) then 723
      when l.ITEMID in (454,223901) then 454
      when l.ITEMID in (184,220739) then 184
      else l.ITEMID end
    as ITEMID
  -- convert the data into a number, reserving a value of 0 for ET/Trach
  , case
      -- endotrach/vent is assigned a value of 0, later parsed specially
      when l.ITEMID = 723 and l.VALUE = '1.0 ET/Trach' then 0 -- carevue
      when l.ITEMID = 223900 and l.VALUE = 'No Response-ETT' then 0 -- metavision
      else VALUENUM
      end
    as VALUENUM
  , l.CHARTTIME
  from CHARTEVENTS l
  -- get intime for charttime subselection
  inner join icustays b
    on l.icustay_id = b.icustay_id
  -- Isolate the desired GCS variables
  where l.ITEMID in
  (
    -- 198 -- GCS
    -- GCS components, CareVue
    184, 454, 723
    -- GCS components, Metavision
    , 223900, 223901, 220739
  )
  -- Only get data for the first 24 hours
  --and l.charttime between '2186-01-26 09:30:00' ::timestamp and '2186-01-26 09:30:00' ::timestamp + interval '1h'
  -- exclude rows marked as error
  and l.error IS DISTINCT FROM 1
  ) pvt
  group by pvt.ICUSTAY_ID, pvt.charttime;
  
  ------------------------
create  materialized view sofa_query_weight as

SELECT ie.icustay_id ,
       c.itemid,
       c.valuenum,
       c.charttime

       from icustays ie --todo

       inner join sepsis3_cohort coh
       on coh.hadm_id = ie.hadm_id
       and coh.icustay_id = ie.icustay_id
        and coh.excluded=0

        left join chartevents c
        on ie.icustay_id = c.icustay_id
       WHERE c.valuenum IS NOT NULL
        AND c.itemid IN
        (
              762, 763, 3723, 3580,                     -- Weight Kg
              3581,                                     -- Weight lb
              3582,                                     -- Weight oz
              226512 -- Metavision: Admission Weight (Kg)
          )
          AND c.valuenum != 0
          AND c.error IS DISTINCT FROM 1;

-----------------
drop materialized view  sofa_query_vaso_cv;
create materialized view  sofa_query_vaso_cv as

select ie.icustay_id, itemid, rate,cv.charttime

from inputevents_cv cv
inner join icustays ie
on ie.icustay_id = cv.icustay_id

inner join sepsis3_cohort coh
on coh.hadm_id = ie.hadm_id
and coh.icustay_id = ie.icustay_id
and coh.excluded=0

and cv.itemid in (30047,30120,30044,30119,30309,30043,30307,30042,30306)
and rate is not null;

--------------------------

drop  materialized view  sofa_query_vaso_mv

create materialized view  sofa_query_vaso_mv as

select ie.icustay_id, itemid, rate,mv.starttime

from inputevents_mv mv
inner join icustays ie
on ie.icustay_id = mv.icustay_id

inner join sepsis3_cohort coh
on coh.hadm_id = ie.hadm_id
and coh.icustay_id = ie.icustay_id
and coh.excluded=0

and mv.itemid in  (221906,221289,221662,221653)
and mv.statusdescription != 'Rewritten';
