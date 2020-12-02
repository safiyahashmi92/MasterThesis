CREATE OR REPLACE FUNCTION sp_calc_hourly_sofa_comparing()
 RETURNS VOID AS
 $BODY$
 DECLARE
  cur_cohort CURSOR FOR
  SELECT coh.icustay_id,coh.hadm_id,coh.suspected_infection_time_poe, coh.intime,coh.outtime, coh.subject_id
  FROM sepsis3_cohort coh
  WHERE coh.suspected_of_infection_poe = 1
  and  coh.excluded=0
  and coh.waveform_exists =1
  and coh.icustay_id=200191
  order by icustay_id;



  cv_icustay_id                       integer;
  cv_hadm_id                          sepsis3_cohort.hadm_id%TYPE;
  cv_suspected_infection_time         sepsis3_cohort.suspected_infection_time_poe%TYPE;
  init_window_hr                      sepsis3_cohort.suspected_infection_time_poe%TYPE;
  final_window_hr                     sepsis3_cohort.suspected_infection_time_poe%TYPE;
  starting_hour                       sepsis3_cohort.suspected_infection_time_poe%TYPE;
  cv_intime                           sepsis3_cohort.intime%TYPE;
  cv_outtime                          sepsis3_cohort.intime%TYPE;
  tmp                                sepsis3_cohort.intime%TYPE;
  current_hour sepsis3_cohort.suspected_infection_time_poe%TYPE;
  next_hour sepsis3_cohort.suspected_infection_time_poe%TYPE;
  window_gap integer;
  cohort_count integer := 0;
  v_sepsis_onset_count integer :=0;
  rec record;
  number_hours integer;
   --ignore_prev_sofa boolean := flase;
  prev_sofa integer;
  v_sofa_score INTEGER := 0;
  v_respiration INTEGER := 0;
  v_coagulation INTEGER := 0;
  v_liver INTEGER := 0;
  v_cardiovascular INTEGER := 0;
  v_cns INTEGER := 0;
  v_renal INTEGER := 0;
  v_initial_sofa_Score INTEGER := 0;
  v_Sepsis_onset_time sepsis3_cohort.suspected_infection_time_poe%TYPE;
  cv_subject_id sepsis3_cohort.subject_id%TYPE;
BEGIN


  open cur_cohort;

  LOOP
    -- fetch row into the film
     fetch next from cur_cohort
      into cv_icustay_id, cv_hadm_id, cv_suspected_infection_time,cv_intime,cv_outtime,cv_subject_id;

      EXIT WHEN NOT FOUND;

      v_sofa_score := 0;
      v_respiration := 0;
      v_coagulation := 0;
      v_liver := 0;
      v_cardiovascular := 0;
      v_cns := 0;
      v_renal := 0;
      v_initial_sofa_Score := 0;


      init_window_hr = (cv_suspected_infection_time - interval '48' hour );
      final_window_hr = ( cv_suspected_infection_time + interval '24' hour );
      /*
      if init_window_hr < cv_intime then
        init_window_hr = cv_intime;
      end if;

      raise notice 'outtime: %', cv_outtime;
      if final_window_hr > cv_outtime then
        final_window_hr = cv_outtime;
      end if;
      */
      init_window_hr = '2160-11-20 19:50:00' ::timestamp; -- 2160-11-25 18:50:00




      number_hours = round(CAST(EXTRACT(EPOCH FROM (final_window_hr - init_window_hr ))/3600 as numeric));


      raise notice 'ICUSTAY ID   :%',cv_icustay_id;
      raise notice 'time of infection suspection  :%',cv_suspected_infection_time;
      raise notice 'intial window time  :%',init_window_hr;
      raise notice 'final window time  :%',final_window_hr;
      raise notice 'number of hours between initial and final window  :%',number_hours;

      --starting_hour = init_window_hr;
      current_hour = init_window_hr;

      FOR rec IN 1..number_hours BY 1 LOOP
      /*if rec = 1 then
        raise notice 'starting hour   :%',(cv_intime);
        raise notice 'next hour   :%',(starting_hour);
        if starting_hour < cv_intime then
          ignore_prev_sofa := true;
          /* sofa score = 0 */
        end if;

        current_hour = cv_intime;
        next_hour = starting_hour;

        raise notice 'starting hour   :%',(current_hour);
        raise notice 'next hour   :%',(next_hour);

        ELSE

        raise notice 'INTERVALS STARTING';

        if rec = 2 then
          current_hour = init_window_hr;
        end if;
        */
        next_hour = current_hour + interval '1' hour;

        raise notice 'starting hour   :%',current_hour ;
        raise notice 'next hour   :%',next_hour;

      --end if;

       /*  calculating sofa score between current hour and next hour */
      -------
        select SOFA
          , respiration
          , coagulation
          , liver
          , cardiovascular
          , cns
          , renal
        into
            v_sofa_score,
            v_respiration,
            v_coagulation,
            v_liver,
            v_cardiovascular,
            v_cns,
            v_renal
        from (
               with vitals as (
                 SELECT pvt.subject_id
                      , pvt.hadm_id
                      , pvt.icustay_id

                      -- Easier names
                      , min(case when VitalID = 1 then valuenum else null end) as HeartRate_Min
                      , max(case when VitalID = 1 then valuenum else null end) as HeartRate_Max
                      , avg(case when VitalID = 1 then valuenum else null end) as HeartRate_Mean
                      , min(case when VitalID = 2 then valuenum else null end) as SysBP_Min
                      , max(case when VitalID = 2 then valuenum else null end) as SysBP_Max
                      , avg(case when VitalID = 2 then valuenum else null end) as SysBP_Mean
                      , min(case when VitalID = 3 then valuenum else null end) as DiasBP_Min
                      , max(case when VitalID = 3 then valuenum else null end) as DiasBP_Max
                      , avg(case when VitalID = 3 then valuenum else null end) as DiasBP_Mean
                      , min(case when VitalID = 4 then valuenum else null end) as MeanBP_Min
                      , max(case when VitalID = 4 then valuenum else null end) as MeanBP_Max
                      , avg(case when VitalID = 4 then valuenum else null end) as MeanBP_Mean
                      , min(case when VitalID = 5 then valuenum else null end) as RespRate_Min
                      , max(case when VitalID = 5 then valuenum else null end) as RespRate_Max
                      , avg(case when VitalID = 5 then valuenum else null end) as RespRate_Mean
                      , min(case when VitalID = 6 then valuenum else null end) as TempC_Min
                      , max(case when VitalID = 6 then valuenum else null end) as TempC_Max
                      , avg(case when VitalID = 6 then valuenum else null end) as TempC_Mean
                      , min(case when VitalID = 7 then valuenum else null end) as SpO2_Min
                      , max(case when VitalID = 7 then valuenum else null end) as SpO2_Max
                      , avg(case when VitalID = 7 then valuenum else null end) as SpO2_Mean
                      , min(case when VitalID = 8 then valuenum else null end) as Glucose_Min
                      , max(case when VitalID = 8 then valuenum else null end) as Glucose_Max
                      , avg(case when VitalID = 8 then valuenum else null end) as Glucose_Mean

                 FROM (
                        select ie.subject_id
                             , ie.hadm_id
                             , ie.icustay_id
                             , case
                                 when itemid in (211, 220045) and valuenum > 0 and valuenum < 300 then 1 -- HeartRate
                                 when itemid in (51, 442, 455, 6701, 220179, 220050) and valuenum > 0 and valuenum < 400
                                   then 2 -- SysBP
                                 when itemid in (8368, 8440, 8441, 8555, 220180, 220051) and valuenum > 0 and
                                      valuenum < 300 then 3 -- DiasBP
                                 when itemid in (456, 52, 6702, 443, 220052, 220181, 225312) and valuenum > 0 and
                                      valuenum < 300 then 4 -- MeanBP
                                 when itemid in (615, 618, 220210, 224690) and valuenum > 0 and valuenum < 70
                                   then 5 -- RespRate
                                 when itemid in (223761, 678) and valuenum > 70 and valuenum < 120
                                   then 6 -- TempF, converted to degC in valuenum call
                                 when itemid in (223762, 676) and valuenum > 10 and valuenum < 50 then 6 -- TempC
                                 when itemid in (646, 220277) and valuenum > 0 and valuenum <= 100 then 7 -- SpO2
                                 when itemid in (807, 811, 1529, 3745, 3744, 225664, 220621, 226537) and valuenum > 0
                                   then 8 -- Glucose

                                 else null end                                                                as VitalID
                             -- convert F to C
                             , case
                                 when itemid in (223761, 678) then (valuenum - 32) / 1.8
                                 else valuenum end                                                            as valuenum

                        from icustays ie
                               left join chartevents ce
                                         on ie.subject_id = ce.subject_id and ie.hadm_id = ce.hadm_id and
                                            ie.icustay_id = ce.icustay_id
                                           and ce.charttime between current_hour and next_hour
                                           -- exclude rows marked as error
                                           and ce.error IS DISTINCT FROM 1
                        where ce.itemid in
                              (
                                -- HEART RATE
                               211, --"Heart Rate"
                               220045, --"Heart Rate"

                                -- Systolic/diastolic

                               51, --	Arterial BP [Systolic]
                               442, --	Manual BP [Systolic]
                               455, --	NBP [Systolic]
                               6701, --	Arterial BP #2 [Systolic]
                               220179, --	Non Invasive Blood Pressure systolic
                               220050, --	Arterial Blood Pressure systolic

                               8368, --	Arterial BP [Diastolic]
                               8440, --	Manual BP [Diastolic]
                               8441, --	NBP [Diastolic]
                               8555, --	Arterial BP #2 [Diastolic]
                               220180, --	Non Invasive Blood Pressure diastolic
                               220051, --	Arterial Blood Pressure diastolic


                                -- MEAN ARTERIAL PRESSURE
                               456, --"NBP Mean"
                               52, --"Arterial BP Mean"
                               6702, --	Arterial BP Mean #2
                               443, --	Manual BP Mean(calc)
                               220052, --"Arterial Blood Pressure mean"
                               220181, --"Non Invasive Blood Pressure mean"
                               225312, --"ART BP mean"

                                -- RESPIRATORY RATE
                               618,--	Respiratory Rate
                               615,--	Resp Rate (Total)
                               220210,--	Respiratory Rate
                               224690, --	Respiratory Rate (Total)


                                -- SPO2, peripheral
                               646, 220277,

                                -- GLUCOSE, both lab and fingerstick
                               807,--	Fingerstick Glucose
                               811,--	Glucose (70-105)
                               1529,--	Glucose
                               3745,--	BloodGlucose
                               3744,--	Blood Glucose
                               225664,--	Glucose finger stick
                               220621,--	Glucose (serum)
                               226537,--	Glucose (whole blood)

                                -- TEMPERATURE
                               223762, -- "Temperature Celsius"
                               676, -- "Temperature C"
                               223761, -- "Temperature Fahrenheit"
                               678 --	"Temperature F"

                                )
                      ) pvt
                 group by pvt.subject_id, pvt.hadm_id, pvt.icustay_id
                 order by pvt.subject_id, pvt.hadm_id, pvt.icustay_id)
                  ,
                 urine_output as
                   (select
                         -- patient identifiers
                      ie.subject_id
                         , ie.hadm_id
                         , ie.icustay_id

                         -- volumes associated with urine output ITEMIDs
                         , sum(
                       -- we consider input of GU irrigant as a negative volume
                         case
                           when oe.itemid = 227488 then -1 * VALUE
                           else VALUE end
                       ) as UrineOutput
                    from icustays ie
                           -- Join to the outputevents table to get urine output
                           left join outputevents oe
                      -- join on all patient identifiers
                                     on ie.subject_id = oe.subject_id and ie.hadm_id = oe.hadm_id and
                                        ie.icustay_id = oe.icustay_id
                                       -- and ensure the data occurs during the first day
                                       and oe.charttime between current_hour and next_hour
                    where itemid in
                          (
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

                            -- these are the most frequently occurring urine output observations in MetaVision
                           226559, -- "Foley"
                           226560, -- "Void"
                           226561, -- "Condom Cath"
                           226584, -- "Ileoconduit"
                           226563, -- "Suprapubic"
                           226564, -- "R Nephrostomy"
                           226565, -- "L Nephrostomy"
                           226567, --	Straight Cath
                           226557, -- R Ureteral Stent
                           226558, -- L Ureteral Stent
                           227488, -- GU Irrigant Volume In
                           227489 -- GU Irrigant/Urine Volume Out
                            )
                    group by ie.subject_id, ie.hadm_id, ie.icustay_id
                    order by ie.subject_id, ie.hadm_id, ie.icustay_id)
                  ,
                 labs as (
                   SELECT pvt.subject_id
                        , pvt.hadm_id
                        , pvt.icustay_id

                        , min(CASE WHEN label = 'ANION GAP' THEN valuenum ELSE null END)   as ANIONGAP_min
                        , max(CASE WHEN label = 'ANION GAP' THEN valuenum ELSE null END)   as ANIONGAP_max
                        , min(CASE WHEN label = 'ALBUMIN' THEN valuenum ELSE null END)     as ALBUMIN_min
                        , max(CASE WHEN label = 'ALBUMIN' THEN valuenum ELSE null END)     as ALBUMIN_max
                        , min(CASE WHEN label = 'BANDS' THEN valuenum ELSE null END)       as BANDS_min
                        , max(CASE WHEN label = 'BANDS' THEN valuenum ELSE null END)       as BANDS_max
                        , min(CASE WHEN label = 'BICARBONATE' THEN valuenum ELSE null END) as BICARBONATE_min
                        , max(CASE WHEN label = 'BICARBONATE' THEN valuenum ELSE null END) as BICARBONATE_max
                        , min(CASE WHEN label = 'BILIRUBIN' THEN valuenum ELSE null END)   as BILIRUBIN_min
                        , max(CASE WHEN label = 'BILIRUBIN' THEN valuenum ELSE null END)   as BILIRUBIN_max
                        , min(CASE WHEN label = 'CREATININE' THEN valuenum ELSE null END)  as CREATININE_min
                        , max(CASE WHEN label = 'CREATININE' THEN valuenum ELSE null END)  as CREATININE_max
                        , min(CASE WHEN label = 'CHLORIDE' THEN valuenum ELSE null END)    as CHLORIDE_min
                        , max(CASE WHEN label = 'CHLORIDE' THEN valuenum ELSE null END)    as CHLORIDE_max
                        , min(CASE WHEN label = 'GLUCOSE' THEN valuenum ELSE null END)     as GLUCOSE_min
                        , max(CASE WHEN label = 'GLUCOSE' THEN valuenum ELSE null END)     as GLUCOSE_max
                        , min(CASE WHEN label = 'HEMATOCRIT' THEN valuenum ELSE null END)  as HEMATOCRIT_min
                        , max(CASE WHEN label = 'HEMATOCRIT' THEN valuenum ELSE null END)  as HEMATOCRIT_max
                        , min(CASE WHEN label = 'HEMOGLOBIN' THEN valuenum ELSE null END)  as HEMOGLOBIN_min
                        , max(CASE WHEN label = 'HEMOGLOBIN' THEN valuenum ELSE null END)  as HEMOGLOBIN_max
                        , min(CASE WHEN label = 'LACTATE' THEN valuenum ELSE null END)     as LACTATE_min
                        , max(CASE WHEN label = 'LACTATE' THEN valuenum ELSE null END)     as LACTATE_max
                        , min(CASE WHEN label = 'PLATELET' THEN valuenum ELSE null END)    as PLATELET_min
                        , max(CASE WHEN label = 'PLATELET' THEN valuenum ELSE null END)    as PLATELET_max
                        , min(CASE WHEN label = 'POTASSIUM' THEN valuenum ELSE null END)   as POTASSIUM_min
                        , max(CASE WHEN label = 'POTASSIUM' THEN valuenum ELSE null END)   as POTASSIUM_max
                        , min(CASE WHEN label = 'PTT' THEN valuenum ELSE null END)         as PTT_min
                        , max(CASE WHEN label = 'PTT' THEN valuenum ELSE null END)         as PTT_max
                        , min(CASE WHEN label = 'INR' THEN valuenum ELSE null END)         as INR_min
                        , max(CASE WHEN label = 'INR' THEN valuenum ELSE null END)         as INR_max
                        , min(CASE WHEN label = 'PT' THEN valuenum ELSE null END)          as PT_min
                        , max(CASE WHEN label = 'PT' THEN valuenum ELSE null END)          as PT_max
                        , min(CASE WHEN label = 'SODIUM' THEN valuenum ELSE null END)      as SODIUM_min
                        , max(CASE WHEN label = 'SODIUM' THEN valuenum ELSE null end)      as SODIUM_max
                        , min(CASE WHEN label = 'BUN' THEN valuenum ELSE null end)         as BUN_min
                        , max(CASE WHEN label = 'BUN' THEN valuenum ELSE null end)         as BUN_max
                        , min(CASE WHEN label = 'WBC' THEN valuenum ELSE null end)         as WBC_min
                        , max(CASE WHEN label = 'WBC' THEN valuenum ELSE null end)         as WBC_max


                   FROM ( -- begin query that extracts the data
                          SELECT ie.subject_id
                               , ie.hadm_id
                               , ie.icustay_id
                               -- here we assign labels to ITEMIDs
                               -- this also fuses together multiple ITEMIDs containing the same data
                               , CASE
                                   WHEN itemid = 50868 THEN 'ANION GAP'
                                   WHEN itemid = 50862 THEN 'ALBUMIN'
                                   WHEN itemid = 51144 THEN 'BANDS'
                                   WHEN itemid = 50882 THEN 'BICARBONATE'
                                   WHEN itemid = 50885 THEN 'BILIRUBIN'
                                   WHEN itemid = 50912 THEN 'CREATININE'
                                   WHEN itemid = 50806 THEN 'CHLORIDE'
                                   WHEN itemid = 50902 THEN 'CHLORIDE'
                                   WHEN itemid = 50809 THEN 'GLUCOSE'
                                   WHEN itemid = 50931 THEN 'GLUCOSE'
                                   WHEN itemid = 50810 THEN 'HEMATOCRIT'
                                   WHEN itemid = 51221 THEN 'HEMATOCRIT'
                                   WHEN itemid = 50811 THEN 'HEMOGLOBIN'
                                   WHEN itemid = 51222 THEN 'HEMOGLOBIN'
                                   WHEN itemid = 50813 THEN 'LACTATE'
                                   WHEN itemid = 51265 THEN 'PLATELET'
                                   WHEN itemid = 50822 THEN 'POTASSIUM'
                                   WHEN itemid = 50971 THEN 'POTASSIUM'
                                   WHEN itemid = 51275 THEN 'PTT'
                                   WHEN itemid = 51237 THEN 'INR'
                                   WHEN itemid = 51274 THEN 'PT'
                                   WHEN itemid = 50824 THEN 'SODIUM'
                                   WHEN itemid = 50983 THEN 'SODIUM'
                                   WHEN itemid = 51006 THEN 'BUN'
                                   WHEN itemid = 51300 THEN 'WBC'
                                   WHEN itemid = 51301 THEN 'WBC'
                                   ELSE null
                            END   AS label
                               , -- add in some sanity checks on the values
                               -- the where clause below requires all valuenum to be > 0, so these are only upper limit checks
                            CASE
                              WHEN itemid = 50862 and valuenum > 10 THEN null -- g/dL 'ALBUMIN'
                              WHEN itemid = 50868 and valuenum > 10000 THEN null -- mEq/L 'ANION GAP'
                              WHEN itemid = 51144 and valuenum < 0 THEN null -- immature band forms, %
                              WHEN itemid = 51144 and valuenum > 100 THEN null -- immature band forms, %
                              WHEN itemid = 50882 and valuenum > 10000 THEN null -- mEq/L 'BICARBONATE'
                              WHEN itemid = 50885 and valuenum > 150 THEN null -- mg/dL 'BILIRUBIN'
                              WHEN itemid = 50806 and valuenum > 10000 THEN null -- mEq/L 'CHLORIDE'
                              WHEN itemid = 50902 and valuenum > 10000 THEN null -- mEq/L 'CHLORIDE'
                              WHEN itemid = 50912 and valuenum > 150 THEN null -- mg/dL 'CREATININE'
                              WHEN itemid = 50809 and valuenum > 10000 THEN null -- mg/dL 'GLUCOSE'
                              WHEN itemid = 50931 and valuenum > 10000 THEN null -- mg/dL 'GLUCOSE'
                              WHEN itemid = 50810 and valuenum > 100 THEN null -- % 'HEMATOCRIT'
                              WHEN itemid = 51221 and valuenum > 100 THEN null -- % 'HEMATOCRIT'
                              WHEN itemid = 50811 and valuenum > 50 THEN null -- g/dL 'HEMOGLOBIN'
                              WHEN itemid = 51222 and valuenum > 50 THEN null -- g/dL 'HEMOGLOBIN'
                              WHEN itemid = 50813 and valuenum > 50 THEN null -- mmol/L 'LACTATE'
                              WHEN itemid = 51265 and valuenum > 10000 THEN null -- K/uL 'PLATELET'
                              WHEN itemid = 50822 and valuenum > 30 THEN null -- mEq/L 'POTASSIUM'
                              WHEN itemid = 50971 and valuenum > 30 THEN null -- mEq/L 'POTASSIUM'
                              WHEN itemid = 51275 and valuenum > 150 THEN null -- sec 'PTT'
                              WHEN itemid = 51237 and valuenum > 50 THEN null -- 'INR'
                              WHEN itemid = 51274 and valuenum > 150 THEN null -- sec 'PT'
                              WHEN itemid = 50824 and valuenum > 200 THEN null -- mEq/L == mmol/L 'SODIUM'
                              WHEN itemid = 50983 and valuenum > 200 THEN null -- mEq/L == mmol/L 'SODIUM'
                              WHEN itemid = 51006 and valuenum > 300 THEN null -- 'BUN'
                              WHEN itemid = 51300 and valuenum > 1000 THEN null -- 'WBC'
                              WHEN itemid = 51301 and valuenum > 1000 THEN null -- 'WBC'
                              ELSE le.valuenum
                              END AS valuenum

                          FROM icustays ie

                                 LEFT JOIN labevents le
                                           ON le.subject_id = ie.subject_id AND le.hadm_id = ie.hadm_id
                                             AND le.charttime BETWEEN current_hour and next_hour
                                             AND le.ITEMID in
                                                 (
                                                   -- comment is: LABEL | CATEGORY | FLUID | NUMBER OF ROWS IN LABEVENTS
                                                  50868, -- ANION GAP | CHEMISTRY | BLOOD | 769895
                                                  50862, -- ALBUMIN | CHEMISTRY | BLOOD | 146697
                                                  51144, -- BANDS - hematology
                                                  50882, -- BICARBONATE | CHEMISTRY | BLOOD | 780733
                                                  50885, -- BILIRUBIN, TOTAL | CHEMISTRY | BLOOD | 238277
                                                  50912, -- CREATININE | CHEMISTRY | BLOOD | 797476
                                                  50902, -- CHLORIDE | CHEMISTRY | BLOOD | 795568
                                                  50806, -- CHLORIDE, WHOLE BLOOD | BLOOD GAS | BLOOD | 48187
                                                  50931, -- GLUCOSE | CHEMISTRY | BLOOD | 748981
                                                  50809, -- GLUCOSE | BLOOD GAS | BLOOD | 196734
                                                  51221, -- HEMATOCRIT | HEMATOLOGY | BLOOD | 881846
                                                  50810, -- HEMATOCRIT, CALCULATED | BLOOD GAS | BLOOD | 89715
                                                  51222, -- HEMOGLOBIN | HEMATOLOGY | BLOOD | 752523
                                                  50811, -- HEMOGLOBIN | BLOOD GAS | BLOOD | 89712
                                                  50813, -- LACTATE | BLOOD GAS | BLOOD | 187124
                                                  51265, -- PLATELET COUNT | HEMATOLOGY | BLOOD | 778444
                                                  50971, -- POTASSIUM | CHEMISTRY | BLOOD | 845825
                                                  50822, -- POTASSIUM, WHOLE BLOOD | BLOOD GAS | BLOOD | 192946
                                                  51275, -- PTT | HEMATOLOGY | BLOOD | 474937
                                                  51237, -- INR(PT) | HEMATOLOGY | BLOOD | 471183
                                                  51274, -- PT | HEMATOLOGY | BLOOD | 469090
                                                  50983, -- SODIUM | CHEMISTRY | BLOOD | 808489
                                                  50824, -- SODIUM, WHOLE BLOOD | BLOOD GAS | BLOOD | 71503
                                                  51006, -- UREA NITROGEN | CHEMISTRY | BLOOD | 791925
                                                  51301, -- WHITE BLOOD CELLS | HEMATOLOGY | BLOOD | 753301
                                                  51300 -- WBC COUNT | HEMATOLOGY | BLOOD | 2371
                                                   )
                                             AND valuenum IS NOT null AND
                                              valuenum > 0 -- lab values cannot be 0 and cannot be negative
                        ) pvt
                   GROUP BY pvt.subject_id, pvt.hadm_id, pvt.icustay_id
                   ORDER BY pvt.subject_id, pvt.hadm_id, pvt.icustay_id)
                  ,
                 base as
                   (
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
                       end                                                                  as EndoTrachFlag

                          , ROW_NUMBER()
                         OVER (PARTITION BY pvt.ICUSTAY_ID ORDER BY pvt.charttime ASC)      as rn

                     FROM (
                            select l.ICUSTAY_ID
                                 -- merge the ITEMIDs so that the pivot applies to both metavision/carevue data
                                 , case
                                     when l.ITEMID in (723, 223900) then 723
                                     when l.ITEMID in (454, 223901) then 454
                                     when l.ITEMID in (184, 220739) then 184
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
                              and l.charttime between current_hour and next_hour
                              -- exclude rows marked as error
                              and l.error IS DISTINCT FROM 1
                          ) pvt
                     group by pvt.ICUSTAY_ID, pvt.charttime
                   )
                  , gcs_part as (
                 select b.*
                      , b2.GCSVerbal as GCSVerbalPrev
                      , b2.GCSMotor  as GCSMotorPrev
                      , b2.GCSEyes   as GCSEyesPrev
                      -- Calculate GCS, factoring in special case when they are intubated and prev vals
                      -- note that the coalesce are used to implement the following if:
                      --  if current value exists, use it
                      --  if previous value exists, use it
                      --  otherwise, default to normal
                      , case
                   -- replace GCS during sedation with 15
                          when b.GCSVerbal = 0
                            then 15
                          when b.GCSVerbal is null and b2.GCSVerbal = 0
                            then 15
                   -- if previously they were intub, but they aren't now, do not use previous GCS values
                          when b2.GCSVerbal = 0
                            then
                              coalesce(b.GCSMotor, 6)
                              + coalesce(b.GCSVerbal, 5)
                              + coalesce(b.GCSEyes, 4)
                   -- otherwise, add up score normally, imputing previous value if none available at current time
                          else
                              coalesce(b.GCSMotor, coalesce(b2.GCSMotor, 6))
                              + coalesce(b.GCSVerbal, coalesce(b2.GCSVerbal, 5))
                              + coalesce(b.GCSEyes, coalesce(b2.GCSEyes, 4))
                   end               as GCS

                 from base b
                        -- join to itself within 6 hours to get previous value
                        left join base b2
                                  on b.ICUSTAY_ID = b2.ICUSTAY_ID and b.rn = b2.rn + 1 and
                                     b2.charttime > b.charttime - interval '6' hour
               )
                  , gcs_final as (
                 select gcs.*
                      -- This sorts the data by GCS, so rn=1 is the the lowest GCS values to keep
                      , ROW_NUMBER()
                     OVER (PARTITION BY gcs.ICUSTAY_ID
                       ORDER BY gcs.GCS
                       ) as IsMinGCS
                 from gcs_part gcs
               )
                  ,
                 gcs as (
                   select ie.SUBJECT_ID
                        , ie.HADM_ID
                        , ie.ICUSTAY_ID
                        -- The minimum GCS is determined by the above row partition, we only join if IsMinGCS=1
                        , GCS                                as MinGCS
                        , coalesce(GCSMotor, GCSMotorPrev)   as GCSMotor
                        , coalesce(GCSVerbal, GCSVerbalPrev) as GCSVerbal
                        , coalesce(GCSEyes, GCSEyesPrev)     as GCSEyes
                        , EndoTrachFlag                      as EndoTrachFlag

                        -- subselect down to the cohort of eligible patients
                   from icustays ie
                          left join gcs_final gs
                                    on ie.ICUSTAY_ID = gs.ICUSTAY_ID and gs.IsMinGCS = 1
                   ORDER BY ie.ICUSTAY_ID)
                  ,
                 pvt as
                   ( -- begin query that extracts the data
                     select ie.subject_id
                          , ie.hadm_id
                          , ie.icustay_id
                          -- here we assign labels to ITEMIDs
                          -- this also fuses together multiple ITEMIDs containing the same data
                          , case
                              when itemid = 50800 then 'SPECIMEN'
                              when itemid = 50801 then 'AADO2'
                              when itemid = 50802 then 'BASEEXCESS'
                              when itemid = 50803 then 'BICARBONATE'
                              when itemid = 50804 then 'TOTALCO2'
                              when itemid = 50805 then 'CARBOXYHEMOGLOBIN'
                              when itemid = 50806 then 'CHLORIDE'
                              when itemid = 50808 then 'CALCIUM'
                              when itemid = 50809 then 'GLUCOSE'
                              when itemid = 50810 then 'HEMATOCRIT'
                              when itemid = 50811 then 'HEMOGLOBIN'
                              when itemid = 50812 then 'INTUBATED'
                              when itemid = 50813 then 'LACTATE'
                              when itemid = 50814 then 'METHEMOGLOBIN'
                              when itemid = 50815 then 'O2FLOW'
                              when itemid = 50816 then 'FIO2'
                              when itemid = 50817 then 'SO2' -- OXYGENSATURATION
                              when itemid = 50818 then 'PCO2'
                              when itemid = 50819 then 'PEEP'
                              when itemid = 50820 then 'PH'
                              when itemid = 50821 then 'PO2'
                              when itemid = 50822 then 'POTASSIUM'
                              when itemid = 50823 then 'REQUIREDO2'
                              when itemid = 50824 then 'SODIUM'
                              when itemid = 50825 then 'TEMPERATURE'
                              when itemid = 50826 then 'TIDALVOLUME'
                              when itemid = 50827 then 'VENTILATIONRATE'
                              when itemid = 50828 then 'VENTILATOR'
                              else null
                       end as label
                          , charttime
                          , value
                          -- add in some sanity checks on the values
                          , case
                              when valuenum <= 0 then null
                              when itemid = 50810 and valuenum > 100 then null -- hematocrit
                       -- ensure FiO2 is a valid number between 21-100
                       -- mistakes are rare (<100 obs out of ~100,000)
                       -- there are 862 obs of valuenum == 20 - some people round down!
                       -- rather than risk imputing garbage data for FiO2, we simply NULL invalid values
                              when itemid = 50816 and valuenum < 20 then null
                              when itemid = 50816 and valuenum > 100 then null
                              when itemid = 50817 and valuenum > 100 then null -- O2 sat
                              when itemid = 50815 and valuenum > 70 then null -- O2 flow
                              when itemid = 50821 and valuenum > 800 then null -- PO2
                       -- conservative upper limit
                              else valuenum
                       end as valuenum

                     from icustays ie
                            left join labevents le
                                      on le.subject_id = ie.subject_id and le.hadm_id = ie.hadm_id
                                        and le.charttime between current_hour and next_hour
                                        and le.ITEMID in
                                           -- blood gases
                                            (
                                             50800, 50801, 50802, 50803, 50804, 50805, 50806, 50807, 50808, 50809,
                                             50810, 50811, 50812, 50813, 50814, 50815, 50816, 50817, 50818, 50819,
                                             50820, 50821, 50822, 50823, 50824, 50825, 50826, 50827, 50828, 51545
                                              )
                   )
                  ,
                 blood_gas as (
                   select pvt.SUBJECT_ID
                        , pvt.HADM_ID
                        , pvt.ICUSTAY_ID
                        , pvt.CHARTTIME
                        , max(case when label = 'SPECIMEN' then value else null end)             as SPECIMEN
                        , max(case when label = 'AADO2' then valuenum else null end)             as AADO2
                        , max(case when label = 'BASEEXCESS' then valuenum else null end)        as BASEEXCESS
                        , max(case when label = 'BICARBONATE' then valuenum else null end)       as BICARBONATE
                        , max(case when label = 'TOTALCO2' then valuenum else null end)          as TOTALCO2
                        , max(case when label = 'CARBOXYHEMOGLOBIN' then valuenum else null end) as CARBOXYHEMOGLOBIN
                        , max(case when label = 'CHLORIDE' then valuenum else null end)          as CHLORIDE
                        , max(case when label = 'CALCIUM' then valuenum else null end)           as CALCIUM
                        , max(case when label = 'GLUCOSE' then valuenum else null end)           as GLUCOSE
                        , max(case when label = 'HEMATOCRIT' then valuenum else null end)        as HEMATOCRIT
                        , max(case when label = 'HEMOGLOBIN' then valuenum else null end)        as HEMOGLOBIN
                        , max(case when label = 'INTUBATED' then valuenum else null end)         as INTUBATED
                        , max(case when label = 'LACTATE' then valuenum else null end)           as LACTATE
                        , max(case when label = 'METHEMOGLOBIN' then valuenum else null end)     as METHEMOGLOBIN
                        , max(case when label = 'O2FLOW' then valuenum else null end)            as O2FLOW
                        , max(case when label = 'FIO2' then valuenum else null end)              as FIO2
                        , max(case when label = 'SO2' then valuenum else null end)               as SO2 -- OXYGENSATURATION
                        , max(case when label = 'PCO2' then valuenum else null end)              as PCO2
                        , max(case when label = 'PEEP' then valuenum else null end)              as PEEP
                        , max(case when label = 'PH' then valuenum else null end)                as PH
                        , max(case when label = 'PO2' then valuenum else null end)               as PO2
                        , max(case when label = 'POTASSIUM' then valuenum else null end)         as POTASSIUM
                        , max(case when label = 'REQUIREDO2' then valuenum else null end)        as REQUIREDO2
                        , max(case when label = 'SODIUM' then valuenum else null end)            as SODIUM
                        , max(case when label = 'TEMPERATURE' then valuenum else null end)       as TEMPERATURE
                        , max(case when label = 'TIDALVOLUME' then valuenum else null end)       as TIDALVOLUME
                        , max(case when label = 'VENTILATIONRATE' then valuenum else null end)   as VENTILATIONRATE
                        , max(case when label = 'VENTILATOR' then valuenum else null end)        as VENTILATOR
                   from pvt
                   group by pvt.subject_id, pvt.hadm_id, pvt.icustay_id, pvt.CHARTTIME
                   order by pvt.subject_id, pvt.hadm_id, pvt.icustay_id, pvt.CHARTTIME)
                  ,
                 stg_spo2 as
                   (
                     select SUBJECT_ID
                          , HADM_ID
                          , ICUSTAY_ID
                          , CHARTTIME
                          -- max here is just used to group SpO2 by charttime
                          , max(case when valuenum <= 0 or valuenum > 100 then null else valuenum end) as SpO2
                     from CHARTEVENTS
                          -- o2 sat
                     where ITEMID in
                           (
                            646 -- SpO2
                             , 220277 -- O2 saturation pulseoxymetry
                             )
                     group by SUBJECT_ID, HADM_ID, ICUSTAY_ID, CHARTTIME
                   )
                  , stg_fio2 as
                 (
                   select SUBJECT_ID
                        , HADM_ID
                        , ICUSTAY_ID
                        , CHARTTIME
                        -- pre-process the FiO2s to ensure they are between 21-100%
                        , max(
                       case
                         when itemid = 223835
                           then case
                                  when valuenum > 0 and valuenum <= 1
                                    then valuenum * 100
                           -- improperly input data - looks like O2 flow in litres
                                  when valuenum > 1 and valuenum < 21
                                    then null
                                  when valuenum >= 21 and valuenum <= 100
                                    then valuenum
                                  else null end -- unphysiological
                         when itemid in (3420, 3422)
                           -- all these values are well formatted
                           then valuenum
                         when itemid = 190 and valuenum > 0.20 and valuenum < 1
                           -- well formatted but not in %
                           then valuenum * 100
                         else null end
                     ) as fio2_chartevents
                   from CHARTEVENTS
                   where ITEMID in
                         (
                          3420 -- FiO2
                           , 190 -- FiO2 set
                           , 223835 -- Inspired O2 Fraction (FiO2)
                           , 3422 -- FiO2 [measured]
                           )
                     -- exclude rows marked as error
                     and error IS DISTINCT FROM 1
                   group by SUBJECT_ID, HADM_ID, ICUSTAY_ID, CHARTTIME
                 )
                  , stg2 as
                 (
                   select bg.*
                        , ROW_NUMBER()
                       OVER (partition by bg.icustay_id, bg.charttime order by s1.charttime DESC) as lastRowSpO2
                        , s1.spo2
                   from blood_gas bg
                          left join stg_spo2 s1
                     -- same patient
                                    on bg.icustay_id = s1.icustay_id
                                      -- spo2 occurred at most 2 hours before this blood gas
                                      and s1.charttime between bg.charttime - interval '2' hour and bg.charttime
                   where bg.po2 is not null
                 )
                  , stg3 as
                 (
                   select bg.*
                        , ROW_NUMBER()
                       OVER (partition by bg.icustay_id, bg.charttime order by s2.charttime DESC) as lastRowFiO2
                        , s2.fio2_chartevents

                        -- create our specimen prediction
                        , 1 / (1 + exp(-(-0.02544
                     + 0.04598 * po2
                     + coalesce(-0.15356 * spo2, -0.15356 * 97.49420 + 0.13429)
                     + coalesce(0.00621 * fio2_chartevents, 0.00621 * 51.49550 + -0.24958)
                     + coalesce(0.10559 * hemoglobin, 0.10559 * 10.32307 + 0.05954)
                     + coalesce(0.13251 * so2, 0.13251 * 93.66539 + -0.23172)
                     + coalesce(-0.01511 * pco2, -0.01511 * 42.08866 + -0.01630)
                     + coalesce(0.01480 * fio2, 0.01480 * 63.97836 + -0.31142)
                     + coalesce(-0.00200 * aado2, -0.00200 * 442.21186 + -0.01328)
                     + coalesce(-0.03220 * bicarbonate, -0.03220 * 22.96894 + -0.06535)
                     + coalesce(0.05384 * totalco2, 0.05384 * 24.72632 + -0.01405)
                     + coalesce(0.08202 * lactate, 0.08202 * 3.06436 + 0.06038)
                     + coalesce(0.10956 * ph, 0.10956 * 7.36233 + -0.00617)
                     + coalesce(0.00848 * o2flow, 0.00848 * 7.59362 + -0.35803)
                     )))                                                                          as SPECIMEN_PROB
                   from stg2 bg
                          left join stg_fio2 s2
                     -- same patient
                                    on bg.icustay_id = s2.icustay_id
                                      -- fio2 occurred at most 4 hours before this blood gas
                                      and s2.charttime between bg.charttime - interval '4' hour and bg.charttime
                   where bg.lastRowSpO2 = 1 -- only the row with the most recent SpO2 (if no SpO2 found lastRowSpO2 = 1)
                 )
                  ,
                 bloodgasarterial as (
                   select subject_id
                        , hadm_id
                        ,
                     icustay_id
                        , charttime
                        , SPECIMEN -- raw data indicating sample type, only present 80% of the time

                        -- prediction of specimen for missing data
                        , case
                            when SPECIMEN is not null then SPECIMEN
                            when SPECIMEN_PROB > 0.75 then 'ART'
                            else null end as SPECIMEN_PRED
                        , SPECIMEN_PROB

                        -- oxygen related parameters
                        , SO2
                        , spo2     -- note spo2 is from chartevents
                        , PO2
                        , PCO2
                        , fio2_chartevents
                        , FIO2
                        , AADO2
                        -- also calculate AADO2
                        , case
                            when PO2 is not null
                              and pco2 is not null
                              and coalesce(FIO2, fio2_chartevents) is not null
                              -- multiple by 100 because FiO2 is in a % but should be a fraction
                              then (coalesce(FIO2, fio2_chartevents) / 100) * (760 - 47) - (pco2 / 0.8) - po2
                            else null
                     end                  as AADO2_calc
                        , case
                            when PO2 is not null and coalesce(FIO2, fio2_chartevents) is not null
                              -- multiply by 100 because FiO2 is in a % but should be a fraction
                              then 100 * PO2 / (coalesce(FIO2, fio2_chartevents))
                            else null
                     end                  as PaO2FiO2
                        -- acid-base parameters
                        , PH
                        , BASEEXCESS
                        , BICARBONATE
                        , TOTALCO2

                        -- blood count parameters
                        , HEMATOCRIT
                        , HEMOGLOBIN
                        , CARBOXYHEMOGLOBIN
                        , METHEMOGLOBIN

                        -- chemistry
                        , CHLORIDE
                        , CALCIUM
                        , TEMPERATURE
                        , POTASSIUM
                        , SODIUM
                        , LACTATE
                        , GLUCOSE

                        -- ventilation stuff that's sometimes input
                        , INTUBATED
                        , TIDALVOLUME
                        , VENTILATIONRATE
                        , VENTILATOR
                        , PEEP
                        , O2Flow
                        , REQUIREDO2

                   from stg3
                   where lastRowFiO2 = 1 -- only the most recent FiO2
                     -- restrict it to *only* arterial samples
                     and (SPECIMEN = 'ART' or SPECIMEN_PROB > 0.75)
                   order by icustay_id, charttime)
                  ,
                 ventsettings AS (
                   select icustay_id
                        , charttime
                        -- case statement determining whether it is an instance of mech vent
                        , max(
                       case
                         when itemid is null or value is null then 0 -- can't have null values
                         when itemid = 720 and value != 'Other/Remarks' THEN 1 -- VentTypeRecorded
                         when itemid = 223848 and value != 'Other' THEN 1
                         when itemid = 223849 then 1 -- ventilator mode
                         when itemid = 467 and value = 'Ventilator' THEN 1 -- O2 delivery device == ventilator
                         when itemid in
                              (
                               445, 448, 449, 450, 1340, 1486, 1600, 224687 -- minute volume
                                , 639, 654, 681, 682, 683, 684, 224685, 224684, 224686 -- tidal volume
                                , 218, 436, 535, 444, 459, 224697, 224695, 224696, 224746,
                               224747 -- High/Low/Peak/Mean/Neg insp force ("RespPressure")
                                , 221, 1, 1211, 1655, 2000, 226873, 224738, 224419, 224750, 227187 -- Insp pressure
                                , 543 -- PlateauPressure
                                , 5865, 5866, 224707, 224709, 224705, 224706 -- APRV pressure
                                , 60, 437, 505, 506, 686, 220339, 224700 -- PEEP
                                , 3459 -- high pressure relief
                                , 501, 502, 503, 224702 -- PCV
                                , 223, 667, 668, 669, 670, 671, 672 -- TCPCV
                                , 224701 -- PSVlevel
                                )
                           THEN 1
                         else 0
                         end
                     ) as MechVent
                        , max(
                       case
                         -- initiation of oxygen therapy indicates the ventilation has ended
                         when itemid = 226732 and value in
                                                  (
                                                   'Nasal cannula', -- 153714 observations
                                                   'Face tent', -- 24601 observations
                                                   'Aerosol-cool', -- 24560 observations
                                                   'Trach mask ', -- 16435 observations
                                                   'High flow neb', -- 10785 observations
                                                   'Non-rebreather', -- 5182 observations
                                                   'Venti mask ', -- 1947 observations
                                                   'Medium conc mask ', -- 1888 observations
                                                   'T-piece', -- 1135 observations
                                                   'High flow nasal cannula', -- 925 observations
                                                   'Ultrasonic neb', -- 9 observations
                                                   'Vapomist' -- 3 observations
                                                    ) then 1
                         when itemid = 467 and value in
                                               (
                                                'Cannula', -- 278252 observations
                                                'Nasal Cannula', -- 248299 observations
                                                'None', -- 95498 observations
                                                'Face Tent', -- 35766 observations
                                                'Aerosol-Cool', -- 33919 observations
                                                'Trach Mask', -- 32655 observations
                                                'Hi Flow Neb', -- 14070 observations
                                                'Non-Rebreather', -- 10856 observations
                                                'Venti Mask', -- 4279 observations
                                                'Medium Conc Mask', -- 2114 observations
                                                'Vapotherm', -- 1655 observations
                                                'T-Piece', -- 779 observations
                                                'Hood', -- 670 observations
                                                'Hut', -- 150 observations
                                                'TranstrachealCat', -- 78 observations
                                                'Heated Neb', -- 37 observations
                                                'Ultrasonic Neb' -- 2 observations
                                                 ) then 1
                         else 0
                         end
                     ) as OxygenTherapy
                        , max(
                       case
                         when itemid is null or value is null then 0
                         -- extubated indicates ventilation event has ended
                         when itemid = 640 and value = 'Extubated' then 1
                         when itemid = 640 and value = 'Self Extubation' then 1
                         else 0
                         end
                     )
                       as Extubated
                        , max(
                       case
                         when itemid is null or value is null then 0
                         when itemid = 640 and value = 'Self Extubation' then 1
                         else 0
                         end
                     )
                       as SelfExtubated
                   from chartevents ce
                   where ce.value is not null
                     -- exclude rows marked as error
                     and ce.error IS DISTINCT FROM 1
                     and itemid in
                         (
                           -- the below are settings used to indicate ventilation
                          720, 223849 -- vent mode
                           , 223848 -- vent type
                           , 445, 448, 449, 450, 1340, 1486, 1600, 224687 -- minute volume
                           , 639, 654, 681, 682, 683, 684, 224685, 224684, 224686 -- tidal volume
                           , 218, 436, 535, 444, 224697, 224695, 224696, 224746,
                          224747 -- High/Low/Peak/Mean ("RespPressure")
                           , 221, 1, 1211, 1655, 2000, 226873, 224738, 224419, 224750, 227187 -- Insp pressure
                           , 543 -- PlateauPressure
                           , 5865, 5866, 224707, 224709, 224705, 224706 -- APRV pressure
                           , 60, 437, 505, 506, 686, 220339, 224700 -- PEEP
                           , 3459 -- high pressure relief
                           , 501, 502, 503, 224702 -- PCV
                           , 223, 667, 668, 669, 670, 671, 672 -- TCPCV
                           , 224701 -- PSVlevel

                           -- the below are settings used to indicate extubation
                           , 640 -- extubated

                           -- the below indicate oxygen/NIV, i.e. the end of a mechanical vent event
                           , 468 -- O2 Delivery Device#2
                           , 469 -- O2 Delivery Mode
                           , 470 -- O2 Flow (lpm)
                           , 471 -- O2 Flow (lpm) #2
                           , 227287 -- O2 Flow (additional cannula)
                           , 226732 -- O2 Delivery Device(s)
                           , 223834 -- O2 Flow

                           -- used in both oxygen + vent calculation
                           , 467 -- O2 Delivery Device
                           )
                   group by icustay_id, charttime
                   UNION
                   -- add in the extubation flags from procedureevents_mv
                   -- note that we only need the start time for the extubation
                   -- (extubation is always charted as ending 1 minute after it started)
                   select icustay_id
                        , starttime                                   as charttime
                        , 0                                           as MechVent
                        , 0                                           as OxygenTherapy
                        , 1                                           as Extubated
                        , case when itemid = 225468 then 1 else 0 end as SelfExtubated
                   from procedureevents_mv
                   where itemid in
                         (
                          227194 -- "Extubation"
                           , 225468 -- "Unplanned Extubation (patient-initiated)"
                           , 225477 -- "Unplanned Extubation (non-patient initiated)"
                           ))
                  ,
                 ventdurations as (
                   with vd0 as
                     (
                       select icustay_id
                            -- this carries over the previous charttime which had a mechanical ventilation event
                            , case
                                when MechVent = 1 then
                                  LAG(CHARTTIME, 1) OVER (partition by icustay_id, MechVent order by charttime)
                                else null
                         end as charttime_lag
                            , charttime
                            , MechVent
                            , OxygenTherapy
                            , Extubated
                            , SelfExtubated
                       from ventsettings
                     )
                      , vd1 as
                     (
                       select icustay_id
                            , charttime_lag
                            , charttime
                            , MechVent
                            , OxygenTherapy
                            , Extubated
                            , SelfExtubated

                            -- if this is a mechanical ventilation event, we calculate the time since the last event
                            , case
                         -- if the current observation indicates mechanical ventilation is present
                         -- calculate the time since the last vent event
                                when MechVent = 1 then
                                  CHARTTIME - charttime_lag
                                else null
                         end          as ventduration

                            , LAG(Extubated, 1)
                                  OVER
                                    (
                                    partition by icustay_id, case when MechVent = 1 or Extubated = 1 then 1 else 0 end
                                    order by charttime
                                    ) as ExtubatedLag

                            -- now we determine if the current mech vent event is a "new", i.e. they've just been intubated
                            , case
                         -- if there is an extubation flag, we mark any subsequent ventilation as a new ventilation event
                         --when Extubated = 1 then 0 -- extubation is *not* a new ventilation event, the *subsequent* row is
                                when
                                    LAG(Extubated, 1)
                                        OVER
                                          (
                                          partition by icustay_id, case when MechVent = 1 or Extubated = 1 then 1 else 0 end
                                          order by charttime
                                          )
                                    = 1 then 1
                         -- if patient has initiated oxygen therapy, and is not currently vented, start a newvent
                                when MechVent = 0 and OxygenTherapy = 1 then 1
                         -- if there is less than 8 hours between vent settings, we do not treat this as a new ventilation event
                                when (CHARTTIME - charttime_lag) > interval '8' hour
                                  then 1
                                else 0
                         end          as newvent
                            -- use the staging table with only vent settings from chart events
                       FROM vd0 ventsettings
                     )
                      , vd2 as
                     (
                       select vd1.*
                            -- create a cumulative sum of the instances of new ventilation
                            -- this results in a monotonic integer assigned to each instance of ventilation
                            , case
                                when MechVent = 1 or Extubated = 1 then
                                  SUM(newvent)
                                      OVER ( partition by icustay_id order by charttime )
                                else null end
                         as ventnum
                            --- now we convert CHARTTIME of ventilator settings into durations
                       from vd1
                     )
                      -- create the durations for each mechanical ventilation instance
                   select icustay_id
                        -- regenerate ventnum so it's sequential
                        , ROW_NUMBER() over (partition by icustay_id order by ventnum)  as ventnum
                        , min(charttime)                                                as starttime
                        , max(charttime)                                                as endtime
                        , extract(epoch from max(charttime) - min(charttime)) / 60 / 60 AS duration_hours
                   from vd2
                   group by icustay_id, ventnum
                   having min(charttime) != max(charttime)
                      -- patient had to be mechanically ventilated at least once
                      -- i.e. max(mechvent) should be 1
                      -- this excludes a frequent situation of NIV/oxygen before intub
                      -- in these cases, ventnum=0 and max(mechvent)=0, so they are ignored
                      and max(mechvent) = 1
                   order by icustay_id, ventnum)
                  ,
                 wt AS
                   (
                     SELECT ie.icustay_id
                          -- ensure weight is measured in kg
                          , avg(CASE
                                  WHEN itemid IN (762, 763, 3723, 3580, 226512)
                                    THEN valuenum
                       -- convert lbs to kgs
                                  WHEN itemid IN (3581)
                                    THEN valuenum * 0.45359237
                                  WHEN itemid IN (3582)
                                    THEN valuenum * 0.0283495231
                                  ELSE null
                       END) AS weight

                     from icustays ie
                            left join chartevents c
                                      on ie.icustay_id = c.icustay_id
                     WHERE valuenum IS NOT NULL
                       AND itemid IN
                           (
                            762, 763, 3723, 3580, -- Weight Kg
                            3581, -- Weight lb
                            3582, -- Weight oz
                            226512 -- Metavision: Admission Weight (Kg)
                             )
                       AND valuenum != 0
                       and charttime between current_hour and next_hour
                       -- exclude rows marked as error
                       AND c.error IS DISTINCT FROM 1
                     group by ie.icustay_id
                   )
                  -- 5% of patients are missing a weight, but we can impute weight using their echo notes
                  , echo2 as (
                 select ie.icustay_id, avg(weight * 0.45359237) as weight
                 from icustays ie
                        left join echodata echo
                                  on ie.hadm_id = echo.hadm_id
                                    and echo.charttime > current_hour
                                    and echo.charttime < next_hour
                 group by ie.icustay_id
               )
                  , vaso_cv as
                 (
                   select ie.icustay_id
                        -- case statement determining whether the ITEMID is an instance of vasopressor usage
                        , max(case
                                when itemid = 30047 then rate / coalesce(wt.weight, ec.weight) -- measured in mcgmin
                                when itemid = 30120
                                  then rate -- measured in mcgkgmin ** there are clear errors, perhaps actually mcgmin
                                else null
                     end)                                                       as rate_norepinephrine

                        , max(case
                                when itemid = 30044 then rate / coalesce(wt.weight, ec.weight) -- measured in mcgmin
                                when itemid in (30119, 30309) then rate -- measured in mcgkgmin
                                else null
                     end)                                                       as rate_epinephrine

                        , max(case when itemid in (30043, 30307) then rate end) as rate_dopamine
                        , max(case when itemid in (30042, 30306) then rate end) as rate_dobutamine

                   from icustays ie
                          inner join inputevents_cv cv
                                     on ie.icustay_id = cv.icustay_id and
                                        cv.charttime between current_hour and next_hour
                          left join wt
                                    on ie.icustay_id = wt.icustay_id
                          left join echo2 ec
                                    on ie.icustay_id = ec.icustay_id
                   where itemid in (30047, 30120, 30044, 30119, 30309, 30043, 30307, 30042, 30306)
                     and rate is not null
                   group by ie.icustay_id
                 )
                  , vaso_mv as
                 (
                   select ie.icustay_id
                        -- case statement determining whether the ITEMID is an instance of vasopressor usage
                        , max(case when itemid = 221906 then rate end) as rate_norepinephrine
                        , max(case when itemid = 221289 then rate end) as rate_epinephrine
                        , max(case when itemid = 221662 then rate end) as rate_dopamine
                        , max(case when itemid = 221653 then rate end) as rate_dobutamine
                   from icustays ie
                          inner join inputevents_mv mv
                                     on ie.icustay_id = mv.icustay_id and
                                        mv.starttime between current_hour and next_hour
                   where itemid in (221906, 221289, 221662, 221653)
                     -- 'Rewritten' orders are not delivered to the patient
                     and statusdescription != 'Rewritten'
                   group by ie.icustay_id
                 )
                  , pafi1 as
                 (
                   -- join blood gas to ventilation durations to determine if patient was vent
                   select bg.icustay_id
                        , bg.charttime
                        , PaO2FiO2
                        , case when vd.icustay_id is not null then 1 else 0 end as IsVent
                   from bloodgasarterial bg
                          left join ventdurations vd
                                    on bg.icustay_id = vd.icustay_id
                                      and bg.charttime >= vd.starttime
                                      and bg.charttime <= vd.endtime
                   order by bg.icustay_id, bg.charttime
                 )
                  , pafi2 as
                 (
                   -- because pafi has an interaction between vent/PaO2:FiO2, we need two columns for the score
                   -- it can happen that the lowest unventilated PaO2/FiO2 is 68, but the lowest ventilated PaO2/FiO2 is 120
                   -- in this case, the SOFA score is 3, *not* 4.
                   select icustay_id
                        , min(case when IsVent = 0 then PaO2FiO2 else null end) as PaO2FiO2_novent_min
                        , min(case when IsVent = 1 then PaO2FiO2 else null end) as PaO2FiO2_vent_min
                   from pafi1
                   group by icustay_id
                 )
                  -- Aggregate the components for the score
                  , scorecomp as
                 (
                   select ie.icustay_id
                        , v.MeanBP_Min
                        , coalesce(cv.rate_norepinephrine, mv.rate_norepinephrine) as rate_norepinephrine
                        , coalesce(cv.rate_epinephrine, mv.rate_epinephrine)       as rate_epinephrine
                        , coalesce(cv.rate_dopamine, mv.rate_dopamine)             as rate_dopamine
                        , coalesce(cv.rate_dobutamine, mv.rate_dobutamine)         as rate_dobutamine

                        , l.Creatinine_Max
                        , l.Bilirubin_Max
                        , l.Platelet_Min

                        , pf.PaO2FiO2_novent_min
                        , pf.PaO2FiO2_vent_min

                        , uo.UrineOutput

                        , gcs.MinGCS
                   from icustays ie
                          left join vaso_cv cv
                                    on ie.icustay_id = cv.icustay_id
                          left join vaso_mv mv
                                    on ie.icustay_id = mv.icustay_id
                          left join pafi2 pf
                                    on ie.icustay_id = pf.icustay_id
                          left join vitals v
                                    on ie.icustay_id = v.icustay_id
                          left join labs l
                                    on ie.icustay_id = l.icustay_id
                          left join urine_output uo
                                    on ie.icustay_id = uo.icustay_id
                          left join gcs gcs
                                    on ie.icustay_id = gcs.icustay_id
                 )
                  , scorecalc as
                 (
                   -- Calculate the final score
                   -- note that if the underlying data is missing, the component is null
                   -- eventually these are treated as 0 (normal), but knowing when data is missing is useful for debugging
                   select icustay_id
                        -- Respiration
                        , case
                            when PaO2FiO2_vent_min < 100 then 4
                            when PaO2FiO2_vent_min < 200 then 3
                            when PaO2FiO2_novent_min < 300 then 2
                            when PaO2FiO2_novent_min < 400 then 1
                            when coalesce(PaO2FiO2_vent_min, PaO2FiO2_novent_min) is null then null
                            else 0
                     end as respiration

                        -- Coagulation
                        , case
                            when platelet_min < 20 then 4
                            when platelet_min < 50 then 3
                            when platelet_min < 100 then 2
                            when platelet_min < 150 then 1
                            when platelet_min is null then null
                            else 0
                     end as coagulation

                        -- Liver
                        , case
                     -- Bilirubin checks in mg/dL
                            when Bilirubin_Max >= 12.0 then 4
                            when Bilirubin_Max >= 6.0 then 3
                            when Bilirubin_Max >= 2.0 then 2
                            when Bilirubin_Max >= 1.2 then 1
                            when Bilirubin_Max is null then null
                            else 0
                     end as liver

                        -- Cardiovascular
                        , case
                            when rate_dopamine > 15 or rate_epinephrine > 0.1 or rate_norepinephrine > 0.1 then 4
                            when rate_dopamine > 5 or rate_epinephrine <= 0.1 or rate_norepinephrine <= 0.1 then 3
                            when rate_dopamine > 0 or rate_dobutamine > 0 then 2
                            when MeanBP_Min < 70 then 1
                            when coalesce(MeanBP_Min, rate_dopamine, rate_dobutamine, rate_epinephrine,
                                          rate_norepinephrine) is null then null
                            else 0
                     end as cardiovascular

                        -- Neurological failure (GCS)
                        , case
                            when (MinGCS >= 13 and MinGCS <= 14) then 1
                            when (MinGCS >= 10 and MinGCS <= 12) then 2
                            when (MinGCS >= 6 and MinGCS <= 9) then 3
                            when MinGCS < 6 then 4
                            when MinGCS is null then null
                            else 0 end
                         as cns

                        -- Renal failure - high creatinine or low urine output
                        , case
                            when (Creatinine_Max >= 5.0) then 4
                            when UrineOutput < 200 then 4
                            when (Creatinine_Max >= 3.5 and Creatinine_Max < 5.0) then 3
                            when UrineOutput < 500 then 3
                            when (Creatinine_Max >= 2.0 and Creatinine_Max < 3.5) then 2
                            when (Creatinine_Max >= 1.2 and Creatinine_Max < 2.0) then 1
                            when coalesce(UrineOutput, Creatinine_Max) is null then null
                            else 0 end
                         as renal
                   from scorecomp
                 )

               select ie.subject_id
                    , ie.hadm_id
                    , ie.icustay_id
                    -- Combine all the scores to get SOFA
                    -- Impute 0 if the score is missing
                    , coalesce(respiration, 0)
                 + coalesce(coagulation, 0)
                 + coalesce(liver, 0)
                 + coalesce(cardiovascular, 0)
                 + coalesce(cns, 0)
                 + coalesce(renal, 0)
                 as SOFA
                    , respiration
                    , coagulation
                    , liver
                    , cardiovascular
                    , cns
                    , renal
               from icustays ie
                      left join scorecalc s
                                on ie.icustay_id = s.icustay_id
                                  and ie.icustay_id = 200191
             ) as sofa_query;

        raise notice 'SOFA SCORE: %' , v_sofa_score ;
        raise notice 'RESPIRATION SCORE: %' ,      v_respiration;
        raise notice 'COAGULATION SCORE: %' ,      v_coagulation;
        raise notice 'LIVER SCORE: %' ,      v_liver;
        raise notice 'CARDIO SCORE: %' ,      v_cardiovascular;
        raise notice 'CNS SCORE: %' ,     v_cns ;
        raise notice 'RENAL SCORE: %' ,     v_renaL;

        if rec = 1 then
          v_initial_sofa_Score = v_sofa_score;

        end if;

        insert into mimiciii.sepsis3_hourlysofa_comparing
        (
          hadm_id ,
          icustay_id ,
          intime ,
          outtime ,
          suspection_of_infection_time ,
          window_start_time ,
          window_end_time ,
          calculation_time ,
          sofa_score ,
          respiration ,
          coagulation ,
          liver ,
          cardiovascular ,
          cns ,
          renal,
          subject_id

        ) values
        (
         cv_hadm_id,
         cv_icustay_id,
         cv_intime,
         cv_outtime,
         cv_suspected_infection_time,
         init_window_hr,
         final_window_hr,
         current_hour,
         v_sofa_score,
         v_respiration,
         v_coagulation,
         v_liver,
         v_cardiovascular,
         v_cns,
         v_renal,
         cv_subject_id
        );

        select count(*)
          into v_sepsis_onset_count
        from sepsis3_onsettime_new
          where icustay_id = cv_icustay_id;

        if ((v_sofa_score - v_initial_sofa_Score) >= 2) and(v_sepsis_onset_count=0 ) then
          v_Sepsis_onset_time = current_hour;
          raise notice 'SEPSIS ONSET TIME: %', current_hour;
          insert into sepsis3_onsettime_new_comparing
          (
            hadm_id ,
            icustay_id ,
            intime ,
            outtime ,
            initial_sofa_score  ,
            onsettime_sofa_score ,
            onsettime_respiration ,
            onsettime_coagulation ,
            onsettime_liver ,
            onsettime_cardiovascular ,
            onsettime_cns ,
            onsettime_renal ,
            sepsis_onset_time


          ) values (cv_hadm_id,
                    cv_icustay_id,
                    cv_intime,
                    cv_outtime,
                    v_initial_sofa_Score,
                    v_sofa_score,
                    v_respiration,
                    v_coagulation,
                    v_liver,
                    v_cardiovascular,
                    v_cns,
                    v_renal,
                    v_Sepsis_onset_time);
         -- exit;
        end if;

      --------
        current_hour = next_hour;

      end loop;


      cohort_count = cohort_count +1;
      raise notice 'count : % ', cohort_count;


  END LOOP;
      RAISE NOTICE 'final count :%',cohort_count;


  CLOSE cur_cohort;

END;

$BODY$ LANGUAGE plpgsql;
