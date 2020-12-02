CREATE OR REPLACE FUNCTION sp_calc_hourly_SOFA_entire_icustay()
 RETURNS VOID AS
 $BODY$
 DECLARE
  cur_cohort CURSOR FOR
  SELECT coh.icustay_id,
         coh.hadm_id,
         coh.suspected_infection_time_poe,
         coh.intime,
         coh.outtime,
         coh.subject_id
  FROM sepsis3_cohort coh
  WHERE /* coh.suspected_of_infection_poe = 1
  and */ coh.excluded=0
  and coh.waveform_exists =1
  order by coh.icustay_id;


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

       /*
      init_window_hr = (cv_suspected_infection_time - interval '48' hour );
      final_window_hr = ( cv_suspected_infection_time + interval '24' hour );

      if init_window_hr < cv_intime then
        init_window_hr = cv_intime;
      end if;

      raise notice 'outtime: %', cv_outtime;

      if final_window_hr > cv_outtime then
        final_window_hr = cv_outtime;
      end if;
      */

      init_window_hr = cv_intime;
      final_window_hr = cv_outtime;
      number_hours = round(CAST(EXTRACT(EPOCH FROM (final_window_hr - init_window_hr ))/3600 as numeric));


      /*
      raise notice 'time of infection suspection  :%',cv_suspected_infection_time;
      raise notice 'intial window time  :%',init_window_hr;
      raise notice 'final window time  :%',final_window_hr;
      raise notice 'number of hours between initial and final window  :%',number_hours;
      */
      --starting_hour = init_window_hr;
      raise notice 'ICUSTAY ID   :%',cv_icustay_id;

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

        --raise notice 'starting hour   :%',current_hour ;
        --raise notice 'next hour   :%',next_hour;

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
            with wt AS
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
            from icustays ie --todo
            left join chartevents c
              on ie.icustay_id = c.icustay_id
            WHERE valuenum IS NOT NULL
            AND itemid IN
            (
              762, 763, 3723, 3580,                     -- Weight Kg
              3581,                                     -- Weight lb
              3582,                                     -- Weight oz
              226512 -- Metavision: Admission Weight (Kg)
            )
            AND valuenum != 0
            --and charttime between ie.intime - interval '1' day and ie.intime + interval '1' day
            and charttime between (current_hour + interval '1' second ) - interval '1' day and next_hour
            -- exclude rows marked as error
            AND c.error IS DISTINCT FROM 1
            and ie.icustay_id = cv_icustay_id
            group by ie.icustay_id
          )
          -- 5% of patients are missing a weight, but we can impute weight using their echo notes
          , echo2 as(
            select ie.icustay_id, avg(weight * 0.45359237) as weight
            from icustays ie
            left join echodata echo
              on ie.hadm_id = echo.hadm_id
              and echo.charttime > (current_hour + interval '1' second ) - interval '7' day
              and echo.charttime < next_hour
              and ie.icustay_id = cv_icustay_id
            group by ie.icustay_id
          )
          , vaso_cv as
          (
            select ie.icustay_id
              -- case statement determining whether the ITEMID is an instance of vasopressor usage
              , max(case
                      when itemid = 30047 then rate / coalesce(wt.weight,ec.weight) -- measured in mcgmin
                      when itemid = 30120 then rate -- measured in mcgkgmin ** there are clear errors, perhaps actually mcgmin
                      else null
                    end) as rate_norepinephrine

              , max(case
                      when itemid =  30044 then rate / coalesce(wt.weight,ec.weight) -- measured in mcgmin
                      when itemid in (30119,30309) then rate -- measured in mcgkgmin
                      else null
                    end) as rate_epinephrine

              , max(case when itemid in (30043,30307) then rate end) as rate_dopamine
              , max(case when itemid in (30042,30306) then rate end) as rate_dobutamine

            from icustays ie ---todo
            inner join inputevents_cv cv
              on ie.icustay_id = cv.icustay_id and cv.charttime between (current_hour + interval '1' second ) and next_hour
            left join wt
              on ie.icustay_id = wt.icustay_id
            left join echo2 ec
              on ie.icustay_id = ec.icustay_id
            where itemid in (30047,30120,30044,30119,30309,30043,30307,30042,30306)
              and ie.icustay_id=cv_icustay_id
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
            from icustays ie --todo
            inner join inputevents_mv mv
              on ie.icustay_id = mv.icustay_id and mv.starttime between (current_hour + interval '1' second ) and next_hour
            where itemid in (221906,221289,221662,221653)
            -- 'Rewritten' orders are not delivered to the patient
            and statusdescription != 'Rewritten'
            and ie.icustay_id=cv_icustay_id
            group by ie.icustay_id
          )
          ,
          base as
            (
              SELECT l.*
              from gcs_part1 l
              inner join icustays b
                on l.icustay_id = b.icustay_id
              and l.icustay_id = cv_icustay_id
              -- Isolate the desired GCS variable
              -- Only get data for the first 24 hours
              and l.charttime between (current_hour + interval '1' second ) and next_hour
              -- exclude rows marked as error
              --and l.error IS DISTINCT FROM 1
            )
            , gcs as (
              select b.*
              , b2.GCSVerbal as GCSVerbalPrev
              , b2.GCSMotor as GCSMotorPrev
              , b2.GCSEyes as GCSEyesPrev
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
                        coalesce(b.GCSMotor,6)
                      + coalesce(b.GCSVerbal,5)
                      + coalesce(b.GCSEyes,4)
                  -- otherwise, add up score normally, imputing previous value if none available at current time
                  else
                        coalesce(b.GCSMotor,coalesce(b2.GCSMotor,6))
                      + coalesce(b.GCSVerbal,coalesce(b2.GCSVerbal,5))
                      + coalesce(b.GCSEyes,coalesce(b2.GCSEyes,4))
                  end as GCS

              from base b
              -- join to itself within 6 hours to get previous value
              left join base b2
                on b.ICUSTAY_ID = b2.ICUSTAY_ID and b.rn = b2.rn+1 and b2.charttime > b.charttime - interval '6' hour
            )
            , gcs_final as (
              select gcs.*
              -- This sorts the data by GCS, so rn=1 is the the lowest GCS values to keep
              , ROW_NUMBER ()
                      OVER (PARTITION BY gcs.ICUSTAY_ID
                            ORDER BY gcs.GCS
                           ) as IsMinGCS
              from gcs
            ),
            mingcs as (
            select ie.SUBJECT_ID, ie.HADM_ID, ie.ICUSTAY_ID
            -- The minimum GCS is determined by the above row partition, we only join if IsMinGCS=1
            , GCS as MinGCS
            , coalesce(GCSMotor,GCSMotorPrev) as GCSMotor
            , coalesce(GCSVerbal,GCSVerbalPrev) as GCSVerbal
            , coalesce(GCSEyes,GCSEyesPrev) as GCSEyes
            , EndoTrachFlag as EndoTrachFlag

            -- subselect down to the cohort of eligible patients
            from icustays ie
            left join gcs_final gs
              on ie.ICUSTAY_ID = gs.ICUSTAY_ID and gs.IsMinGCS = 1
            ORDER BY ie.ICUSTAY_ID )
          , pafi1 as
          (
            -- join blood gas to ventilation durations to determine if patient was vent
            select bg.icustay_id, bg.charttime
            , PaO2FiO2
            , case when vd.icustay_id is not null then 1 else 0 end as IsVent
            from bloodgasarterial bg
            left join ventdurations vd
              on bg.icustay_id = vd.icustay_id
              and bg.charttime >= vd.starttime
              and bg.charttime <= vd.endtime
              -- and bg.charttime between (current_hour + interval '1' second ) and next_hour
              and bg.icustay_id = cv_icustay_id
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
            where pafi1.charttime between (current_hour + interval '1' second ) and next_hour
            and pafi1.icustay_id = cv_icustay_id
            group by icustay_id
          )
          -- Aggregate the components for the score
          , scorecomp as
          (
          select ie.icustay_id
            , (v.MeanBP_Min)
            , coalesce(cv.rate_norepinephrine, mv.rate_norepinephrine) as rate_norepinephrine
            , coalesce(cv.rate_epinephrine, mv.rate_epinephrine) as rate_epinephrine
            , coalesce(cv.rate_dopamine, mv.rate_dopamine) as rate_dopamine
            , coalesce(cv.rate_dobutamine, mv.rate_dobutamine) as rate_dobutamine

            , (l.Creatinine_Max)
            , (l.Bilirubin_Max)
            , (l.Platelet_Min)

            , pf.PaO2FiO2_novent_min
            , pf.PaO2FiO2_vent_min

            , (uo.UrineOutput)

            , gcs.MinGCS
          from icustays ie
          left join vaso_cv cv
            on ie.icustay_id = cv.icustay_id
          left join vaso_mv mv
            on ie.icustay_id = mv.icustay_id
          left join pafi2 pf
           on ie.icustay_id = pf.icustay_id
          left join (select min(vit.meanbp_min) as MeanBP_Min,
                            vit.icustay_id
                      from vitals vit
                      where vit.icustay_id = cv_icustay_id
                      and  vit.charttime between  (current_hour + interval '1' second ) and next_hour
                        group by vit.icustay_id) v
            on ie.icustay_id = v.icustay_id
            -- and v.charttime between current_hour and next_hour
          left join (select max(labs.creatinine_max) as Creatinine_Max,
                            max(labs.Bilirubin_Max) as Bilirubin_Max,
                            min(labs.Platelet_Min) as Platelet_Min ,
                            labs.icustay_id
                      from labs labs
                      where labs.icustay_id = cv_icustay_id
                      and  labs.charttime between  (current_hour + interval '1' second ) and next_hour
                      group by labs.icustay_id) l
            on ie.icustay_id = l.icustay_id
            --and l.charttime between current_hour and next_hour
          left join
            (select sum(up.urineoutput) as UrineOutput,
                            up.icustay_id
              from urine_output up
              where up.icustay_id = cv_icustay_id
              and  up.charttime between  (current_hour + interval '1' second ) and next_hour
              group by up.icustay_id) uo
            on ie.icustay_id = uo.icustay_id
            -- and uo.charttime between current_hour and next_hour
          left join mingcs gcs
            on ie.icustay_id = gcs.icustay_id
            -- and gcs.charttime between current_hour and next_hour
          and ie.icustay_id=cv_icustay_id

          )
          , scorecalc as
          (
            -- Calculate the final score
            -- note that if the underlying data is missing, the component is null
            -- eventually these are treated as 0 (normal), but knowing when data is missing is useful for debugging
            select icustay_id
            -- Respiration
            , case
                when PaO2FiO2_vent_min   < 100 then 4
                when PaO2FiO2_vent_min   < 200 then 3
                when PaO2FiO2_novent_min < 300 then 2
                when PaO2FiO2_novent_min < 400 then 1
                when coalesce(PaO2FiO2_vent_min, PaO2FiO2_novent_min) is null then null
                else 0
              end as respiration

            -- Coagulation
            , case
                when platelet_min < 20  then 4
                when platelet_min < 50  then 3
                when platelet_min < 100 then 2
                when platelet_min < 150 then 1
                when platelet_min is null then null
                else 0
              end as coagulation

            -- Liver
            , case
                -- Bilirubin checks in mg/dL
                  when Bilirubin_Max >= 12.0 then 4
                  when Bilirubin_Max >= 6.0  then 3
                  when Bilirubin_Max >= 2.0  then 2
                  when Bilirubin_Max >= 1.2  then 1
                  when Bilirubin_Max is null then null
                  else 0
                end as liver

            -- Cardiovascular
            , case
                when rate_dopamine > 15 or rate_epinephrine >  0.1 or rate_norepinephrine >  0.1 then 4
                when rate_dopamine >  5 or rate_epinephrine <= 0.1 or rate_norepinephrine <= 0.1 then 3
                when rate_dopamine >  0 or rate_dobutamine > 0 then 2
                when MeanBP_Min < 70 then 1
                when coalesce(MeanBP_Min, rate_dopamine, rate_dobutamine, rate_epinephrine, rate_norepinephrine) is null then null
                else 0
              end as cardiovascular

            -- Neurological failure (GCS)
            , case
                when (MinGCS >= 13 and MinGCS <= 14) then 1
                when (MinGCS >= 10 and MinGCS <= 12) then 2
                when (MinGCS >=  6 and MinGCS <=  9) then 3
                when  MinGCS <   6 then 4
                when  MinGCS is null then null
            else 0 end
              as cns

            -- Renal failure - high creatinine or low urine output
            , case
              when (Creatinine_Max >= 5.0) then 4
              when  UrineOutput < 200 then 4
              when (Creatinine_Max >= 3.5 and Creatinine_Max < 5.0) then 3
              when  UrineOutput < 500 then 3
              when (Creatinine_Max >= 2.0 and Creatinine_Max < 3.5) then 2
              when (Creatinine_Max >= 1.2 and Creatinine_Max < 2.0) then 1
              when coalesce(UrineOutput, Creatinine_Max) is null then null
            else 0 end
              as renal
            from scorecomp
          )
          select ie.subject_id, ie.hadm_id, ie.icustay_id
            -- Combine all the scores to get SOFA
            -- Impute 0 if the score is missing
            , coalesce(respiration,0)
            + coalesce(coagulation,0)
            + coalesce(liver,0)
            + coalesce(cardiovascular,0)
            + coalesce(cns,0)
            + coalesce(renal,0)
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
          --and  ie.icustay_id=cv_icustay_id
          order by ie.icustay_id ) test
        WHERE test.icustay_id = cv_icustay_id;

      /*
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

        */

        insert into mimiciii.sepsis3_hourlysofa_entire_icustay
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

        /*
        if (v_sofa_score - v_initial_sofa_Score) >= 2 then
          v_Sepsis_onset_time = current_hour;
          raise notice 'SEPSIS ONSET TIME: %', current_hour;
          insert into sepsis3_onsettime_new
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
        */
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



