CREATE OR REPLACE FUNCTION sp_check_septic_shock()
RETURNS VOID AS
 $BODY$
 DECLARE
  cur_cohort CURSOR FOR
  SELECT coh.icustay_id,
         coh.hadm_id,
         coh.suspected_infection_time_poe,
         coh.intime,
         coh.outtime,
         coh.subject_id,
         coh.sepsis_onsettime
  FROM sepsis3_cohort coh
  WHERE coh.suspected_of_infection_poe = 1
  and coh.excluded=0
  and coh.waveform_exists = 1
  and coh.sepsis_onsettime is not null
  order by  coh.icustay_id;


  cv_icustay_id                       integer;
  cv_hadm_id                          sepsis3_cohort.hadm_id%TYPE;
  cv_suspected_infection_time         sepsis3_cohort.suspected_infection_time_poe%TYPE;
  init_window_hr                      sepsis3_cohort.suspected_infection_time_poe%TYPE;
  final_window_hr                     sepsis3_cohort.suspected_infection_time_poe%TYPE;
  starting_hour                       sepsis3_cohort.suspected_infection_time_poe%TYPE;
  cv_intime                           sepsis3_cohort.intime%TYPE;
  cv_outtime                          sepsis3_cohort.intime%TYPE;
  tmp                                sepsis3_cohort.intime%TYPE;
  current_hour sepsis3_cohort.sepsis_onsettime%TYPE;
  next_hour sepsis3_cohort.sepsis_onsettime%TYPE;
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
  cv_sepsis_onsettime sepsis3_cohort.sepsis_onsettime%type;
  v_weight chartevents.valuenum%type := 98.8;
  v_current_weight chartevents.valuenum%type;
    v_previous_weight chartevents.valuenum%type;
   v_next_weight chartevents.valuenum%type;
   v_fluids chartevents.valuenum%type;
   v_urine_output chartevents.valuenum%type;
   v_cvp chartevents.valuenum%type;
   v_low_map_count integer;
   v_high_lactate_count integer;
   v_low_map_charttime  chartevents.charttime%type;
   v_low_map_value chartevents.valuenum%type;
   v_high_lactate_charttime chartevents.charttime%type;
   v_high_lactate_value chartevents.valuenum%type;
   v_septic_shock_onsettime chartevents.charttime%type;

BEGIN

  -- This procedure is used to calulate sepsis onset time for certain test patients  considering the time window suspected infection time - 48 hours and susspected infection time + 24 hours (without ICU inntime , outtime)
  open cur_cohort;

  LOOP
    -- fetch row into the film
     fetch next from cur_cohort
      into cv_icustay_id,
       cv_hadm_id,
       cv_suspected_infection_time,
       cv_intime,
       cv_outtime,
       cv_subject_id,
       cv_sepsis_onsettime;

      EXIT WHEN NOT FOUND;

      v_sofa_score := 0;
      v_respiration := 0;
      v_coagulation := 0;
      v_liver := 0;
      v_cardiovascular := 0;
      v_cns := 0;
      v_renal := 0;
      v_initial_sofa_Score := 0;


      --cv_sepsis_onsettime = TIMESTAMP '2186-01-25 21:30:00.00';
      --number_hours = 48;
      number_hours = round(CAST(EXTRACT(EPOCH FROM (cv_outtime - cv_sepsis_onsettime ))/3600 as numeric));

      current_hour = cv_sepsis_onsettime;

      FOR rec IN 1..number_hours BY 1 LOOP

        next_hour = current_hour + interval '1' hour;

        -- raise notice 'starting hour   :%',current_hour ;
        -- raise notice 'next hour   :%',next_hour;


        ---- gettting the current weight


        select avg(
          (case
          when mv_weight.itemid IN (226531,3581) then
          mv_weight.valuenum * 0.45359237

          WHEN mv_weight.itemid IN (3582)
          THEN mv_weight.valuenum * 0.0283495231

          else
          mv_weight.valuenum
          end )
          )
        into v_current_weight
        from mv_septic_shock_weight mv_weight
        where mv_weight.icustay_id = cv_icustay_id
        -- and ch.itemid in (763,224639, 762,226531,226512)
        and mv_weight.charttime between current_hour and next_hour;

        --raise notice 'Current weight : % ', v_current_weight;

        ---- If we do not have any value for current weight , get the most recent weight before the current hour



          select (
          (case
          when mv_weight.itemid IN (226531,3581) then
          mv_weight.valuenum * 0.45359237

          WHEN mv_weight.itemid IN (3582)
          THEN mv_weight.valuenum * 0.0283495231

          else
          mv_weight.valuenum
          end )
          )
            into v_previous_weight
          from mv_septic_shock_weight mv_weight
          where mv_weight.icustay_id = cv_icustay_id
          -- and ch.itemid in (763,224639, 762,226531,226512)
            and mv_weight.charttime < current_hour
            order by mv_weight.charttime desc
            LIMIT 1;


        --raise notice 'previous weight : % ', v_previous_weight;

       ---- If we do not have any value for current weight and previous weight , get the most recent weight after the next hour

          select (
          (case
          when mv_weight.itemid IN (226531,3581) then
          mv_weight.valuenum * 0.45359237

          WHEN mv_weight.itemid IN (3582)
          THEN mv_weight.valuenum * 0.0283495231

          else
          mv_weight.valuenum
          end )
          )
            into v_next_weight
          from mv_septic_shock_weight mv_weight
          where mv_weight.icustay_id = cv_icustay_id
          -- and ch.itemid in (763,224639, 762,226531,226512)
            and mv_weight.charttime > next_hour
            order by mv_weight.charttime
            LIMIT 1;


        --raise notice 'NEXT weight : % ', v_next_weight;

        if (v_current_weight is null) or (v_current_weight=0) then
          if  (v_previous_weight is null) or (v_previous_weight=0) then
            if (v_next_weight is not null) or (v_next_weight<>0) then
              v_weight = v_next_weight;
            else
              raise notice 'Weight not found';
            end if;
           else
             v_weight = v_previous_weight;
          end if;
        else
          v_weight = v_current_weight;
        end if;

        --raise notice 'final weight fot this hour : % ', v_weight;

        select sum(amount)/ v_weight
        into v_fluids
        from mv_septic_shock_fluids MV
        where MV.starttime between cv_sepsis_onsettime and next_hour
        AND MV.icustay_id = cv_icustay_id;
        --AND MV.hadm_id = 112221
        -- and amountuom = 'ml'
       -- and ( lower(ordercategoryname) like '%crystalloid%' or lower(secondaryordercategoryname) like '%crystalloid%');

        --raise notice 'FLUIDS : % ', v_fluids;

        select sum(value)/v_weight
        into v_urine_output
        from  mv_septic_shock_urineout op
        where op.icustay_id = cv_icustay_id
        and op.charttime  between current_hour and next_hour;

    --raise notice 'URINE OUTPUT : % ', v_urine_output;

        --716, 1103, 113, 220074
       select avg(op.valuenum)
        into v_cvp
        from  mv_septic_shock_cvp op
        where op.icustay_id = cv_icustay_id
        and op.charttime  between current_hour and next_hour;

      --raise notice 'CVP : % ', v_cvp;

        if (v_fluids >= 30) or (v_urine_output >= 0.5) or (v_cvp between 8 and 12) then
          --the patient was given enough fluid
          -- check for MAP < 65 mmhg and lactate > 2
        raise notice 'Reached adequate fuild given state';

          select count(*)
          into v_low_map_count
            from  mv_septic_shock_map map
          where map.icustay_id = cv_icustay_id
          -- and ch.itemid in (456,52,6702,443,220052,220181,225312)
            and map.charttime >= current_hour
            and map.valuenum < 65;

          --raise notice 'low map count : % ', v_low_map_count;

          select count(*)
          into v_high_lactate_count
            from  mv_septic_shock_lactate lac
          where lac.icustay_id = cv_icustay_id
          -- and ch.itemid in ( 818, 1531, 225668 )
            and lac.charttime >= current_hour
            and lac.valuenum > 2;

            --raise notice 'v_high_lactate_count : % ', v_high_lactate_count;

          if v_low_map_count > 0  AND v_high_lactate_count > 0 THEN
            raise notice 'septic shock diagnosed';

            select map.charttime,map.valuenum
            into v_low_map_charttime, v_low_map_value
            from  mv_septic_shock_map map
          where map.icustay_id = cv_icustay_id
          -- and ch.itemid in (456,52,6702,443,220052,220181,225312)
            and map.charttime >= current_hour
            and map.valuenum < 65
            order by map.charttime
            limit 1;

        --raise notice 'v_low_map_charttime : % ', v_low_map_charttime;
        raise notice 'v_low_map_value : % ', v_low_map_value;

        select lac.charttime, lac.valuenum
            into v_high_lactate_charttime, v_high_lactate_value
            from   mv_septic_shock_lactate lac
          where lac.icustay_id = cv_icustay_id
          -- and ch.itemid in  ( 818, 1531, 225668 )
            and lac.charttime >= current_hour
            and lac.valuenum > 2
            order by charttime
            limit 1;


        --raise notice 'v_high_lactate_charttime : % ', v_high_lactate_charttime;
        raise notice 'v_high_lactate_value : % ', v_high_lactate_value;

            if v_low_map_charttime < v_high_lactate_charttime then
              v_septic_shock_onsettime = v_high_lactate_charttime;

            elseif v_low_map_charttime > v_high_lactate_charttime then
              v_septic_shock_onsettime = v_low_map_charttime;

            elseif   v_low_map_charttime = v_high_lactate_charttime then
               v_septic_shock_onsettime = v_low_map_charttime;

            end if;
            raise notice 'sepsis shock onset time : % ', v_septic_shock_onsettime;

            insert into sepsis3_shock_onsettime
              (icustay_id ,
              intime ,
              outtime ,
              suspected_infection_time ,
              sepsis_onsettime ,
              septic_shock_onsettime ,
              first_low_map_charttime ,
              first_high_lactate_charttime ,
              first_low_map_value ,
              first_high_lactate_value
              )
            values
              (cv_icustay_id,
               cv_intime,
               cv_outtime,
               cv_suspected_infection_time,
               cv_sepsis_onsettime,
               v_septic_shock_onsettime,
               v_low_map_charttime,
               v_high_lactate_charttime,
               v_low_map_value,
               v_high_lactate_value
               );

        end if;
        EXIT;

      end if;
      current_hour = next_hour;
      end loop;


      cohort_count = cohort_count +1;
      raise notice 'count : % ', cohort_count;

  END LOOP;
      RAISE NOTICE 'final count :%',cohort_count;


  CLOSE cur_cohort;

END;

$BODY$ LANGUAGE plpgsql;