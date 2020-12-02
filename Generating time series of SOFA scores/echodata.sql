CREATE MATERIALIZED VIEW ECHODATA AS
select ne.ROW_ID
  , ne.subject_id, ne.hadm_id
  , ne.chartdate

  -- charttime is always null for echoes..
  -- however, the time is available in the echo text, e.g.:
  -- , substring(ne.text, 'Date/Time: [\[\]0-9*-]+ at ([0-9:]+)') as TIMESTAMP
  -- we can therefore impute it and re-create charttime
  , cast(to_timestamp( (to_char( ne.chartdate, 'DD-MM-YYYY' ) || substring(ne.text, 'Date/Time: [\[\]0-9*-]+ at ([0-9:]+)')),
            'DD-MM-YYYYHH24:MI') as timestamp without time zone)
    as charttime

  -- explanation of below substring:
  --  'Indication: ' - matched verbatim
  --  (.*?) - match any character
  --  \n - the end of the line
  -- substring only returns the item in ()s
  -- note: the '?' makes it non-greedy. if you exclude it, it matches until it reaches the *last* \n

  , substring(ne.text, 'Indication: (.*?)\n') as Indication

  -- sometimes numeric values contain de-id text, e.g. [** Numeric Identifier **]
  -- this removes that text
  , case
      when substring(ne.text, 'Height: \(in\) (.*?)\n') like '%*%'
        then null
      else cast(substring(ne.text, 'Height: \(in\) (.*?)\n') as numeric)
    end as Height

  , case
      when substring(ne.text, 'Weight \(lb\): (.*?)\n') like '%*%'
        then null
      else cast(substring(ne.text, 'Weight \(lb\): (.*?)\n') as numeric)
    end as Weight

  , case
      when substring(ne.text, 'BSA \(m2\): (.*?) m2\n') like '%*%'
        then null
      else cast(substring(ne.text, 'BSA \(m2\): (.*?) m2\n') as numeric)
    end as BSA -- ends in 'm2'

  , substring(ne.text, 'BP \(mm Hg\): (.*?)\n') as BP -- Sys/Dias

  , case
      when substring(ne.text, 'BP \(mm Hg\): ([0-9]+)/[0-9]+?\n') like '%*%'
        then null
      else cast(substring(ne.text, 'BP \(mm Hg\): ([0-9]+)/[0-9]+?\n') as numeric)
    end as BPSys -- first part of fraction

  , case
      when substring(ne.text, 'BP \(mm Hg\): [0-9]+/([0-9]+?)\n') like '%*%'
        then null
      else cast(substring(ne.text, 'BP \(mm Hg\): [0-9]+/([0-9]+?)\n') as numeric)
    end as BPDias -- second part of fraction

  , case
      when substring(ne.text, 'HR \(bpm\): ([0-9]+?)\n') like '%*%'
        then null
      else cast(substring(ne.text, 'HR \(bpm\): ([0-9]+?)\n') as numeric)
    end as HR

  , substring(ne.text, 'Status: (.*?)\n') as Status
  , substring(ne.text, 'Test: (.*?)\n') as Test
  , substring(ne.text, 'Doppler: (.*?)\n') as Doppler
  , substring(ne.text, 'Contrast: (.*?)\n') as Contrast
  , substring(ne.text, 'Technical Quality: (.*?)\n') as TechnicalQuality
from noteevents ne
inner join sepsis3_cohort coh
on coh.hadm_id = ne.hadm_id
--and coh.suspected_of_infection_poe = 1
and coh.excluded=0
where ne.category = 'Echo';
