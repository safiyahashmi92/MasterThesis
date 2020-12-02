
create table if not exists mimiciii.sepsis3_shock_onsettime
(
	icustay_id int4,
	intime timestamp(0),
	outtime timestamp(0),
	suspected_infection_time timestamp(0),
	sepsis_onsettime timestamp(0),
	septic_shock_onsettime timestamp(0),
	first_low_map_charttime timestamp(0),
	first_high_lactate_charttime timestamp(0),
	first_low_map_value float8,
  first_high_lactate_value float8
);

create index if not exists idx_sepsis3_shock_onsettime
	on mimiciii.sepsis3_shock_onsettime (icustay_id);