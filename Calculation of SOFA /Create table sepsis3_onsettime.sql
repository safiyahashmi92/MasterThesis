create table if not exists mimiciii.sepsis3_onsettime
(
	hadm_id int4,
	icustay_id int4,
	intime timestamp(0),
	outtime timestamp(0),
	initial_sofa_score int4,
	onsettime_sofa_score int4,
	onsettime_respiration int4,
	onsettime_coagulation int4,
	onsettime_liver int4,
	onsettime_cardiovascular int4,
	onsettime_cns int4,
	onsettime_renal int4,
	sepsis_onset_time timestamp(6)
);

create index if not exists sepsis3_onsettime_icustay_id_idx
	on mimiciii.sepsis3_onsettime (icustay_id);

