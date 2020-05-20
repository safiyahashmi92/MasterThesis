select count(*)
from spesis_onsettime_bp bp,
     spesis_onsettime_hr hr,
     spesis_onsettime_oxysat oxy,
     spesis_onsettime_temp temp,
     spesis_onsettime_gcs gcs,
     spesis_onsettime_resp resp
where bp.icustay_id = hr.icustay_id
and hr.icustay_id = oxy.icustay_id
and temp.icustay_id = gcs.icustay_id
and temp.icustay_id = oxy.icustay_id
and gcs.icustay_id = resp.icustay_id;
