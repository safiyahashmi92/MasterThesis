# SQL statements to get number of ICU stays with different criterias




# To select initial cohort
select count(*) from sepsis3_cohort coh where coh.excluded=0 ;--11791


# To select patients from initial cohort and suffering from sepsis
select count(*) from sepsis3_cohort coh where coh.excluded=0 and coh.has_sepsis is null; --5776


# To select patients from initial cohort and not suffering from sepsis
select count(*) from sepsis3_cohort coh where coh.excluded=0 and coh.has_sepsis =1; --6015





# To select patients from initial cohort and those who have waveform recored in the waveform DB
select count(*) from sepsis3_cohort coh where coh.excluded=0 and coh.waveform_exists = 1 ;--4653


# To select patients from initial cohort that have waveform recored in the waveform DB and sufferig from sepsis
select count(*) from sepsis3_cohort coh where coh.excluded=0 and coh.waveform_exists = 1 and coh.has_sepsis =1 ;--2325


# To select patients from initial cohort that have waveform recored in the waveform DB and not sufferig from sepsis
select count(*) from sepsis3_cohort coh where coh.excluded=0 and coh.waveform_exists = 1 and coh.has_sepsis is null ;--2328


