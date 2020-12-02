select ch.charttime as recorded_time,
       (case
         when di.label = 'Temperature Fahrenheit' then
           (ch.valuenum - 32)/1.8
       else
          ch.valuenum
       end ) as recorded_temp_value
from chartevents ch, d_items di
where ch.itemid = di.itemid
and di.label in ('Temperature Fahrenheit', 'Temperature Celsius')
and ch.icustay_id = :icutay_id
order by ch.charttime;