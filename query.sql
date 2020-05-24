select t1.company, t1.hour, t2.ts as datetime, t1.hourly_high_price

from (
    select name as company, hour(date_add('hour',-4,cast(ts as timestamp))) as hour, max(high) as hourly_high_price
    from  "sta-9760-project-3-db"."01"
    group BY 1, 2
    order BY 1, 2
     ) t1,
  "sta-9760-project-3-db"."01" t2
 
where t1.company=t2.name and  t1.hour = hour(date_add('hour',-4,cast(t2.ts as timestamp))) and t1.hourly_high_price = t2.high
 
order by 1, 3
;
