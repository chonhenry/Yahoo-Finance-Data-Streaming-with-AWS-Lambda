SELECT t1.company, t1.hour, substring(ts,1,19) as datatime , t1.hourly_high_price

from (
    SELECT name as company, substring(ts,12,2) as hour, max(high) as hourly_high_price
    FROM  "sta-9760-project-3-db"."01"
    GROUP BY 1, 2
    ORDER BY 1, 2
     ) t1,
  "sta-9760-project-3-db"."01" t2
 
where t1.company=t2.name and  t1.hour = substring(ts,12,2) and t1.hourly_high_price = t2.high
 
order by 1, 2
;
