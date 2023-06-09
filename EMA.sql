with recursive MA as (
    select date_key as dt, 
           0.5 as alpha,
           row_number() over (order by date_key) rn,
           close as cl 
    from stock_market_fact        
),
ema as (
    select dt,alpha,rn,cl, close as close_ema from MA 
    where rn = 1
    
    union all
    
    select t2.date_key, 
           t2.alpha, 
           t2.rn, 
           t2.cl, 
           t2.alpha * t2.close + (1.0 - t2.alpha) * ema.close as close_ema
    from ema
    join MA t2 on ema.rn = t2.rn - 1
)

select date_key, close, close_ema
from ema;



select p.*,
     (
       sum(power((1 / 0.5), seqnum) * close) 
        over ( order by seqnum) 
         +
        first_value(close) over (order by seqnum)
       ) 
       / 
       power(2, seqnum + 1) as EMA
from (select p.*,
             row_number() over (order by date_key) - 1 as seqnum
      from stock_market_fact p
      where company_id = 103 and  date_key >to_date('12/18/2022', 'mm/dd/yyyy')
     ) p






with recursive p as (
      select p.*, row_number() over (partition by company_id order by date_key) as seqnum
      from stock_market_fac p
     ),
     cte as (
      select seqnum, date_key, close, close * 1.0 as exp_avg
      from p
      where seqnum = 1
      union all
      select p.seqnum, p.date_key, p.close, (cte.exp_avg * 0.5  + p.close * 0.5)  
      from cte join
           p
           on p.seqnum = cte.seqnum + 1
     )
select *
from cte;






