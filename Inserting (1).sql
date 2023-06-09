declare 
    cursor SMA_CALC is 
    select sm.Company_id,sm.date_key ,sm.Close, sum(sm.close) over(partition by sm.company_id order by sm.date_key   rows between current row and 20 following)/20 as SMA_20_day 
    from stock_market_fact sm
    join company_dim cc on sm.company_id = cc.company_key ;

begin 
    
    for sma_record in SMA_CALC loop 
    
    update stock_market_fact 
    set SMA = sma_record.SMA_20_day 
    where date_key = sma_record.date_key and company_id = sma_record.company_id ; 
    end loop ; 

end;


declare 
    cursor ADTV_CALC is 
    select sm.Company_ID,sm.date_key ,sm.volume, sum(sm.volume) over(partition by sm.company_id order by sm.date_key   rows between current row and 10 following)/10 as ADTV_10_day 
    from stock_market_fact sm
    join company_dim cc on sm.company_id = cc.company_key;

begin 
    
    for ADTV_record in ADTV_CALC loop 
    
    update stock_market_fact 
    set ADTV = ADTV_record.ADTV_10_day 
    where date_key = ADTV_record.date_key and company_id = ADTV_record.company_id ; 
    end loop ; 

end;



declare 
    cursor ROC_CALC is 
    select Company_ID , date_key ,close,trunc(((close - avg_close_10_day)/avg_close_10_day) *100,3) as ROC
    from(
    select sm.Company_id,sm.date_key ,sm.High,sm.Low,sm.Close, avg(sm.close) over(partition by sm.company_id order by sm.date_key desc range between current row and interval '30' day following) as avg_close_10_day 
    from stock_market_fact sm
    join company_dim cc on sm.company_id = cc.company_key 
    );


begin 
    
    for ROC_record in ROC_CALC loop 
    
    update stock_market_fact 
    set ROC = ROC_record.ROC
    where date_key = ROC_record.date_key and company_id = ROC_record.company_id ; 
    end loop ; 

end;






declare 
    cursor ATR_CALC is 
        with ATR_calc(Company_id,date_key,Hl, Hclose,Lclose) 
        as (
        select Company_id,date_key, High-Low as HL , high - lag_close as Hclose,  low-lag_close as Lclose
        from(
        select sm.Company_id,sm.date_key ,sm.High,sm.Low,sm.Close,lag(sm.close,1) over(partition by sm.company_id order by sm.date_key) as lag_close 
        from stock_market_fact sm
        join company_dim cc on sm.company_id = cc.company_key  ) )
        select company_id,date_key , sum(TR) over (partition by company_id order by date_key range between current row and interval '7' day following ) as ATR
        from (select  Company_id,date_key,greatest(Hl,Hclose,Lclose) as TR
        from ATR_calc )  ;


begin 
    
    for ATR_record in ATR_CALC loop 
    
    update stock_market_fact 
    set ATR = ATR_record.ATR
    where date_key = ATR_record.date_key and company_id = ATR_record.company_id ; 
    end loop ; 

end;





declare 
    cursor EMA_CALC is 
          select company_id , date_key ,
               trunc((
               sum(power((1 / 0.5), seqnum) * close)
                over (partition by company_id order by seqnum)
                 +
                first_value(close) over (partition by company_id order by seqnum)
               ) 
               / 
               power(2, seqnum + 1),3) as EMA
              from (select p.*,
                     row_number() over (partition by company_id order by date_key) - 1 as seqnum
              from stock_market_fact p
              where date_key < to_date('1/15/2022', 'mm/dd/yyyy')
              
             ) p ; 

begin 
    
    for EMA_record in EMA_CALC loop 
    
    update stock_market_fact 
    set EMA = EMA_record.EMA
    where date_key = EMA_record.date_key and company_id = EMA_record.company_id ; 
    end loop ; 

end;




declare 
    cursor Change_CALC is 
         select company_id,date_key ,close ,((close - prev_close)/ prev_close ) * 100 as change
         from (
         select  company_id , date_key,close , lag(close) over (partition by company_id order by date_key ) as prev_close
         from stock_market_fact
            ) ; 

begin 
    
    for Change_record in Change_CALC loop 
    
    update stock_market_fact 
    set change = Change_record.change
    where date_key = Change_record.date_key and company_id = Change_record.company_id ; 
    end loop ; 

end;
