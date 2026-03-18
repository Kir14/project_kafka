
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pizza_type
from "pizza_db"."public"."revenue_by_pizza"
where pizza_type is null



  
  
      
    ) dbt_internal_test