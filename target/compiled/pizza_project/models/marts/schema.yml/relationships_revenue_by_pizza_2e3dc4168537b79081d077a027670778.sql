
    
    

with child as (
    select pizza_type as from_field
    from "pizza_db"."public"."revenue_by_pizza"
    where pizza_type is not null
),

parent as (
    select pizza_type as to_field
    from "pizza_db"."public"."pizza_prices"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


