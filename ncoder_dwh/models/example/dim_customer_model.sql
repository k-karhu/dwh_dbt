{{ config(materialized='table', sort='timestamp', dist='user_id') }}

with hubspot_contacts as (
    select * 
    from {{ sources('contacts', 'processed_hubspot_contacts_all')}}
), 

mailjet_contacts as (
    select * 
    from {{ sources('contacts', 'processed_mailjet_contacts_all')}}
), 

dim_email as (
    select * 
    from {{ sources('contacts', 'dim_email')}}
)

final as (

    select distinct 
    email_id as customer_id, 
    case when 
         hc.email is not null 
         then 1 
         else 0 
    end as hubspot_customer, 
    case when 
         mc.email is not null 
         then 1 
         else 0 
    end as mailjet_customer, 
    coalesce(hc.domain_suffix, mc.domain_suffix) as domain_suffix

    from      dim_email as em 
    left join hubspot_contacts as hc 
    on hc.email = em.email_address
    left join mailjet_contacts as mc 
    on mc.email = em.email_address 
)

select * from final