select s.* from site s 
where name like '%MELBOURNE%' 
and state='VIC' 
and HCIS_L2='KX3P' 
and POSTCODE not in (3068,3032,3018,3015)




select c.* from device_details dd, licence_subservice ls, licence l, client c
where dd.ss_id=ls.ss_id
and ls.ss_id=l.ss_id
and l.client_no=c.client_no
and dd.licence_no=l.licence_no
and dd.site_id=10000022
