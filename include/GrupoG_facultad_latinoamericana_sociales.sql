SELECT 
    upper(trim(replace(universities,'-',' '))) as university,
    upper(trim(replace(careers,'-',' '))) as career, 
    to_date(inscription_dates, 'DD-MM-YYYY') as inscription_date, 
    split_part(names,'-',1) as first_name,
    split_part(names,'-',2) as last_name, 
    sexo as gender, 
    date_part('year', age(now(), to_date(birth_dates, 'DD-MM-YYYY'))) as age,
    l.codigo_postal as postal_code, 
    upper(trim(replace(locations,'-',' '))) as location, 
    emails as email
FROM lat_sociales_cine lsc, localidad l 
where l.localidad = REPLACE(lsc.locations  , '-', ' ')
and universities = '-FACULTAD-LATINOAMERICANA-DE-CIENCIAS-SOCIALES'  
and to_date(inscription_dates, 'DD-MM-YYYY') between to_date('01-09-2020','DD-MM-YYY') and to_date('01-02-2021','DD-MM-YYY')
order by inscription_date, career, last_name, first_name