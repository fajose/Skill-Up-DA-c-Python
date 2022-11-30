SELECT 
    upper(trim(replace(universidades,'-',' '))) as university,
    upper(trim(replace(carreras,'-', ' '))) as career, 
    to_date(fechas_de_inscripcion, 'YY-Mon-DD') as inscription_date, 
    upper(trim(split_part(nombres,'-',1))) as first_name, 
    upper(trim(split_part(nombres,'-',2))) as last_name,
    upper(sexo) as gender,
    case 
    when date_part('year', age(now(), to_date(fechas_nacimiento, 'YY-Mon-DD'))) < 0 
    then date_part('year', age(now(), to_date(fechas_nacimiento, 'YY-Mon-DD'))) + 100
    else date_part('year', age(now(), to_date(fechas_nacimiento, 'YY-Mon-DD')))
    end as age,
    codigos_postales as postal_code, 
    l.localidad as location, 
    upper(emails) as email
FROM uba_kenedy uk, localidad l 
where cast(uk.codigos_postales as integer) = l.codigo_postal
and universidades = 'universidad-de-buenos-aires' 
and to_date(fechas_de_inscripcion, 'YY-Mon-DD') between to_date('01-09-2020','DD-MM-YYY') and to_date('01-02-2021','DD-MM-YYY')
order by inscription_date, career, last_name, first_name;