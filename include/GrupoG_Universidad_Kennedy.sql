SELECT universidades as university, carreras as career, to_date(fechas_de_inscripcion, 'YY-Mon-DD') as inscription_date, split_part(nombres,'-',1) as first_name,
split_part(nombres,'-',2) as last_name, sexo as gender, date_part('year', age(now(), to_date(fechas_nacimiento, 'YY-Mon-DD'))) as age,
codigos_postales as postal_code, l.localidad as location, emails as email
FROM uba_kenedy uk, localidad l 
where cast(uk.codigos_postales as integer) = l.codigo_postal
and universidades = 'universidad-j.-f.-kennedy' 
and to_date(fechas_de_inscripcion, 'YY-Mon-DD') between to_date('01-09-2020','DD-MM-YYY') and to_date('01-02-2021','DD-MM-YYY')
order by inscription_date, career, last_name, first_name