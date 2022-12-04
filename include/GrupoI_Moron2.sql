select  universidad as university,
        carrerra as career,
        fechaiscripccion as inscription_date,
        nombrre as first_name,
        null as last_name,
        sexo as gender,
        nacimiento as age,
        codgoposstal as postal_code,
        null as location,
        eemail as email
from    moron_nacional_pampa 
where   universidad = 'Universidad de morón' and
        to_date(fechaiscripccion, 'dd/mm/yyyy')  '2019-01-01' and '2020-08-01');
