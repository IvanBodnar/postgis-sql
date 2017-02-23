
-- Función para contar la agregación de puntos coincidentes geográficamente.
-- Devuelve una tabla con id, geometría representando el punto, y un entero
-- que representa la cantidad de puntos encontrados en la distancia que se 
-- ingresó.
create or replace function clusters(tabla text, distancia integer)
returns table (id bigint, punto geometry, cantidad integer)
as
$$
begin

return query execute
format ('
with x as (
  select st_transform(geom, 3857) as geom from %s
),
y as (
select 
       unnest(st_clusterwithin(geom, %s)) as geom_1
       from x
)
select row_number() over() as id,
       st_centroid(st_transform(geom_1, 4326)) as punto,
       st_numgeometries(geom_1) as cantidad
       from y
', tabla, distancia);
end
$$ language plpgsql;

-----------------------------------------------------------------

-- Tabla con siniestros a 500 mts de la traza de metro 9 de julio
create table vistas.nuevo_metro_9 as (
with metro_9 as (
  select 
    1 as idq,
    st_union(geom) as geom
  from trabajo.lineas_metrobus
  where metrobus = 'Metrobus 9 de Julio'
)
select
  v.id,
  fecha,
  hora,
  franja_horaria,
  dia_semana,
  mes,
  anio as año,
  tipo_calle,
  v.causa,
  sexo,
  edad,
  franja_edad,
  rol,
  tipo,
  case when fecha <= '13-08-2013' then 'pre'
       when fecha >= '25-07-2013' then 'post'
       end
       as obra_pre_post,
  case when fecha <= '10-01-2013' then 'pre'
       when fecha >= '11-01-2013' and fecha <= '13-08-2013' then 'durante'
       when fecha >= '14-08-2013' then 'post'
       end
       as obra_pre_durante_post,
  v.tipo_reclas_1 as tipo_victima_reclasificado,
  case when array_length(participantes, 1) > 2
       then 'multiple'
       when participantes is null
       then null
       -- Primero ordena alfabeticamente el array y despues lo transforma a string:
       else array_to_string(array(select unnest(participantes) as x order by x), ' - ', 'sin datos')
       end
       as
       participantes_involucrados,
  comuna,
  geom
from victimas v
join hechos h
on h.id = v.id_hecho
where
st_dwithin(
  st_transform(geom, 98334),
  st_transform(st_setsrid((select geom from metro_9), 4326), 98334),
  500)
and
anio between 2010 and 2015
and tipo_calle != 'autopista'
order by fecha
)

---------------------------------------------------------------------------

-- Devuelve el punto donde las calles se cruzan o se tocan.
-- Si es resultado es multipoint devuelve el primero.
-- Si no se cruzan o tocan devuelve NULL.
-- Usa union_geom()
CREATE OR REPLACE FUNCTION punto_interseccion(calle1 text, calle2 text)
  RETURNS GEOMETRY AS $$
DECLARE resultado GEOMETRY;
BEGIN
  IF (st_crosses(union_geom(calle1), union_geom(calle2))) OR 
     (st_touches(union_geom(calle1), union_geom(calle2)))
     THEN
     SELECT ST_Intersection(union_geom(calle1), union_geom(calle2)) limit 1
     INTO resultado;
     
     IF st_numgeometries(resultado) > 1
     THEN
     resultado := st_geometryn(resultado, 1);
     END IF;
     
  ELSE
    resultado := NULL;
  END IF;

  RETURN resultado;
END;
$$ LANGUAGE 'plpgsql';

----------------------------------------------------------------------------------

-- Muestra la cantidad de siniestros ocurridos por mes en 2015 y el ratio entre ese mes
-- y el total del año 2015
with a_2015 as (
  select count(*) from hechos where anio = 2015
)
select
  mes,
  count(*),
  count(*)::float / (select * from a_2015)::float as ratio
from hechos
where anio = 2015
group by mes
order by mes;

