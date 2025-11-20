select 
	TO_CHAR(dt, 'YYYY-MM-DD') as inicio
	,
	TO_CHAR(dt + interval '1 day', 'YYYY-MM-DD') as fim
from
	(
	select
		MIN("E3TimeStamp") as dt
	from
		silver.temp_ciclos
)