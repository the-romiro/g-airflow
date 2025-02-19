select
    ? as id_estabelecimento,
    Codigo,
    Descricao,
    PesoOEE,
    ID_Familia,
    E3TimeStamp
from [elipse].[dbo].[Motivo_Paradas] with(nolock)