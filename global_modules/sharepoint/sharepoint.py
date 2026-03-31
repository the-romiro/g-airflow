import pandas as pd
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from sqlalchemy import types as sa_types

from global_modules.sharepoint.envs import env
from global_modules.sharepoint.utils import map_to_json, parse_datetime, parse_number


def fetch_sharepoint_items(
    list_fields: dict[str, sa_types.TypeEngine],
    expand_fields: list[str] | None = None,
    datetime_columns: list[str] | None = None,
    page_size: int = 1000,
    retrieve: int | None = 10_000,
    site_url: str | None = None,
    list_name: str | None = None,
):
    """
    Recupera os registros de uma lista do Sharepoint.

    Todos os campos serão retornados como `str`

    Para funciona, é necessário fornecer um usuário e senha que tenha acesso a lista do Sharepoint. Esses dados devem ser fornecidos via variável de ambiente `SHAREPOINT_USERNAME` e `SHAREPOINT_USER_PASSWORD`. Se não fornecido um `ValueError` será gerado.

    :param list_fields: Um dicionário contendo os nomes das colunas da lista como key e o tipo do banco de dados como value `dict[field_name, sqlalchemy.types]`. Exemplo: `{'Title': sqlalchemy.types.VARCHAR(255), 'fabrica/Title': sqlalchemy.types.VARCHAR(255)}`.

    :type list_fields: dict[str, sqlalchemy.types]

    :param expand_fields: Uma lista com o nome das colunas que são referencia de outra lista (colunas do tipo lookup/consulta).
    :type expand_fields: list[str] | None

    :param datetime_columns: Uma lista com o nome das colunas que são datetime para que seja feito a conversão.
    :type datetime_columns: list[str] | None

    :param page_size: Quantidade registros a serem recuperados por vez pela api do sharepoint.
    :type page_size: int, `default: 1000`.

    :param retrieve: Total de linhas a serem recuperadas da lista. O calculo é `ID - retrieve`, onde `ID` é o campo que o Sharepoint preenche automaticamente. Em uma lista com `50_000` linhas e queremos recuperar `10_000`, serão retornados todos os registro com `ID > 40_000`.
    :type retrieve: int | None, `Default: 10_000`.

    :param site_url: Site onde a lista se encontra exemplo: `https://grendenecombr.sharepoint.com/sites/dados_industriais`. Se `None` for passado tentamos pegar da variável de ambiente `SHAREPOINT_SITE_URL` se não encontrar será gerado um `ValueError`.
    :type site_url: str | None

    :param list_name: Nome das lista para recuperar os dados. Se `None` for passado tentamos pegar da variável de ambiente `SHAREPOINT_SITE_URL` se não encontrar será gerado um `ValueError`.

    :type list_name: str | None

    :return: Um `Pandas.DataFrame` contendo os dados.
    :rtype: DataFrame
    """
    initial_id: int | None = None

    if site_url is None:
        site_url = env("SHAREPOINT_SITE_URL")

    ctx = ClientContext(site_url).with_credentials(
        UserCredential(env("SHAREPOINT_USERNAME"), env("SHAREPOINT_USER_PASSWORD")),
    )

    if list_name is None:
        list_name = env("SHAREPOINT_LIST_NAME")

    sp_list = ctx.web.lists.get_by_title(list_name)

    query = sp_list.items

    if retrieve is not None:
        last_item = (
            query.select(["ID"]).top(page_size).order_by("ID desc").top(1).get().execute_query()
        )
        initial_id = last_item.to_json()[0]["ID"] - retrieve

    query = query.clear_state().select(list(list_fields.keys()))

    if datetime_columns is not None:
        query = query.expand(expand_fields)  # type: ignore

    if initial_id is not None:
        query = query.filter(f"ID ge {initial_id}")

    paged_items = query.top(page_size).get().execute_query()

    def to_df(items) -> pd.DataFrame:
        return (
            pd.DataFrame(map_to_json(items.to_json()))
            .astype(str)
            .drop(columns=["Id"], errors="ignore")
        )

    all_items = to_df(paged_items)

    while True:
        if not paged_items.has_next:
            break
        paged_items = paged_items._get_next().execute_query()
        all_items = pd.concat([all_items, to_df(paged_items)], ignore_index=True)

    if datetime_columns is not None:
        all_items = parse_datetime(all_items, datetime_columns)  # type: ignore

    all_items = parse_number(all_items, list_fields)

    return all_items
