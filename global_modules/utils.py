from os import cpu_count
from pathlib import Path

_THREADS = (cpu_count() or 2) // 2
DUCKDB_THREADS = 3
GLOBAL_FILES_PATH = Path(__file__).parents[1].joinpath("global_files")


def get_parquet_file(sufixo: str, root_path: str):
    """
    Facilita o criação de arquivos `.parquet`.

    :param sufixo: Sufixo para compor o final do nome do arquivo.
    :type sufixo: str
    :param root_path: Caminho da pasta para colocar o arquivo. Se for um arquivo, será considerado a pasta deste.
    :type root_path: str
    :return: `Path` que representa o arquivo `.parquet`
    :rtype: Path
    """
    f = Path(root_path)
    root_dir = f.parent if f.is_file() else f
    file = root_dir.joinpath(f"extract_data_{str(sufixo)}.parquet")
    return file


# reads the sql file and returns the query
def read_sql_file(filename: str, root_path: str):
    """
    Recupera o conteúdo de um arquivo que está em uma pasta `sql_files`.

    :param filename: Nome do arquivo `sql`.
    :type filename: str
    :param root_path: Caminho da pasta onde a pasta `sql_files` está. Se for um arquivo, será considerado a pasta deste.
    :type root_path: str
    :return: O conteúdo do arquivo.
    :rtype: str
    """
    f = Path(root_path)
    root_dir = f.parent if f.is_file() else f
    sql_file = root_dir.joinpath("sql_files", filename)

    if not sql_file.exists():
        raise ValueError(f"O arquivo '{sql_file.resolve()}' não existe.")

    return sql_file.read_text()
