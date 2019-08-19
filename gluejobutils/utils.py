import json

standard_date_format = "yyyy-MM-dd"
standard_date_format_python = "%Y-%m-%d"

standard_datetime_format = "yyyy-MM-dd HH:mm:ss"
standard_datetime_format_python = "%Y-%m-%d %H:%M:%S"


def add_slash(s):
    """
    Adds slash to end of string
    """
    return s if s[-1] == "/" else s + "/"


def remove_slash(s):
    """
    Removes slash to end of string
    """
    return s[0:-1] if s[-1] == "/" else s


def list_to_sql_select(col_list, table_alias=None, exclude=None):
    """
    combines a list of columns to a single string seperated with ', '
    If table alias is provided each column is prefixed with table_name.
    Users can exclude any columns in col_list by adding them in the exclude input
    """
    if table_alias is None:
        table_alias = ""
    else:
        table_alias = table_alias + "."

    if exclude is not None:
        col_list = [c for c in col_list if c not in exclude]

    return ", ".join([table_alias + c for c in col_list])


def read_json(filepath):
    """
    read json from local file
    """
    with open(filepath) as json_data:
        data = json.load(json_data)
    return data


def write_json(data, filepath):
    """
    write json to local file
    """
    with open(filepath, "w") as outfile:
        json.dump(data, outfile, indent=4, separators=(",", ": "))
