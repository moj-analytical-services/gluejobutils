

def add_slash(s) :
    """
    Adds slash to end of string
    """
    return s if s[-1] == '/' else s + '/'

def remove_slash(s) :
    """
    Removes slash to end of string
    """
    return s[0:-1] if s[-1] == '/' else s

    