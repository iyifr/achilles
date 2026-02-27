from achillesdb.config import get_config


cfg = get_config()

def validate_name(name: str, name_type: str):
    if not name:
        raise ValueError(f"Invalid {name_type} name: empty")
    if not name.isidentifier():
        raise ValueError(f"Invalid {name_type} name: {name!r}")
