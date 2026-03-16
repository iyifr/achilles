# get aa from  ade.aaa
def get_collections_name(ns: str) -> str:
    name = ns.split(".")[-1]
    return name
