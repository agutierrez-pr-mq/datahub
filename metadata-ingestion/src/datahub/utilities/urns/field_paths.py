import re

# [...] tokens may contain dots, e.g. [type=com.example.Order]
_V2_PATH_TOKEN_RE = re.compile(r"\[[^\]]*\](?=\.|$)|[^.]+")


def get_simple_field_path_from_v2_field_path(field_path: str) -> str:
    """A helper function to extract simple . path notation from the v2 field path"""

    if field_path.startswith("[version=2.0]"):
        # this is a v2 field path
        tokens = [
            t
            for t in _V2_PATH_TOKEN_RE.findall(field_path)
            if not (t.startswith("[") or t.endswith("]"))
        ]
        path = ".".join(tokens)
        return path
    else:
        # not a v2, we assume this is a simple path
        return field_path
