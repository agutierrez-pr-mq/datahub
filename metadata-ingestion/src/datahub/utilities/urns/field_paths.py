import re

# A v2 path token such as `[version=2.0]`, `[key=True]` or `[type=a.b.Envelope]`.
# Record names may contain dots, so tokens are matched by brackets, not split on ".".
_V2_TOKEN_RE = re.compile(r"\[[^\]]*\]\.?")


def get_simple_field_path_from_v2_field_path(field_path: str) -> str:
    """A helper function to extract simple . path notation from the v2 field path"""

    if field_path.startswith("[version=2.0]"):
        # this is a v2 field path
        return _V2_TOKEN_RE.sub("", field_path).strip(".")
    else:
        # not a v2, we assume this is a simple path
        return field_path
