def validate_equal_lengths(**kwargs) -> None:
    """
    makes sure all fields have the same length
    """
    lengths = {key: len(val) for key, val in kwargs.items() if val}

    if not lengths:
        return

    reference_key, reference_len = next(iter(lengths.items()))
    unequal = {k: v for k, v in lengths.items() if v != reference_len}

    if unequal:
        breakdown = ", ".join(f"{k}={v}" for k, v in lengths.items())
        raise ValueError(
            f"All fields must have the same number of entries. Got: {breakdown}"
        )


def validate_name(name: str, name_type: str):
    if not name:
        raise ValueError(f"Invalid {name_type} name: empty")
    if not name.isidentifier():
        raise ValueError(f"Invalid {name_type} name: {name!r}")
