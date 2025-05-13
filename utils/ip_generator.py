import random


def generate_ip() -> str:
    """
    Genera una dirección IP aleatoria válida v4.

    Returns
    -------
    str
        IP en formato 'X.X.X.X'.
    """
    return ".".join(str(random.randint(0, 255)) for _ in range(4))
