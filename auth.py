import re
from typing import Dict, Optional, Tuple, List

from db import get_person_by_legajo


def normalize_digits(s: str) -> str:
    return re.sub(r"\D+", "", (s or "").strip())


def verify_login(secrets, legajo_input: str, cuil_input: str) -> Tuple[bool, Optional[Dict], str]:
    legajo = (legajo_input or "").strip()
    if not legajo:
        return False, None, "Ingresá un legajo."

    person = get_person_by_legajo(secrets, legajo)
    if person is None:
        return False, None, "Legajo inexistente en el maestro."

    cuil_db = normalize_digits(person["cuil"])
    cuil_in = normalize_digits(cuil_input)

    if not cuil_in:
        return False, None, "Ingresá CUIL (completo) o últimos 4."

    if len(cuil_in) <= 4:
        if cuil_db[-len(cuil_in):] != cuil_in:
            return False, None, "CUIL no coincide (últimos dígitos incorrectos)."
    else:
        if cuil_db != cuil_in:
            return False, None, "CUIL no coincide."

    user = {
        "legajo": str(person["legajo"]),
        "nombre": person["nombre"],
        "leader_legajo": str(person["leader_legajo"]),
    }
    return True, user, ""


def resolve_role(user_legajo: str, leaders: List[str]) -> str:
    return "lider" if str(user_legajo).strip() in set(map(str, leaders)) else "empleado"
