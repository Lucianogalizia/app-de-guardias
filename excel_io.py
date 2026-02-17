import io
import json
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


def _col(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    """
    Devuelve el nombre real de columna por match case-insensitive y sin espacios.
    Acepta múltiples candidatos (alias).
    """
    norm = {str(c).strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = str(cand).strip().lower()
        if key in norm:
            return norm[key]
    return None


def import_maestro_general(excel_bytes: bytes) -> Tuple[List[Dict[str, Any]], List[str]]:
    warnings: List[str] = []
    xls = pd.ExcelFile(io.BytesIO(excel_bytes))

    sheet_name = None
    for s in xls.sheet_names:
        if str(s).strip().lower() == "general":
            sheet_name = s
            break
    if sheet_name is None:
        raise ValueError("No se encontró la pestaña 'General' en el Excel.")

    df = pd.read_excel(xls, sheet_name=sheet_name)
    df.columns = [str(c).strip() for c in df.columns]

    c_legajo = _col(df, "Legajo", "Legajo Clear", "LegajoClear")
    c_cuil = _col(df, "CUIL")
    c_nombre = _col(df, "Nombre y Apellido", "Nombre", "Apellido y Nombre")
    c_leader = _col(df, "leader_legajo", "Lider", "Líder", "Jefe", "leader")

    if not c_legajo or not c_cuil or not c_nombre or not c_leader:
        missing = []
        if not c_legajo: missing.append("Legajo (o Legajo Clear)")
        if not c_cuil: missing.append("CUIL")
        if not c_nombre: missing.append("Nombre y Apellido (o Nombre)")
        if not c_leader: missing.append("leader_legajo (o Lider/Líder/Jefe)")
        raise ValueError("Faltan columnas requeridas en 'General': " + ", ".join(missing))

    c_funcion = _col(df, "FUNCIÓN", "Función", "Funcion")
    c_origen = _col(df, "Origen")
    c_lugar = _col(df, "Lugar de trabajo", "Lugar de Trabajo", "LugarTrabajo", "Lugar")

    rows: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        legajo = str(r[c_legajo]).strip() if pd.notna(r[c_legajo]) else ""
        if not legajo:
            continue

        cuil = str(r[c_cuil]).strip() if pd.notna(r[c_cuil]) else ""
        nombre = str(r[c_nombre]).strip() if pd.notna(r[c_nombre]) else ""
        leader_legajo = str(r[c_leader]).strip() if pd.notna(r[c_leader]) else ""

        if not cuil or not nombre or not leader_legajo:
            warnings.append(f"Legajo {legajo}: faltan datos (CUIL/NOMBRE/LÍDER).")

        extra = {}
        for col in df.columns:
            if col in {c_legajo, c_cuil, c_nombre, c_leader, c_funcion, c_origen, c_lugar}:
                continue
            val = r[col]
            if pd.notna(val):
                extra[str(col)] = val

        rows.append(
            {
                "legajo": legajo,
                "cuil": cuil,
                "nombre": nombre,
                "leader_legajo": leader_legajo,
                "funcion": str(r[c_funcion]).strip() if c_funcion and pd.notna(r[c_funcion]) else None,
                "origen": str(r[c_origen]).strip() if c_origen and pd.notna(r[c_origen]) else None,
                "lugar_trabajo": str(r[c_lugar]).strip() if c_lugar and pd.notna(r[c_lugar]) else None,
                "extra_json": json.dumps(extra, default=str, ensure_ascii=False) if extra else None,
            }
        )

    return rows, warnings


def export_parte_to_excel(
    person_nombre: str,
    legajo: str,
    periodo_yyyymm: str,
    df_mes: pd.DataFrame,
    totales: Dict[str, Any],
) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        detalle = df_mes.copy()
        detalle.insert(0, "Legajo", legajo)
        detalle.insert(1, "Empleado", person_nombre)
        detalle.insert(2, "Periodo", periodo_yyyymm)

        detalle.to_excel(writer, index=False, sheet_name="Detalle")

        resumen = pd.DataFrame(
            [
                {"Métrica": "Días Guardia (G)", "Valor": totales.get("G", 0)},
                {"Métrica": "Días Franco (F)", "Valor": totales.get("F", 0)},
                {"Métrica": "Días Desarraigo (D)", "Valor": totales.get("D", 0)},
                {"Métrica": "Días HomeOffice (HO)", "Valor": totales.get("HO", 0)},
                {"Métrica": "Total Hs Viaje (HV)", "Valor": totales.get("HV", 0.0)},
                {"Métrica": "Total Hs Extra (HE)", "Valor": totales.get("HE", 0.0)},
            ]
        )
        resumen.to_excel(writer, index=False, sheet_name="Resumen")

        workbook = writer.book
        fmt_header = workbook.add_format({"bold": True})
        for sheet in ["Detalle", "Resumen"]:
            ws = writer.sheets[sheet]
            ws.set_row(0, None, fmt_header)
            ws.freeze_panes(1, 0)

        ws = writer.sheets["Detalle"]
        ws.set_column(0, 0, 10)
        ws.set_column(1, 1, 28)
        ws.set_column(2, 2, 10)
        ws.set_column(3, 3, 12)
        ws.set_column(4, 7, 6)
        ws.set_column(8, 9, 10)
        ws.set_column(10, 10, 30)

    return output.getvalue()
