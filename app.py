import calendar
import os
from datetime import date
from typing import Dict, List, Tuple

import pandas as pd
import streamlit as st

import db as dbmod
from auth import resolve_role, verify_login
from excel_io import export_parte_to_excel, import_maestro_general


st.set_page_config(page_title="APP de guardias", page_icon="🗓️", layout="wide")


def cfg(key: str, default=None):
    # Prioridad: ENV -> st.secrets -> default
    if os.getenv(key) is not None:
        return os.getenv(key)
    if key in st.secrets:
        return st.secrets.get(key)
    return default


ADMIN_PASSWORD = cfg("ADMIN_PASSWORD", "")
LEADER_LEGAJOS = cfg("LEADER_LEGAJOS", None)  # lista o None
DB_BACKEND = cfg("DB_BACKEND", "postgres")

# Migrar al inicio
dbmod.migrate(st.secrets)


TIPOS_DIA = ["G", "F", "D", "HO"]
ESTADOS_EDITABLES = {"BORRADOR", "RECHAZADO"}


def yyyymm_from_year_month(y: int, m: int) -> str:
    return f"{y:04d}{m:02d}"


def month_bounds(y: int, m: int) -> Tuple[date, date]:
    last_day = calendar.monthrange(y, m)[1]
    return date(y, m, 1), date(y, m, last_day)


def month_dates(y: int, m: int) -> List[date]:
    start, end = month_bounds(y, m)
    return [date(y, m, d) for d in range(1, end.day + 1)]


def init_session():
    st.session_state.setdefault("user", None)
    st.session_state.setdefault("role", None)
    st.session_state.setdefault("leaders", [])
    st.session_state.setdefault("admin_ok", False)
    st.session_state.setdefault("msg", "")


def set_message(msg: str):
    st.session_state["msg"] = msg


def show_message():
    msg = st.session_state.get("msg") or ""
    if msg:
        st.info(msg)
        st.session_state["msg"] = ""


def resolve_leaders() -> List[str]:
    leaders = cfg("LEADER_LEGAJOS", None)
    if leaders:
        # Streamlit secrets puede devolver list nativa o string; normalizamos
        if isinstance(leaders, str):
            # si lo ponen como "5478,5483,..." en ENV
            parts = [x.strip() for x in leaders.split(",") if x.strip()]
            return parts
        return [str(x).strip() for x in leaders]
    return dbmod.leader_set_in_db(st.secrets)


def ensure_user_loaded():
    st.session_state["leaders"] = resolve_leaders()


def logout():
    st.session_state["user"] = None
    st.session_state["role"] = None
    set_message("Sesión cerrada.")


def build_month_df(y: int, m: int) -> pd.DataFrame:
    dates = month_dates(y, m)
    df = pd.DataFrame({"Fecha": [d.isoformat() for d in dates]})
    for t in TIPOS_DIA:
        df[t] = False
    df["HV"] = 0.0
    df["HE"] = 0.0
    df["Comentario"] = ""
    return df


def items_to_month_df(items_rows, y: int, m: int) -> pd.DataFrame:
    df = build_month_df(y, m)
    if not items_rows:
        return df

    by_date = {}
    for r in items_rows:
        f = r["fecha"]
        by_date.setdefault(f, []).append(r)

    for i, row in df.iterrows():
        f = row["Fecha"]
        lst = by_date.get(f, [])
        flags = {t: False for t in TIPOS_DIA}
        hv = 0.0
        he = 0.0
        comentario = ""
        for it in lst:
            tipo = it["tipo"]
            if tipo in TIPOS_DIA:
                flags[tipo] = True
            elif tipo == "HV":
                hv += float(it["valor_num"] or 0.0)
            elif tipo == "HE":
                he += float(it["valor_num"] or 0.0)
            if it.get("comentario") and not comentario:
                comentario = it["comentario"]

        for t in TIPOS_DIA:
            df.loc[i, t] = bool(flags[t])
        df.loc[i, "HV"] = float(hv)
        df.loc[i, "HE"] = float(he)
        df.loc[i, "Comentario"] = comentario

    return df


def compute_totals(df: pd.DataFrame) -> Dict:
    totals = {}
    for t in TIPOS_DIA:
        totals[t] = int(df[t].fillna(False).astype(bool).sum())
    totals["HV"] = float(pd.to_numeric(df["HV"], errors="coerce").fillna(0).sum())
    totals["HE"] = float(pd.to_numeric(df["HE"], errors="coerce").fillna(0).sum())
    return totals


def save_month_df_as_items(legajo: str, y: int, m: int, df: pd.DataFrame):
    fechas = df["Fecha"].tolist()
    dbmod.delete_items_for_dates(st.secrets, legajo, fechas)

    items = []
    for _, r in df.iterrows():
        f = str(r["Fecha"])
        comentario = (r.get("Comentario") or "").strip() or None

        for t in TIPOS_DIA:
            if bool(r.get(t, False)):
                items.append(
                    {"legajo": legajo, "fecha": f, "tipo": t, "valor_text": "1", "valor_num": None, "comentario": comentario}
                )

        hv = float(pd.to_numeric(r.get("HV", 0), errors="coerce") or 0.0)
        he = float(pd.to_numeric(r.get("HE", 0), errors="coerce") or 0.0)

        if hv > 0:
            items.append({"legajo": legajo, "fecha": f, "tipo": "HV", "valor_text": None, "valor_num": hv, "comentario": comentario})
        if he > 0:
            items.append({"legajo": legajo, "fecha": f, "tipo": "HE", "valor_text": None, "valor_num": he, "comentario": comentario})

    dbmod.insert_items(st.secrets, items)


def can_edit(estado: str) -> bool:
    return estado in ESTADOS_EDITABLES


def ui_totals(tot: Dict):
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Días G", tot.get("G", 0))
    c2.metric("Días F", tot.get("F", 0))
    c3.metric("Días D", tot.get("D", 0))
    c4.metric("Días HO", tot.get("HO", 0))
    c5.metric("Hs Viaje (HV)", f'{tot.get("HV", 0.0):.2f}')
    c6.metric("Hs Extra (HE)", f'{tot.get("HE", 0.0):.2f}')


def page_admin():
    st.title("🔧 Admin")
    if not ADMIN_PASSWORD:
        st.error("Falta configurar ADMIN_PASSWORD (ENV o secrets).")
        return

    if not st.session_state.get("admin_ok"):
        with st.form("admin_login"):
            pwd = st.text_input("Contraseña Admin", type="password")
            ok = st.form_submit_button("Entrar")
        if ok:
            if pwd == ADMIN_PASSWORD:
                st.session_state["admin_ok"] = True
                set_message("Admin habilitado.")
                st.rerun()
            else:
                st.error("Contraseña incorrecta.")
        return

    st.success("Admin habilitado ✅")
    st.caption("Importá el Excel maestro (pestaña 'General') para poblar/actualizar personal (upsert por legajo).")

    uploaded = st.file_uploader("Subir Excel maestro", type=["xlsx", "xls"])
    leaders = resolve_leaders()
    st.caption(f"Líderes configurados/detectados: {', '.join(map(str, leaders)) if leaders else '(vacío)'}")

    if uploaded is not None:
        bytes_data = uploaded.read()
        try:
            rows, warnings = import_maestro_general(bytes_data)
            st.write(f"Filas leídas: **{len(rows)}**")

            if warnings:
                with st.expander("Advertencias"):
                    for w in warnings[:200]:
                        st.warning(w)
                    if len(warnings) > 200:
                        st.info(f"Se omitieron {len(warnings) - 200} advertencias adicionales.")

            # Validación leader_legajo en set de líderes si existe config
            leaders_cfg = cfg("LEADER_LEGAJOS", None)
            leader_set = None
            if leaders_cfg:
                if isinstance(leaders_cfg, str):
                    leader_set = set([x.strip() for x in leaders_cfg.split(",") if x.strip()])
                else:
                    leader_set = set(map(str, leaders_cfg))

            invalid = []
            for r in rows:
                ll = str(r.get("leader_legajo", "")).strip()
                if not ll:
                    invalid.append((r.get("legajo"), "leader_legajo vacío"))
                elif leader_set is not None and ll not in leader_set:
                    invalid.append((r.get("legajo"), f"leader_legajo {ll} no pertenece a LEADER_LEGAJOS"))
            if invalid:
                st.error("Hay registros con líder inválido. Corregí el maestro o LEADER_LEGAJOS.")
                st.dataframe(pd.DataFrame(invalid, columns=["Legajo", "Problema"]), use_container_width=True, hide_index=True)
                return

            if st.button("✅ Importar / Actualizar personal", type="primary"):
                ins, upd = dbmod.upsert_personal_rows(st.secrets, rows)
                st.success(f"Import OK. Insertados: {ins} | Actualizados: {upd}")
                st.rerun()

        except Exception as e:
            st.error(f"Error importando: {e}")

    st.divider()
    st.subheader("Personal cargado")
    people = dbmod.list_personal(st.secrets)
    if not people:
        st.info("Todavía no hay personal cargado.")
        return
    st.dataframe(pd.DataFrame(people), use_container_width=True, hide_index=True)


def page_login():
    st.title("🧾 APP de guardias — Login")
    st.caption("Ingresá con **Legajo + CUIL** (completo o últimos 4).")

    if not dbmod.list_personal(st.secrets):
        st.warning("No hay personal cargado aún. Pedí al admin que importe el maestro.")
        return

    with st.form("login_form"):
        c1, c2 = st.columns(2)
        legajo = c1.text_input("Legajo", placeholder="Ej: 5478")
        cuil = c2.text_input("CUIL (completo o últimos 4)", placeholder="Ej: 20359612835 o 2835")
        submitted = st.form_submit_button("Ingresar", type="primary")

    if submitted:
        ok, user, err = verify_login(st.secrets, legajo, cuil)
        if not ok:
            st.error(err)
            return
        ensure_user_loaded()
        role = resolve_role(user["legajo"], st.session_state["leaders"])
        st.session_state["user"] = user
        st.session_state["role"] = role
        set_message(f"Bienvenido/a {user['nombre']} ({role}).")
        st.rerun()


def page_empleado():
    user = st.session_state["user"]
    st.title("🗓️ Carga mensual")
    st.caption(f"Empleado: **{user['nombre']}** — Legajo **{user['legajo']}** — Líder **{user['leader_legajo']}**")

    today = date.today()
    c1, c2, _ = st.columns([1, 1, 2])
    y = c1.selectbox("Año", options=list(range(today.year - 2, today.year + 2)), index=2)
    m = c2.selectbox("Mes", options=list(range(1, 13)), index=today.month - 1)
    periodo = yyyymm_from_year_month(y, m)

    parte = dbmod.get_or_create_parte(st.secrets, user["legajo"], periodo)
    estado = parte["estado"]

    st.write(f"Estado: **{estado}**")
    if estado == "ENVIADO":
        st.info("Este parte está enviado y bloqueado. Esperá aprobación/rechazo.")
    elif estado == "APROBADO":
        st.success("Parte aprobado ✅ (solo lectura).")
    elif estado == "RECHAZADO":
        st.warning(f"Parte rechazado ❌ — Comentario: {parte.get('rejection_comment') or '(sin comentario)'}")

    start, end = month_bounds(y, m)
    items = dbmod.list_items_for_period(st.secrets, user["legajo"], start.isoformat(), end.isoformat())
    df = items_to_month_df(items, y, m)

    editable = can_edit(estado)
    st.subheader("Grilla del mes")

    edited = st.data_editor(
        df,
        use_container_width=True,
        hide_index=True,
        disabled=not editable,
        column_config={
            "Fecha": st.column_config.TextColumn("Fecha", help="YYYY-MM-DD", disabled=True),
            "G": st.column_config.CheckboxColumn("G", help="Guardia"),
            "F": st.column_config.CheckboxColumn("F", help="Franco"),
            "D": st.column_config.CheckboxColumn("D", help="Desarraigo"),
            "HO": st.column_config.CheckboxColumn("HO", help="Home Office"),
            "HV": st.column_config.NumberColumn("HV", help="Horas viaje", min_value=0.0, step=0.5, format="%.2f"),
            "HE": st.column_config.NumberColumn("HE", help="Horas extra", min_value=0.0, step=0.5, format="%.2f"),
            "Comentario": st.column_config.TextColumn("Comentario", help="Opcional"),
        },
        key=f"editor_{user['legajo']}_{periodo}",
    )

    tot = compute_totals(edited)
    st.subheader("Totales")
    ui_totals(tot)

    st.divider()
    cbtn1, cbtn2, _ = st.columns([1, 1, 2])

    if editable:
        if cbtn1.button("💾 Guardar borrador", type="primary"):
            save_month_df_as_items(user["legajo"], y, m, edited)
            dbmod.update_parte_estado(st.secrets, user["legajo"], periodo, "BORRADOR", rejection_comment=None)
            st.success("Guardado.")
            st.rerun()

        if cbtn2.button("📤 Enviar a aprobación"):
            save_month_df_as_items(user["legajo"], y, m, edited)
            dbmod.update_parte_estado(st.secrets, user["legajo"], periodo, "ENVIADO", submitted_at=dbmod.utcnow_str(), rejection_comment=None)
            st.success("Enviado a aprobación. Queda bloqueado.")
            st.rerun()
    else:
        st.caption("Edición deshabilitada por estado del parte.")

    st.divider()
    st.subheader("Exportar a Excel")
    excel_bytes = export_parte_to_excel(
        person_nombre=user["nombre"],
        legajo=user["legajo"],
        periodo_yyyymm=periodo,
        df_mes=edited,
        totales=tot,
    )
    st.download_button(
        "⬇️ Descargar Excel",
        data=excel_bytes,
        file_name=f"parte_{user['legajo']}_{periodo}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def page_lider():
    user = st.session_state["user"]
    st.title("✅ Bandeja de líder")
    st.caption(f"Líder: **{user['nombre']}** — Legajo **{user['legajo']}**")

    pendientes = dbmod.list_pendientes_para_lider(st.secrets, user["legajo"])
    if not pendientes:
        st.info("No hay partes ENVIADAS pendientes.")
        return

    dfp = pd.DataFrame(pendientes)
    st.subheader("Pendientes")
    st.dataframe(dfp, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Abrir parte")
    options = [f"{r['legajo']} | {r['nombre']} | {r['periodo_yyyymm']}" for r in pendientes]
    choice = st.selectbox("Seleccioná", options=options)
    legajo_sel = choice.split("|")[0].strip()
    periodo_sel = choice.split("|")[-1].strip()

    per = dbmod.get_person_by_legajo(st.secrets, legajo_sel)
    if not per or str(per["leader_legajo"]).strip() != str(user["legajo"]).strip():
        st.error("No tenés permisos para ver este parte.")
        return

    y = int(periodo_sel[:4])
    m = int(periodo_sel[4:6])
    start, end = month_bounds(y, m)
    items = dbmod.list_items_for_period(st.secrets, legajo_sel, start.isoformat(), end.isoformat())
    df = items_to_month_df(items, y, m)
    tot = compute_totals(df)

    parte = dbmod.get_parte(st.secrets, legajo_sel, periodo_sel)
    st.write(f"Empleado: **{per['nombre']}** — Estado: **{parte['estado']}** — Enviado: {parte.get('submitted_at') or '-'}")

    st.subheader("Detalle (solo lectura)")
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.subheader("Totales")
    ui_totals(tot)

    st.divider()
    c1, c2, _ = st.columns([1, 1, 2])

    if c1.button("✅ Aprobar", type="primary"):
        dbmod.update_parte_estado(
            st.secrets,
            legajo_sel,
            periodo_sel,
            "APROBADO",
            approved_at=dbmod.utcnow_str(),
            approved_by_legajo=user["legajo"],
            rejection_comment=None,
        )
        st.success("Aprobado.")
        st.rerun()

    with c2:
        with st.popover("❌ Rechazar"):
            comment = st.text_area("Comentario (obligatorio)", placeholder="Motivo del rechazo...")
            if st.button("Confirmar rechazo"):
                if not comment.strip():
                    st.error("El comentario es obligatorio.")
                else:
                    dbmod.update_parte_estado(
                        st.secrets,
                        legajo_sel,
                        periodo_sel,
                        "RECHAZADO",
                        approved_by_legajo=user["legajo"],
                        rejection_comment=comment.strip(),
                    )
                    st.success("Rechazado.")
                    st.rerun()


def main():
    init_session()
    ensure_user_loaded()

    with st.sidebar:
        st.header("🧭 Navegación")
        show_message()

        if st.session_state["user"]:
            u = st.session_state["user"]
            st.write(f"👤 **{u['nombre']}**")
            st.write(f"Legajo: `{u['legajo']}`")
            st.write(f"Rol: `{st.session_state['role']}`")
            st.button("Cerrar sesión", on_click=logout)
        else:
            st.caption("No logueado.")

        st.divider()
        page = st.radio("Ir a", options=["Login", "Empleado", "Líder", "Admin"], index=0)

    if page == "Admin":
        page_admin()
        return

    if st.session_state["user"] is None:
        page_login()
        return

    role = st.session_state["role"]
    if page == "Líder":
        if role != "lider":
            st.warning("Tu usuario no está marcado como líder.")
        else:
            page_lider()
        return

    page_empleado()


if __name__ == "__main__":
    main()
