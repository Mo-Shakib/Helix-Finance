"""Streamlit control room for the Helix Finance mini data warehouse."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import text
from sqlalchemy.engine import Engine

from finance_dwh.analytics import load_analytics_queries, run_analytics_query
from finance_dwh.config import get_settings
from finance_dwh.pipeline import run_pipeline
from finance_dwh.schema import (
    ALL_WAREHOUSE_TABLES,
    create_db_engine,
    initialize_database,
)

st.set_page_config(
    page_title="Helix Finance · Mini Data Warehouse",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

S = get_settings()

SOURCE_LABELS = {
    "crm_customers": "CRM customers",
    "product_catalog": "Product catalog",
    "loan_core": "Loan core",
    "payment_ledger": "Payment ledger",
}

TABLE_LABELS = {
    "dim_date": "dim_date · calendar",
    "dim_customer": "dim_customer · customer master",
    "dim_product": "dim_product · lending products",
    "dim_loan": "dim_loan · loan contracts",
    "fact_loan_originations": "fact_loan_originations · loan grain",
    "fact_transactions": "fact_transactions · event grain",
}

TABLE_GRAIN = {
    "dim_date": "One row per calendar day.",
    "dim_customer": "One row per de-duplicated customer.",
    "dim_product": "One row per product code.",
    "dim_loan": "One row per de-duplicated loan contract.",
    "fact_loan_originations": "One row per originated loan.",
    "fact_transactions": "One row per de-duplicated transaction event.",
}

STATUS_CLASS = {"SUCCESS": "ok", "FAILED": "bad", "RUNNING": "warn"}

st.markdown(
    """
    <style>
      .block-container {max-width: 1440px; padding-top: 1.6rem; padding-bottom: 3rem;}
      [data-testid="stMetric"] {
          background: rgba(20, 34, 56, 0.72);
          border: 1px solid rgba(148, 163, 184, 0.18);
          border-radius: 14px;
          padding: 1rem 1rem 0.8rem 1rem;
      }
      [data-testid="stMetricLabel"] {color: #a7b4c8;}
      .hero {
          background: linear-gradient(120deg, #0e2a3b 0%, #112f4d 45%, #1d3b55 100%);
          border: 1px solid rgba(45, 212, 191, 0.20);
          border-radius: 20px;
          padding: 1.65rem 1.8rem 1.45rem 1.8rem;
          margin-bottom: 1.15rem;
          box-shadow: 0 18px 48px rgba(0, 0, 0, 0.18);
      }
      .hero .eyebrow {
          color: #5eead4; font-size: 0.72rem; letter-spacing: 0.14em;
          font-weight: 700; text-transform: uppercase; margin-bottom: 0.5rem;
      }
      .hero h1 {font-size: 2.05rem; line-height: 1.15; margin: 0; color: #f8fafc;}
      .hero h1 span {color: #5eead4;}
      .hero p {color: #b8c7d9; margin: 0.55rem 0 0.25rem 0; max-width: 900px;}
      .badge {
          display: inline-block; border-radius: 999px; padding: 0.2rem 0.65rem;
          font-size: 0.75rem; font-weight: 700; letter-spacing: 0.05em;
      }
      .badge.ok {background: rgba(16, 185, 129, 0.16); color: #6ee7b7; border: 1px solid rgba(16,185,129,.32);}
      .badge.bad {background: rgba(239, 68, 68, 0.16); color: #fca5a5; border: 1px solid rgba(239,68,68,.32);}
      .badge.warn {background: rgba(245, 158, 11, 0.16); color: #fcd34d; border: 1px solid rgba(245,158,11,.32);}
      .muted {color: #94a3b8; font-size: 0.88rem;}
      .section-kicker {color: #5eead4; font-size: 0.72rem; font-weight: 700; letter-spacing: .12em; text-transform: uppercase;}
      div[data-testid="stSidebar"] {border-right: 1px solid rgba(148,163,184,.12);}
      div[data-testid="stExpander"] {border-radius: 12px;}
      .footer-note {color: #7c8da4; font-size: 0.8rem; margin-top: 1rem;}
      [data-testid="stToolbar"], [data-testid="stDecoration"], #MainMenu, footer {display: none !important;}
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource(show_spinner=False)
def _bootstrap(database_url: str) -> Engine:
    engine = create_db_engine(database_url)
    initialize_database(engine)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return engine


def _fetch(engine: Engine, sql: str, params: dict | None = None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


@st.cache_data(ttl=15, show_spinner=False)
def _runs(_engine: Engine) -> pd.DataFrame:
    return _fetch(
        _engine,
        """
        SELECT run_id, started_at, completed_at, status, extracted_rows,
               transformed_rows, rejected_rows, loaded_rows,
               validation_failures, validation_warnings, duration_seconds,
               error_message, run_details
        FROM etl_pipeline_runs
        ORDER BY run_id DESC
        """,
    )


@st.cache_data(ttl=15, show_spinner=False)
def _rejections(_engine: Engine, run_id: int) -> pd.DataFrame:
    return _fetch(
        _engine,
        """
        SELECT rejection_id, source, source_row, reason_code, reason, raw_record
        FROM etl_rejected_rows
        WHERE run_id = :run_id
        ORDER BY source, source_row, rejection_id
        """,
        {"run_id": run_id},
    )


@st.cache_data(ttl=15, show_spinner=False)
def _validations(_engine: Engine, run_id: int) -> pd.DataFrame:
    return _fetch(
        _engine,
        """
        SELECT check_name, category, status, expected_value, actual_value, details
        FROM etl_validation_results
        WHERE run_id = :run_id
        ORDER BY
            CASE status WHEN 'FAIL' THEN 1 WHEN 'WARN' THEN 2 ELSE 3 END,
            category, check_name
        """,
        {"run_id": run_id},
    )


@st.cache_data(ttl=15, show_spinner=False)
def _warehouse_counts(_engine: Engine) -> pd.DataFrame:
    rows = []
    with _engine.connect() as conn:
        for table_name in ALL_WAREHOUSE_TABLES:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            rows.append({"table": table_name, "rows": int(count or 0)})
    return pd.DataFrame(rows)


@st.cache_data(ttl=15, show_spinner=False)
def _table_preview(_engine: Engine, table_name: str, limit: int = 200) -> pd.DataFrame:
    if table_name not in ALL_WAREHOUSE_TABLES:
        raise ValueError("Table is not part of the warehouse.")
    return _fetch(_engine, f"SELECT * FROM {table_name} LIMIT {int(limit)}")


@st.cache_data(ttl=60, show_spinner=False)
def _source_manifest(data_dir: str) -> dict:
    path = Path(data_dir) / "_manifest.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


@st.cache_data(ttl=60, show_spinner=False)
def _analytics_library(path: str):
    return load_analytics_queries(Path(path))


@st.cache_data(ttl=15, show_spinner=False)
def _analytics_result(_engine: Engine, query_name: str):
    library = {
        query.name: query for query in _analytics_library(str(S.analytics_sql_path))
    }
    return run_analytics_query(_engine, library[query_name])


def _as_dict(value) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _fmt_dt(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return str(value)
    if parsed.tzinfo is not None:
        parsed = parsed.tz_convert("UTC")
        return parsed.strftime("%d %b %Y, %H:%M UTC")
    return parsed.strftime("%d %b %Y, %H:%M")


def _run_label(row: pd.Series) -> str:
    return (
        f"Run #{int(row['run_id'])} · {_fmt_dt(row['started_at'])} · "
        f"{str(row['status']).title()}"
    )


def _hero() -> None:
    st.markdown(
        """
        <div class="hero">
          <div class="eyebrow">Portfolio data engineering demo</div>
          <h1>Helix Finance <span>Data Warehouse</span></h1>
          <p>Four intentionally messy operational feeds move through a guarded
          Python ETL pipeline into a validated PostgreSQL star schema — with every
          rejection and validation check observable in this control room.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _flash() -> None:
    flash = st.session_state.pop("_flash", None)
    if not flash:
        return
    kind, message = flash
    {"success": st.success, "error": st.error, "warning": st.warning}.get(
        kind, st.info
    )(message)


def _execute_pipeline(engine: Engine) -> None:
    with st.status("Running ETL pipeline…", expanded=True) as status:
        progress_bar = st.progress(0)

        def progress(message: str, percent: int) -> None:
            progress_bar.progress(percent / 100.0)
            status.write(f"**{percent}%** · {message}")

        try:
            result = run_pipeline(engine=engine, progress=progress)
        except Exception as exc:  # database or configuration failure
            status.update(
                label="Pipeline could not start", state="error", expanded=True
            )
            st.error(f"{type(exc).__name__}: {exc}")
            st.stop()

        if result.status == "SUCCESS":
            status.update(
                label=f"Pipeline completed in {result.duration_seconds:.2f}s",
                state="complete",
                expanded=False,
            )
            st.session_state["_flash"] = (
                "success",
                f"Run #{result.run_id} loaded {result.loaded_rows:,} warehouse rows. "
                f"{result.rejected_rows:,} source rows were quarantined for review.",
            )
            st.session_state["selected_run_id"] = result.run_id
        else:
            status.update(label="Pipeline failed", state="error", expanded=True)
            st.session_state["_flash"] = (
                "error",
                f"Run #{result.run_id} failed: {result.error or 'unknown error'}",
            )
            st.session_state["selected_run_id"] = result.run_id

    st.cache_data.clear()
    st.rerun()


def _render_sidebar(engine: Engine, runs: pd.DataFrame, db_version: str) -> int:
    with st.sidebar:
        st.markdown("### ⚙️ Control room")
        st.caption(
            "The pipeline performs a full, atomic refresh of the star schema. "
            "A failed validation gate rolls the load back."
        )
        if st.button(
            "▶ Run ETL pipeline",
            type="primary",
            width="stretch",
            key="sidebar_run_pipeline",
        ):
            _execute_pipeline(engine)

        st.divider()
        st.markdown("### 🗂️ Source feeds")
        manifest = _source_manifest(str(S.data_dir))
        source_details = manifest.get("sources", {})
        if source_details:
            for filename, details in source_details.items():
                st.caption(f"`{filename}` · {int(details.get('rows', 0)):,} rows")
        else:
            st.caption("Manifest unavailable — generate the raw datasets with the CLI.")
        st.caption(
            f"**{int(manifest.get('totals', {}).get('rows', 0)):,}** raw rows across "
            f"**{int(manifest.get('totals', {}).get('sources', 0))}** sources"
        )

        st.divider()
        st.markdown("### 🐘 Warehouse")
        st.caption(f"Connected · {db_version}")
        st.caption(S.safe_database_url)
        st.caption("PostgreSQL · SQLAlchemy 2.x · psycopg2")

        selected_run_id = int(runs.iloc[0]["run_id"])
        if not runs.empty:
            st.divider()
            st.markdown("### 🕘 Run history")
            options = [int(run_id) for run_id in runs["run_id"].tolist()]
            labels = {int(row["run_id"]): _run_label(row) for _, row in runs.iterrows()}
            existing = st.session_state.get("selected_run_id")
            default_index = options.index(existing) if existing in options else 0
            selected_run_id = st.selectbox(
                "Inspect execution",
                options=options,
                index=default_index,
                format_func=lambda run_id: labels.get(run_id, f"Run #{run_id}"),
                key="selected_run_id",
                label_visibility="collapsed",
            )
        return int(selected_run_id)


def _source_reconciliation(details: dict) -> pd.DataFrame:
    source_counts = details.get("source_counts", {})
    rows = []
    for source, counts in source_counts.items():
        rows.append(
            {
                "source": SOURCE_LABELS.get(source, source.replace("_", " ").title()),
                "extracted": int(counts.get("extracted", 0)),
                "accepted": int(counts.get("accepted", 0)),
                "rejected": int(counts.get("rejected", 0)),
            }
        )
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame["rejection_rate"] = (
            100.0 * frame["rejected"] / frame["extracted"].replace(0, pd.NA)
        ).round(1)
    return frame


def _overview(
    engine: Engine, runs: pd.DataFrame, run: dict, validations: pd.DataFrame
) -> None:
    details = _as_dict(run.get("run_details"))
    run_id = int(run["run_id"])
    status = str(run["status"])
    passed = (
        int((validations["status"] == "PASS").sum()) if not validations.empty else 0
    )
    total_checks = len(validations)
    failed = (
        int((validations["status"] == "FAIL").sum()) if not validations.empty else 0
    )
    warnings = (
        int((validations["status"] == "WARN").sum()) if not validations.empty else 0
    )

    badge_class = STATUS_CLASS.get(status, "warn")
    st.markdown(
        f'<span class="badge {badge_class}">{status}</span>&nbsp;&nbsp;'
        f'<span class="muted">Run #{run_id} · started {_fmt_dt(run["started_at"])} · '
        f"completed {_fmt_dt(run['completed_at'])}</span>",
        unsafe_allow_html=True,
    )
    if run.get("error_message"):
        st.error(str(run["error_message"]))

    kpis = st.columns(5)
    kpis[0].metric(
        "Source rows",
        f"{int(run['extracted_rows']):,}",
        help="Rows extracted from four raw CSV feeds",
    )
    kpis[1].metric(
        "Accepted",
        f"{int(run['transformed_rows']):,}",
        help="Rows retained after cleaning and de-duplication",
    )
    kpis[2].metric(
        "Rejected",
        f"{int(run['rejected_rows']):,}",
        help="Rows quarantined in etl_rejected_rows",
    )
    kpis[3].metric(
        "Warehouse rows",
        f"{int(run['loaded_rows']):,}",
        help="Dimensions + facts written in this run",
    )
    kpis[4].metric(
        "Validations",
        f"{passed}/{total_checks}" if total_checks else "—",
        help="Post-load checks across uniqueness, referential integrity and reconciliation",
    )
    if total_checks:
        kpis[4].caption(f"{failed} failed · {warnings} warnings")

    st.write("")
    left, right = st.columns([1.45, 1], gap="large")
    with left:
        st.markdown("#### Source-to-warehouse reconciliation")
        source_frame = _source_reconciliation(details)
        if source_frame.empty:
            st.info("This run did not persist stage details.")
        else:
            st.dataframe(
                source_frame,
                width="stretch",
                hide_index=True,
                column_config={
                    "source": "Source feed",
                    "extracted": st.column_config.NumberColumn(
                        "Extracted", format="%d"
                    ),
                    "accepted": st.column_config.NumberColumn("Accepted", format="%d"),
                    "rejected": st.column_config.NumberColumn("Rejected", format="%d"),
                    "rejection_rate": st.column_config.ProgressColumn(
                        "Rejection rate",
                        help="Rejected / extracted",
                        format="%.1f%%",
                        min_value=0,
                        max_value=25,
                    ),
                },
            )
            st.caption(
                f"{int(source_frame['extracted'].sum()):,} extracted → "
                f"{int(source_frame['accepted'].sum()):,} accepted → "
                f"{int(source_frame['rejected'].sum()):,} rejected."
            )

        st.markdown("#### Warehouse inventory")
        counts = details.get("warehouse_counts", {})
        if counts:
            count_frame = pd.DataFrame(
                [{"table": table, "rows": int(rows)} for table, rows in counts.items()]
            )
            count_frame["grain"] = count_frame["table"].map(TABLE_GRAIN)
            st.dataframe(
                count_frame,
                width="stretch",
                hide_index=True,
                column_config={
                    "table": "Table",
                    "rows": st.column_config.NumberColumn("Rows", format="%d"),
                    "grain": "Grain",
                },
            )
        else:
            live_counts = _warehouse_counts(engine).rename(
                columns={"table": "table", "rows": "rows"}
            )
            st.dataframe(live_counts, width="stretch", hide_index=True)

    with right:
        st.markdown("#### Validation gate")
        if total_checks:
            ratio = passed / total_checks
            st.progress(ratio, text=f"{passed} of {total_checks} checks passed")
            if failed:
                st.error(
                    f"{failed} critical check(s) failed. The load was rolled back."
                )
            elif warnings:
                st.warning(
                    f"{warnings} data-quality warning(s) were recorded. "
                    "They are visible in the Validation tab."
                )
            else:
                st.success("Every validation check passed.")
            v_counts = (
                validations["status"]
                .value_counts()
                .rename_axis("status")
                .reset_index(name="checks")
            )
            st.dataframe(v_counts, width="stretch", hide_index=True)
        else:
            st.info("No validation results were recorded for this run.")

        st.markdown("#### Rejections by source")
        rejections = _rejections(engine, run_id)
        if rejections.empty:
            st.success("No rows were rejected.")
        else:
            grouped = (
                rejections.groupby("source")
                .size()
                .rename("rejected")
                .reset_index()
                .sort_values("rejected", ascending=False)
            )
            grouped["source"] = grouped["source"].map(
                lambda value: SOURCE_LABELS.get(value, value)
            )
            st.bar_chart(grouped, x="source", y="rejected", color="#2DD4BF", height=230)

        st.markdown("#### Execution timeline")
        timings = _as_dict(details.get("stage_timings"))
        if timings:
            timing_frame = pd.DataFrame(
                [
                    {
                        "stage": key.replace("_", " ").replace(" seconds", ""),
                        "seconds": float(value),
                    }
                    for key, value in timings.items()
                ]
            )
            st.bar_chart(
                timing_frame, x="stage", y="seconds", color="#38BDF8", height=230
            )
        else:
            st.caption("Stage timings unavailable.")

    with st.expander("Run history", expanded=False):
        history = runs.copy().head(12)
        history["started_at"] = history["started_at"].map(_fmt_dt)
        history["duration_seconds"] = history["duration_seconds"].map(
            lambda value: f"{float(value):.2f}s" if pd.notna(value) else "—"
        )
        st.dataframe(
            history[
                [
                    "run_id",
                    "started_at",
                    "status",
                    "extracted_rows",
                    "transformed_rows",
                    "rejected_rows",
                    "loaded_rows",
                    "validation_failures",
                    "duration_seconds",
                ]
            ].rename(
                columns={
                    "run_id": "Run",
                    "started_at": "Started",
                    "status": "Status",
                    "extracted_rows": "Extracted",
                    "transformed_rows": "Accepted",
                    "rejected_rows": "Rejected",
                    "loaded_rows": "Loaded",
                    "validation_failures": "Failures",
                    "duration_seconds": "Duration",
                }
            ),
            width="stretch",
            hide_index=True,
        )


def _quality_tab(engine: Engine, run: dict) -> None:
    run_id = int(run["run_id"])
    rejections = _rejections(engine, run_id)
    st.markdown("### Data-quality quarantine")
    st.caption(
        "Every row that cannot safely enter the warehouse is preserved with its source, "
        "row number, reason, and raw payload. Duplicates keep the latest source record."
    )
    if rejections.empty:
        st.success("This run produced no rejected rows.")
        return

    duplicate_count = int(rejections["reason_code"].str.endswith("_DUPLICATE").sum())
    invalid_count = len(rejections) - duplicate_count
    source_total = max(int(run["extracted_rows"]), 1)
    kpis = st.columns(4)
    kpis[0].metric("Rejected rows", f"{len(rejections):,}")
    kpis[1].metric("Duplicate / superseded", f"{duplicate_count:,}")
    kpis[2].metric("Invalid records", f"{invalid_count:,}")
    kpis[3].metric("Rejection rate", f"{100 * len(rejections) / source_total:.2f}%")

    st.write("")
    left, right = st.columns([1, 1.4], gap="large")
    with left:
        st.markdown("#### Reason mix")
        reason_summary = (
            rejections.groupby("reason_code")
            .size()
            .rename("rows")
            .reset_index()
            .sort_values("rows", ascending=True)
        )
        st.bar_chart(
            reason_summary,
            x="reason_code",
            y="rows",
            color="#F59E0B",
            height=420,
            horizontal=True,
        )
        summary_table = (
            rejections.groupby(["reason_code", "source"])
            .size()
            .rename("count")
            .reset_index()
            .sort_values("count", ascending=False)
        )
        st.dataframe(summary_table, width="stretch", hide_index=True, height=260)

    with right:
        st.markdown("#### Inspect a rejected row")
        options = rejections["rejection_id"].tolist()
        selected = st.selectbox(
            "Quarantined record",
            options=options,
            format_func=lambda value: (
                f"#{int(value)} · "
                f"{rejections.loc[rejections['rejection_id'] == value, 'reason_code'].iloc[0]}"
            ),
            key=f"rejection_inspect_{run_id}",
        )
        row = rejections.loc[rejections["rejection_id"] == selected].iloc[0]
        st.markdown(
            f"**{row['reason_code']}** · `{SOURCE_LABELS.get(row['source'], row['source'])}` "
            f"row {int(row['source_row'])}"
        )
        st.info(str(row["reason"]))
        raw = _as_dict(row["raw_record"])
        st.json(raw if raw else row["raw_record"])

    st.markdown("#### Rejected-row audit")
    display = rejections.copy()
    display["source"] = display["source"].map(
        lambda value: SOURCE_LABELS.get(value, value)
    )
    display["raw_record"] = display["raw_record"].map(
        lambda value: json.dumps(_as_dict(value), ensure_ascii=False)
    )
    display = display.rename(
        columns={
            "rejection_id": "ID",
            "source": "Source",
            "source_row": "Source row",
            "reason_code": "Reason code",
            "reason": "Reason",
            "raw_record": "Raw record",
        }
    )
    st.dataframe(display, width="stretch", hide_index=True, height=340)
    st.download_button(
        "⬇ Download quarantined rows (CSV)",
        data=display.to_csv(index=False).encode("utf-8"),
        file_name=f"rejected_rows_run_{run_id}.csv",
        mime="text/csv",
    )


def _validation_tab(run: dict, validations: pd.DataFrame) -> None:
    st.markdown("### Validation gate")
    st.caption(
        "Uniqueness and referential-integrity failures are blocking. Data-quality "
        "issues are surfaced as warnings so the warehouse remains usable while the "
        "issue is triaged."
    )
    if validations.empty:
        st.warning("No validation results recorded for this run.")
        return

    failed = int((validations["status"] == "FAIL").sum())
    warnings = int((validations["status"] == "WARN").sum())
    passed = int((validations["status"] == "PASS").sum())
    kpis = st.columns(4)
    kpis[0].metric("Passed", f"{passed}")
    kpis[1].metric("Warnings", f"{warnings}")
    kpis[2].metric("Failures", f"{failed}")
    kpis[3].metric("Total checks", f"{len(validations)}")

    if failed:
        st.error(
            "One or more blocking checks failed. The warehouse transaction was "
            "rolled back, leaving the previous snapshot intact."
        )
    elif warnings:
        st.warning(
            "All blocking checks passed; warnings document known source-quality "
            "trade-offs and the remediation policy applied by the pipeline."
        )
    else:
        st.success("All validation checks passed.")

    st.write("")
    mode = st.radio(
        "Show",
        ["Issues only", "All checks"],
        horizontal=True,
        key=f"validation_filter_{int(run['run_id'])}",
        label_visibility="collapsed",
    )
    displayed = validations.copy()
    if mode == "Issues only":
        displayed = displayed[displayed["status"].isin(["FAIL", "WARN"])]
    if displayed.empty:
        st.success("No warnings or failures to display.")
        return
    st.dataframe(
        displayed,
        width="stretch",
        hide_index=True,
        column_config={
            "check_name": "Check",
            "category": "Category",
            "status": "Status",
            "expected_value": "Expected",
            "actual_value": "Actual",
            "details": "What it means",
        },
    )
    st.download_button(
        "⬇ Download validation results (CSV)",
        data=validations.to_csv(index=False).encode("utf-8"),
        file_name=f"validation_results_run_{int(run['run_id'])}.csv",
        mime="text/csv",
    )


def _warehouse_tab(engine: Engine) -> None:
    st.markdown("### Star-schema explorer")
    counts = _warehouse_counts(engine)
    columns = st.columns(6)
    for column, (_, row) in zip(columns, counts.iterrows()):
        column.metric(
            TABLE_LABELS.get(row["table"], row["table"]).split(" · ")[0],
            f"{int(row['rows']):,}",
        )

    st.write("")
    table_name = st.selectbox(
        "Inspect table",
        options=list(ALL_WAREHOUSE_TABLES),
        format_func=lambda value: TABLE_LABELS.get(value, value),
        key="warehouse_table",
    )
    st.caption(TABLE_GRAIN[table_name] + " Preview is limited to 200 rows.")
    preview = _table_preview(engine, table_name)
    st.dataframe(preview, width="stretch", hide_index=True, height=420)
    st.download_button(
        "⬇ Download table preview (CSV)",
        data=preview.to_csv(index=False).encode("utf-8"),
        file_name=f"{table_name}_preview.csv",
        mime="text/csv",
    )


def _render_chart(query_name: str, result: pd.DataFrame) -> None:
    if result.empty:
        st.info("The query returned no rows.")
        return
    if query_name == "portfolio_by_product":
        st.bar_chart(
            result,
            x="product_name",
            y="active_principal",
            color="#2DD4BF",
            height=360,
        )
    elif query_name == "monthly_originations":
        st.line_chart(
            result,
            x="month",
            y="originated_principal",
            color="#38BDF8",
            height=360,
        )
    elif query_name == "transaction_activity":
        chart = result.copy()
        chart["success_rate_pct"] = chart["success_rate_pct"].astype(float)
        st.line_chart(
            chart,
            x="month",
            y="success_rate_pct",
            color="#A78BFA",
            height=360,
        )
    elif query_name == "loan_status_summary":
        st.bar_chart(
            result,
            x="loan_status",
            y="principal",
            color="#F59E0B",
            height=360,
        )
    elif query_name == "segment_performance":
        st.bar_chart(
            result,
            x="segment",
            y="total_principal",
            color="#34D399",
            height=360,
        )
    # top_customers intentionally renders as a ranked table, not a chart.


def _analytics_tab(engine: Engine, runs: pd.DataFrame) -> None:
    st.markdown("### Decision-ready SQL")
    st.caption(
        "Named analytical queries are maintained in sql/analytics_queries.sql and "
        "executed directly against the star schema. Select a report to inspect its "
        "output and source SQL."
    )
    if runs.empty:
        st.info("Run the pipeline to populate the warehouse first.")
        return
    try:
        library = _analytics_library(str(S.analytics_sql_path))
    except Exception as exc:
        st.error(f"Could not load the analytics SQL library: {exc}")
        return

    query_by_title = {query.title: query for query in library}
    selected_title = st.selectbox(
        "Analytical report",
        options=list(query_by_title),
        key="analytics_query",
    )
    query = query_by_title[selected_title]
    st.markdown(f"**{query.description}**")
    try:
        result = _analytics_result(engine, query.name)
    except Exception as exc:
        st.error(f"{type(exc).__name__}: {exc}")
        return

    if query.chart == "table":
        st.dataframe(result, width="stretch", hide_index=True)
    else:
        _render_chart(query.name, result)
        with st.expander("Result set", expanded=False):
            st.dataframe(result, width="stretch", hide_index=True)

    with st.expander("View SQL", expanded=False):
        st.code(query.sql, language="sql")
    st.download_button(
        "⬇ Download result (CSV)",
        data=result.to_csv(index=False).encode("utf-8"),
        file_name=f"{query.name}.csv",
        mime="text/csv",
    )

    with st.expander("All SQL reports in this demo", expanded=False):
        for item in library:
            st.markdown(f"- **{item.title}** — `{item.name}`")


def _dictionary_tab() -> None:
    st.markdown("### Data dictionary")
    path = S.data_dictionary_path
    if not path.exists():
        st.warning(f"Data dictionary not found at {path}.")
        return
    content = path.read_text(encoding="utf-8")
    st.download_button(
        "⬇ Download data dictionary (Markdown)",
        data=content.encode("utf-8"),
        file_name="data_dictionary.md",
        mime="text/markdown",
    )
    st.divider()
    st.markdown(content)


def main() -> None:
    _hero()
    _flash()

    try:
        engine = _bootstrap(S.database_url)
        with engine.connect() as conn:
            version = conn.execute(text("SELECT version()")).scalar()
        db_version = str(version).split(" on ")[0] if version else "PostgreSQL"
    except Exception as exc:
        st.error(
            "Cannot connect to PostgreSQL. Start the database and reload this page."
        )
        st.code(
            "docker compose up -d\n"
            "python -m finance_dwh.generate_data   # only if data/raw is empty\n"
            "python -m finance_dwh.pipeline\n"
            "streamlit run app.py",
            language="bash",
        )
        st.caption(f"Attempted URL: {S.safe_database_url}")
        st.exception(exc)
        st.stop()

    runs = _runs(engine)
    if runs.empty:
        st.info(
            "No pipeline executions yet. Run the ETL from the sidebar, or generate "
            "the demo warehouse from the command line."
        )
        feed_manifest = _source_manifest(str(S.data_dir)).get("sources", {})
        if feed_manifest:
            st.markdown("#### Raw feeds ready for extraction")
            feed_cols = st.columns(4)
            for column, (filename, details) in zip(feed_cols, feed_manifest.items()):
                column.metric(
                    filename.replace(".csv", ""), f"{int(details['rows']):,} rows"
                )
        st.code(
            "python -m finance_dwh.generate_data\n"
            "python -m finance_dwh.pipeline\n"
            "streamlit run app.py",
            language="bash",
        )
        if st.button("▶ Run the first pipeline", type="primary", key="first_pipeline"):
            _execute_pipeline(engine)
        st.stop()

    selected_run_id = _render_sidebar(engine, runs, db_version)
    run_rows = runs.loc[runs["run_id"] == selected_run_id]
    if run_rows.empty:
        selected_run_id = int(runs.iloc[0]["run_id"])
        run_rows = runs.iloc[[0]]
    run = run_rows.iloc[0].to_dict()
    validations = _validations(engine, selected_run_id)

    tabs = st.tabs(
        [
            "Overview",
            "Rejections",
            "Validation",
            "Warehouse",
            "Analytics",
            "Data dictionary",
        ]
    )
    with tabs[0]:
        _overview(engine, runs, run, validations)
    with tabs[1]:
        _quality_tab(engine, run)
    with tabs[2]:
        _validation_tab(run, validations)
    with tabs[3]:
        _warehouse_tab(engine)
    with tabs[4]:
        _analytics_tab(engine, runs)
    with tabs[5]:
        _dictionary_tab()

    st.markdown(
        '<div class="footer-note">Helix Finance is a synthetic portfolio project. '
        "No real customer or financial data is used.</div>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
