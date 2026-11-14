
from pathlib import Path
import os
import subprocess
import sys

import streamlit as st

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
TESTS = ROOT / "tests"
DATA = ROOT / "data"
OUTPUT = ROOT / "output"

st.set_page_config(
    page_title="Schema Drift Pipeline",
    page_icon="📊",
    layout="wide",
)

st.title("📊 Parquet Schema Drift Dashboard")
st.caption("Pipeline execution • Schema evolution • Data quality • Recovery")

# Make modules inside src importable.
sys.path.insert(0, str(SRC))

for folder in [DATA, OUTPUT, OUTPUT / "reports", OUTPUT / "logs"]:
    folder.mkdir(parents=True, exist_ok=True)

def run_command(args):
    """Run a project command and return its result."""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(SRC) + os.pathsep + env.get("PYTHONPATH", "")

    try:
        result = subprocess.run(
            [sys.executable] + args,
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=600,
        )
        return {
            "success": result.returncode == 0,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode,
        }
    except subprocess.TimeoutExpired:
        return {
            "success": False,
            "stdout": "",
            "stderr": "Command timed out after 10 minutes.",
            "returncode": -1,
        }
    except Exception as exc:
        return {
            "success": False,
            "stdout": "",
            "stderr": str(exc),
            "returncode": -1,
        }

def show_result(result):
    if result["success"]:
        st.success("Command completed successfully.")
    else:
        st.error(
            f"Command failed. Exit code: {result['returncode']}"
        )

    if result["stdout"]:
        st.subheader("Execution output")
        st.code(result["stdout"])

    if result["stderr"]:
        st.subheader("Errors / warnings")
        st.code(result["stderr"])

# Top-level project status
col1, col2, col3 = st.columns(3)
col1.metric("Source modules", len(list(SRC.glob("*.py"))))
col2.metric("Test files", len(list(TESTS.glob("test_*.py"))))
col3.metric("Report files", len(list((OUTPUT / "reports").glob("*"))))

pipeline_tab, schema_tab, tests_tab, reports_tab = st.tabs([
    "Pipeline Execution",
    "Schema Drift",
    "Edge-case Testing",
    "Reports & Logs",
])

# --------------------------------------------------
# PIPELINE EXECUTION
# --------------------------------------------------
with pipeline_tab:
    st.header("Run the pipeline")

    rows = st.number_input(
        "Sample records to generate",
        min_value=1,
        max_value=1_000_000,
        value=1000,
        step=100,
    )

    sample_dir = st.text_input(
        "Sample output directory",
        value="data/sample",
    )

    input_dir = st.text_input(
        "Input Parquet directory",
        value="data/sample/input",
    )

    customers_file = st.text_input(
        "Customer reference file",
        value="data/sample/customers.parquet",
    )

    if st.button("Generate sample data", type="primary"):
        result = run_command([
            "src/cli.py",
            "generate",
            "--rows", str(rows),
            "--out", sample_dir,
        ])
        show_result(result)

    if st.button("Execute ingestion pipeline", type="primary"):
        result = run_command([
            "src/cli.py",
            "run",
            "--input-dir", input_dir,
            "--customers", customers_file,
        ])
        show_result(result)

    if st.button("Run reconciliation"):
        result = run_command(["src/cli.py", "reconcile"])
        show_result(result)

    if st.button("Run performance benchmark"):
        result = run_command([
            "src/cli.py",
            "benchmark",
            "--rows", str(rows),
        ])
        show_result(result)

# --------------------------------------------------
# SCHEMA DRIFT
# --------------------------------------------------
with schema_tab:
    st.header("Schema evolution monitoring")

    st.markdown("""
    Review the schema changes detected by the pipeline.

    Expected classifications:
    - **Compatible:** approved nullable-column additions or other
      explicitly permitted changes.
    - **Breaking:** missing required columns, incompatible type
      changes, or unapproved changes.
    - **Unchanged:** source schema matches the registered schema.
    """)

    st.info(
        "Connect this section to schema_drift_log and "
        "schema_registry in your metadata database to display "
        "actual detected changes."
    )

    if st.button("Refresh schema reports"):
        files = list((OUTPUT / "reports").glob("*"))
        if files:
            for file in files:
                st.write(file.name)
        else:
            st.warning("No reports found yet.")

# --------------------------------------------------
# EDGE-CASE TESTING
# --------------------------------------------------
with tests_tab:
    st.header("Edge-case test lab")

    test_options = {
        "Run all tests": None,
        "Duplicate records": "duplicate",
        "Missing or null fields": "null",
        "Malformed input": "malformed",
        "Empty batch": "empty",
        "Schema changes": "schema",
        "Late-arriving data": "late",
        "Invalid reference key": "reference",
        "Retry and idempotency": "retry",
    }

    selected_test = st.selectbox(
        "Select a test scenario",
        list(test_options.keys()),
    )

    if st.button("Execute selected test", type="primary"):
        pattern = test_options[selected_test]

        command = [
            "-m", "unittest",
            "discover",
            "-s", "tests",
            "-v",
        ]

        if pattern:
            command.extend(["-k", pattern])

        result = run_command(command)
        show_result(result)

    st.warning(
        "Each scenario must have a corresponding test method "
        "whose name contains the selected keyword. Add those "
        "tests before expecting a scenario-specific result."
    )

# --------------------------------------------------
# REPORTS AND LOGS
# --------------------------------------------------
with reports_tab:
    st.header("Pipeline reports")

    report_dir = OUTPUT / "reports"
    log_dir = OUTPUT / "logs"

    report_files = sorted(
        report_dir.glob("*"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    log_files = sorted(
        log_dir.glob("*"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    st.subheader("Reports")
    if report_files:
        for file in report_files:
            st.write(file.name)
            if file.is_file():
                st.download_button(
                    label=f"Download {file.name}",
                    data=file.read_bytes(),
                    file_name=file.name,
                    key=f"report_{file.name}",
                )
    else:
        st.info("No reports generated yet.")

    st.divider()
    st.subheader("Logs")
    if log_files:
        for file in log_files:
            st.write(file.name)
            if file.is_file():
                st.download_button(
                    label=f"Download {file.name}",
                    data=file.read_bytes(),
                    file_name=file.name,
                    key=f"log_{file.name}",
                )
    else:
        st.info("No log files generated yet.")
