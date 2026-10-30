import pandas as pd
import streamlit as st
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

KEYSPACE = "cloud_waste"

st.set_page_config(page_title="Cloud Hardware Efficiency", layout="wide")
st.title("Cloud Data Center Hardware Efficiency")
st.caption("Alibaba Cluster Trace v2017: requested vs. actual resource usage")


@st.cache_resource
def get_session():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect(KEYSPACE)
    session.row_factory = dict_factory
    return session


@st.cache_data(ttl=300)
def load(table: str) -> pd.DataFrame:
    rows = get_session().execute(f"SELECT * FROM {table}")
    return pd.DataFrame(list(rows))


#  Load data
try:
    machines = load("waste_by_machine")
    jobs = load("waste_by_job")
    hourly = load("waste_by_hour")
except Exception as e:
    st.error(f"Could not read from Cassandra: {e}")
    st.info("Check that Cassandra is running on port 9042 and that the "
            "tables were created and populated.")
    st.stop()

# Sidebar controls
st.sidebar.header("Controls")
resource = st.sidebar.radio("Resource", ["CPU", "Memory"])
top_n = st.sidebar.slider("Top N", 5, 50, 15)

waste_col = "total_cpu_waste" if resource == "CPU" else "total_mem_waste"
util_col = "avg_util_cpu" if resource == "CPU" else "avg_util_mem"
unit = "core-seconds" if resource == "CPU" else "normalized mem x seconds"

# Summary numbers 
c1, c2, c3 = st.columns(3)
c1.metric("Machines", f"{len(machines):,}")
c2.metric("Jobs", f"{len(jobs):,}")
c3.metric(f"Total {resource} waste", f"{machines[waste_col].sum():,.0f}")

tab1, tab2, tab3, tab4 = st.tabs(
    ["Top machines", "Top jobs", "Waste over time", "Requested vs. actual"]
)

# 1. Top wasteful machines
with tab1:
    st.subheader(f"Top {top_n} machines by {resource} waste")
    top_m = machines.nlargest(top_n, waste_col)
    st.bar_chart(top_m.set_index("machine_id")[waste_col], y_label=unit)
    st.dataframe(top_m, use_container_width=True)

# 2. Top wasteful jobs
with tab2:
    st.subheader(f"Top {top_n} jobs by {resource} waste")
    top_j = jobs.nlargest(top_n, waste_col)
    st.bar_chart(top_j.set_index("job_id")[waste_col], y_label=unit)
    st.dataframe(top_j, use_container_width=True)

# 3. Waste over time
with tab3:
    st.subheader(f"{resource} waste per hour")
    h = hourly.sort_values(["day", "hour"]).set_index("hour")
    st.line_chart(h[waste_col], y_label=unit, x_label="hours since trace start")

# 4. Requested vs actual
with tab4:
    st.subheader(f"Machine {resource} utilization vs {resource} waste")
    st.write("Do machines hosting the most wasteful instances sit idle in practice? "
             "Points in the lower right (high waste, low utilization) are the "
             "clearest sign of over-provisioning.")
    scatter = machines[[util_col, waste_col]].dropna()
    st.scatter_chart(scatter, x=util_col, y=waste_col,
                     x_label=f"average {resource} utilization",
                     y_label=f"total {resource} waste")
    st.write(f"Correlation: {scatter[util_col].corr(scatter[waste_col]):.3f}")

    if {"cpu_capacity", "mem_capacity"} <= set(machines.columns):
        st.subheader("Machine capacity")
        st.dataframe(
            machines[["machine_id", "cpu_capacity", "mem_capacity",
                      "avg_util_cpu", "avg_util_mem"]],
            use_container_width=True,
        )