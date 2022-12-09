# IMPORTS ---------------------------------------------------------------------
import pandas as pd
import yaml
import time
import os

# classes
from fred_resources.fredSubclasses import *
from functools import reduce

# streamlit
import streamlit as st
import plotly.express as px

# YAML ------------------------------------------------------------------------
yml = r"setup.yml"
with open(yml, "r") as file:
    dataMap = yaml.safe_load(file)

class_list = dataMap["class_list"]
state_names = dataMap["state_names"]
full_state_dict = dataMap["full_state_dict"]
columns = dataMap["columns"]

# FUNCTIONS -------------------------------------------------------------------
@st.cache
def read_unintermaster():
    master = pd.read_csv(path + "/final_datasets/uninterpolated_master.csv")
    master = master.drop(["Unnamed: 0"], axis=1)
    return master


@st.cache(suppress_st_warning=True)
def pull_all(cls):
    # # NATIONAL
    # wait = 10
    # dfs = []
    # for cls in class_list:
    #     func = f"{cls}.pull_all_states_data()"
    #     df = eval(func)
    #     dfs.append(df)
    #     time.sleep(wait)
    #     st.write(f"✅ {cls} pulled")
    #     print(f"✅ {cls} pulled")

    # merged_df = reduce(
    #     lambda left, right: pd.merge(
    #         left, right, on=["State", "Year", "Quarter", "Month"], how="outer"
    #     ),
    #     dfs,
    # )

    func = f"{cls}.pull_all_states_data()"
    df = eval(func)
    return df


###############################################################################
# SET UP ----------------------------------------------------------------------
st.set_page_config(page_icon="📊", page_title="Data Analysis", layout="wide")
st.image(
    "https://emojipedia-us.s3.amazonaws.com/source/microsoft-teams/337/bar-chart_1f4ca.png",
    width=100,
)
st.title("Data Analysis")
path = os.path.dirname(__file__)
# INITIALIZE CLASSES ----------------------------------------------------------
Bussiness_Applications = BusinessApplications(
    path + "/fred_resources/fred_yamls/BusinessApplications.yml"
)
Construction_Employees = ConstructionEmployees(
    path + "/fred_resources/fred_yamls/ConstructionEmployees.yml"
)
Construction_Wages = ConstructionWages(
    path + "/fred_resources/fred_yamls/ConstructionWages.yml"
)
HighPropriety_Businesses = HPBusinessApplications(
    path + "/fred_resources/fred_yamls/HPBusinessApplications.yml"
)
House_Price_Index = HousePriceIdx(path + "/fred_resources/fred_yamls/HousePriceIdx.yml")
New_Housing = NewHousingPermits(
    path + "/fred_resources/fred_yamls/NewHousingPermits.yml"
)
Real_GDP = RealGDP(path + "/fred_resources/fred_yamls/RealGDP.yml")
HighPropriety_NAICs = TotalHighPropensityNAICs(
    path + "/fred_resources/fred_yamls/TotalHighPropensityNAICs.yml"
)
NAICs = TotalNAICs(path + "/fred_resources/fred_yamls/TotalNAICs.yml")
Unemployment_Rate = Unemployment(path + "/fred_resources/fred_yamls/unemployment.yml")
Zillow_Home_Value = ZillowHomeValue(
    path + "/fred_resources/fred_yamls/ZillowHomeValue.yml"
)

# load data
uninterMaster = read_unintermaster()

# INPUTS ----------------------------------------------------------------------
cls = st.selectbox("Variable to Pull", options=class_list)

# PULL DATA -------------------------------------------------------------------

pulled_data = pull_all(cls)
col_master = uninterMaster[pulled_data.columns]
col_final = (
    pd.concat([col_master, pulled_data])
    .drop_duplicates()
    .reset_index()
    .drop(columns=["index"])
    .groupby(["State", "Year", "Quarter", "Month"])
    .agg("last")
    .dropna()
    .reset_index()
)

# DOWNLOADS -------------------------------------------------------------------
st.header("Data")
st.dataframe(col_final, use_container_width=True)


@st.cache
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode("utf-8")


col_csv = convert_df(col_final)
st.download_button(
    label="Download CSV",
    data=col_csv,
    file_name=f"{cls}.csv",
    mime="text/csv",
)

# GRAPHS ----------------------------------------------------------------------
st.header("Graphs")

col1, col2 = st.columns(2)
with col1:
    selected_states = st.multiselect(
        "Select Years to Graph",
        col_final["State"].unique(),
        default=["AK", "FL", "NM", "TX", "WA"],
    )

with col2:
    trendline = st.radio(
        "Trendline Options",
        (None, "ols", "lowess", "expanding"),
        horizontal=True,
        help=("""Info [here](https://plotly.com/python/linear-fits/)"""),
    )
    scope = st.radio("Trendline Scope", (None, "state", "overall"), horizontal=True)

nat_graphs_df = col_final[col_final["State"].isin(selected_states)]
nat_graphs_df["Date"] = pd.to_datetime(
    dict(year=nat_graphs_df.Year, month=nat_graphs_df.Month, day="01")
)

col = [
    x
    for x in nat_graphs_df.columns
    if x not in ["State", "Year", "Quarter", "Month", "Date"]
]


if not scope:

    fig = px.scatter(
        nat_graphs_df,
        x="Date",
        y=col[0],
        color="State",
    )
elif scope == "state":
    fig = px.scatter(
        nat_graphs_df,
        x="Date",
        y=col[0],
        color="State",
        trendline=trendline,
    )
else:
    fig = px.scatter(
        nat_graphs_df,
        x="Date",
        y=col[0],
        color="State",
        trendline=trendline,
        trendline_scope=scope,
        trendline_color_override="black",
    )

st.plotly_chart(fig, use_container_width=True)

# CODES -----------------------------------------------------------------------
st.subheader("Sources Pulled")


@st.cache
def master_codes():
    df = pd.read_csv(r"final_datasets\master_codes.csv")
    # df = df.drop(["Unnamed: 0"], axis=1)
    df = df.groupby("Name")[
        "frequency", "units", "seasonal_adjustment", "title", "id"
    ].agg(pd.Series.mode)
    return df


codes_df = master_codes()
st.dataframe(codes_df)
st.caption("*All data taken from [*FRED*](https://fred.stlouisfed.org/)")
