# IMPORTS ---------------------------------------------------------------------
import pandas as pd
import yaml
import os

# classes
from fred_resources.fredSubclasses import *

# date
from datetime import date
from dateutil.relativedelta import relativedelta
import calendar

# pull states
from functools import reduce

# ml
import pickle
import xgboost as xgb

# streamlit
import streamlit as st
import plotly.express as px

# SESSION STATES --------------------------------------------------------------
if "key" not in st.session_state:
    st.session_state["full_state"] = "Alabama"
    st.session_state["ST"] = "AL"


def update_session_states(full_state, ST):
    st.session_state["full_state"] = full_state
    st.session_state["ST"] = ST


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
    master = pd.read_csv(r"final_datasets/uninterpolated_master.csv")
    master = master.drop(["Unnamed: 0"], axis=1)
    return master


@st.cache
def pull_state(ST):
    dfs = []
    for cls in class_list:
        func = f"{cls}.pull_st_data('{ST}')"
        df = eval(func)
        dfs.append(df)

    state_df = reduce(
        lambda left, right: pd.merge(
            left, right, on=["State", "Year", "Quarter", "Month"], how="outer"
        ),
        dfs,
    )

    return state_df


def df_interpolate(df):
    # credit: https://towardsdatascience.com/how-to-interpolate-time-series-data-in-apache-spark-and-python-pandas-part-1-pandas-cff54d76a2ea
    # make date column
    df["Date"] = pd.to_datetime(dict(year=df.Year, month=df.Month, day="01"))
    df.index = df["Date"]
    del df["Date"]
    # groupby state
    df_interpol = df.groupby("State").resample("MS").mean()
    ignore_columns = [
        "State",
        "Date",
        "Unnamed: 0",
    ]
    interpolate_columns = [
        col for col in df_interpol.columns if col not in ignore_columns
    ]
    # interpolate
    for col in interpolate_columns:
        df_interpol[col] = df_interpol[col].interpolate()
    df_interpol.reset_index(inplace=True)  # undo grouping

    return df_interpol


def ml_input(df):
    # ! SHOULD WE KEEP MONTH
    inter = df_interpolate(df)
    inter = inter.drop(columns=["Date", "State", "Year", "Quarter", "Month"])
    return inter.iloc[-1:]


###############################################################################
# SET UP ----------------------------------------------------------------------
st.set_page_config(page_icon="📈", page_title="State Inflation", layout="wide")
st.image(
    "https://emojipedia-us.s3.amazonaws.com/source/microsoft-teams/337/chart-increasing_1f4c8.png",
    width=100,
)
st.title("United States Construction Inflation")
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
col1, col2 = st.columns(2)
with col1:
    full_state = st.selectbox(
        "State:",
        options=state_names,
    )
with col2:
    months = st.slider(
        "Months in the future",
        min_value=6,
        max_value=36,
        step=6,
    )
    new_date = date.today() + relativedelta(months=months)

update_session_states(full_state, full_state_dict.get(full_state))


# PULL DATA -------------------------------------------------------------------

ST = st.session_state["ST"]
state_uninter = uninterMaster[uninterMaster["State"] == ST]
state_df = pull_state(ST)
state_master = (
    pd.concat([state_uninter, state_df])
    .drop_duplicates()
    .reset_index()
    .drop(columns=["index"])
    .groupby(["State", "Year", "Quarter", "Month"])
    .agg("last")
    .reset_index()
)

state_inter = df_interpolate(state_master)
# shown in downloads

# ML MODEL --------------------------------------------------------------------

st.subheader("ML Model")


def load_model(months):
    model_path = path + "/models/XGBoost_{0}.pkl".format(months)
    model = pickle.load(open(model_path, "rb"))
    return model


model = load_model(months)

ml_input = ml_input(state_master)

pred = model.predict(ml_input)
prediction = f"{pred[0]:.3f}"

st.metric(
    label=f"**{full_state}, {calendar.month_name[new_date.month]} {new_date.year} Inflation**",
    value=f"{prediction}",
    delta=f"{(pred[0]-100):.1f}%",
    delta_color="inverse",
)

# model["R2"]

st.markdown("#### Feature Importance")
col1, col2 = st.columns(2)
with col1:
    features = ml_input.columns
    plot1 = px.bar(
        x=features,
        y=model.feature_importances_,
        title=f"Important Features For Predicting {months} Months",
        labels={"y": "Importance Value", "x": "Features"},
    )
    st.plotly_chart(plot1, use_container_width=True)
with col2:
    importance = pd.DataFrame(data=features, columns=["Features"])
    importance[f"Importance"] = model.feature_importances_
    st.dataframe(importance, use_container_width=True)

# DOWNLOADS -------------------------------------------------------------------
st.subheader("Data")
st.caption("**data is interpolated*")
st.dataframe(state_inter)


@st.cache
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode("utf-8")


col1, col2 = st.columns(2)
with col1:
    inter_csv = convert_df(state_inter)
    st.download_button(
        label="Download CSV *(interpolated)*",
        data=inter_csv,
        file_name=f"{full_state}_interpolated.csv",
        mime="text/csv",
    )
with col2:
    state_csv = convert_df(state_master)
    st.download_button(
        label="Download CSV *(no interpolation)*",
        data=state_csv,
        file_name=f"{full_state}_uninterpolated.csv",
        mime="text/csv",
    )

# STATE GRAPHS ----------------------------------------------------------------
st.subheader("Graphs")

# inputs
col1, col2 = st.columns(2)
with col1:
    selected_years = st.multiselect(
        "Select Years to Graph",
        state_master["Year"].unique(),
        default=state_master["Year"].unique()[-5:],
    )
with col2:
    graph_columns = st.multiselect(
        "graphs",
        options=columns,
        default=[
            "UnemploymentRate",
            "BusinessApplications",
            "ConstructionEmployees",
        ],
    )

# wrangling
state_master["Date"] = pd.to_datetime(
    dict(year=state_master.Year, month=state_master.Month, day="01")
)
state_master["Month"] = state_master["Date"].apply(lambda x: x.strftime("%B"))
graphs_df = state_master[state_master["Year"].isin(selected_years)]

# graphing
for col in graph_columns:
    st.subheader(f"{col}")

    fig = px.line(
        graphs_df,
        x="Month",
        y=col,
        color="Year",
        title=f"{col} in {full_state}",
    )
    # fig.add_annotation(x=graphs_df.max(axis=1).idxmax(), y=graphs_df.max().max())
    st.plotly_chart(fig, use_container_width=True)

# CODES -----------------------------------------------------------------------
st.subheader("Sources Pulled")


@st.cache
def master_codes():
    df = pd.read_csv(path + "/final_datasets/master_codes.csv")
    # df = df.drop(["Unnamed: 0"], axis=1)
    df = df.groupby("Name")[
        "frequency", "units", "seasonal_adjustment", "title", "id"
    ].agg(pd.Series.mode)
    return df


codes_df = master_codes()
st.dataframe(codes_df)
st.caption("*All data taken from [*FRED*](https://fred.stlouisfed.org/)")
