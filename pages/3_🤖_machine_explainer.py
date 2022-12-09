# IMPORTS ---------------------------------------------------------------------
import pandas as pd
import os

# streamlit
import streamlit as st


###############################################################################
# SET UP ----------------------------------------------------------------------
st.set_page_config(page_icon="🤖", page_title="Machine Explainer", layout="wide")
st.image(
    "https://emojipedia-us.s3.amazonaws.com/source/microsoft-teams/337/robot_1f916.png",
    width=100,
)
st.title("Machine Learning Model Explanation")
path = os.path.join(os.path.dirname(__file__), "..")

# IMPORTANCE ------------------------------------------------------------------
st.header("Importance")


@st.cache
def pull_importance():
    df = pd.read_csv(path + "/important_table.csv")
    df = df.drop(columns=["Unnamed: 0"])
    return df


# checking something

importance_df = pull_importance()
st.dataframe(importance_df)

st.markdown(
    """
## What is XGBoost?
XGBoost stands for Extreme Gradient Boosting. It relies on the intuition that the best possible 
next model, when combined with previous models, minimizes the overall prediction error.

## What is Gradient Boosting?
Gradient Boosting: It is an additive and sequential model where trees are grown in sequential
manner which converts weak learners into strong learners by adding weights to the weak learners 
and reduce weights of the strong learners. So each tree learns and boosts from the previous tree grown.

## Why Choose XGBoost?
This type of Machine learning is best for our data set. We have a rectangular small data set 
that an XGBoost model can easily move through and train itself on. We looked at doing a neural
network but after multiple attempts we concluded that we would need much more data in order to 
justify the use of a Neural Network.

## How is the model trained?
We created multiple models to predict 6 months, 12 months, 18 months, 24 months, 30 months, and 36
months into the future. For our model, we looked at the following variables: unemployment, construction wages, 
number of construction employees, business applications, high propensity business applications, GDP, 
house price index, new housing permits, and Zillow home values. 

We took all this data and filtered for just the national values. We then split the national data 
into two different datasets, Train and Test. We threw theTrain dataset in our model to create a model.
From this model, we took the Test dataset and ran our model through it. By doing so we can measure
how well our model is performing. The Metrics data table calculatesr-squared values and mean squared error values.

    """
)

# METRICS ---------------------------------------------------------------------
st.header("Metrics")


@st.cache
def pull_metrics():
    df = pd.read_csv(path + "/metrics_table.csv")
    df = df.drop(columns=["Unnamed: 0"])
    return df


metrics_df = pull_metrics()
st.dataframe(metrics_df)

st.markdown(
    """
    You can write in markdown here
    """
)
