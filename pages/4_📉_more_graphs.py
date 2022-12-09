# IMPORTS ---------------------------------------------------------------------
import pandas as pd
import os

# streamlit
import streamlit as st

###############################################################################
# SET UP ----------------------------------------------------------------------
st.set_page_config(page_icon="📉", page_title="More Graphs", layout="wide")
st.image(
    "https://emojipedia-us.s3.amazonaws.com/source/microsoft-teams/337/chart-decreasing_1f4c9.png",
    width=100,
)
st.title("Machine Learning Model Explanation")
st.caption("By Asa Carlson and Camilo Hozman")
path = os.path.join(os.path.dirname(__file__), "..")

graph_list = os.listdir(path + "/graphs")

st.write(f"Graphs list: {graph_list}")
for graph in graph_list:
    st.image(path + "/graphs/" + graph)
