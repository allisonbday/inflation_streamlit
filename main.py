# start streamlit
import sys
from streamlit.web import cli as stcli

# Defining main function
def main():

    # RUN STREAMLIT ------------------------------------------------------------------------
    # https://stackoverflow.com/questions/62760929/how-can-i-run-a-streamlit-app-from-within-a-python-script
    sys.argv = ["streamlit", "run", "📈_state_inflation.py"]
    sys.exit(stcli.main())


if __name__ == "__main__":
    main()
