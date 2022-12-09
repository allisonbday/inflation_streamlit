"""
This class the the superclass that all the other fred requests inherret from
Allison Day - November 18, 2022
"""
import fredapi as fa
import pandas as pd
from datetime import date
import yaml
import pdb


class fredAPI:
    def __init__(
        self,
        yml,
    ):

        # YAML
        self.yml = yml
        with open(yml, "r") as file:
            dataMap = yaml.safe_load(file)
        # collection info
        self._name = dataMap["name"]
        self._frequency = dataMap["frequency"]
        self._geography = dataMap["geography"]
        self._units = dataMap["units"]
        self._seasonally_adjusted = dataMap["seasonally_adjusted"]
        self._national_exists = dataMap["national_exists"]
        self._release = dataMap["release"]
        self.release_filter = ""
        # state info
        self._state_codes = dataMap["state_codes"]  # dictionary with each state & code
        self._state_pulled = dataMap[
            "state_pulled"
        ]  # dictionary with each state & last time it was pulled

        # api
        self.__api_key = "411b80ac425603da23c5346ad9033c1c"
        self.__initialize_fred()

    def __initialize_fred(self):
        self.fred = fa.Fred(api_key=self.__api_key)

    def save_yml(self):
        dataMap = {
            "name": self.get_name(),
            "frequency": self.get_frequency(),
            "geography": self.get_geography(),
            "units": self.get_units(),
            "seasonally_adjusted": self.get_seasonally_adjusted(),
            "national_exists": self.get_national_exists(),
            "state_codes": self._state_codes,
            "state_pulled": self._state_pulled,
            "release": self._release,
        }
        with open(self.yml, "w") as file:
            yaml.dump(dataMap, file)

    # NAME --------------------------------------------------------------------
    # # Can't set name because it is the name of the column in dataframe
    def get_name(self):
        return self._name

    # FREQUENCY ---------------------------------------------------------------
    def get_frequency(self):
        return self._frequency

    def set_frequency(self, freqency):
        self._frequency = freqency

    # GEOGRAPHY ---------------------------------------------------------------
    def get_geography(self):
        return self._geography

    # UNITS -------------------------------------------------------------------
    def get_units(self):
        return self._units

    def set_units(self, units):
        self._units = units

    # SEASONALLY ADJUSTED -----------------------------------------------------
    def get_seasonally_adjusted(self):
        return self._seasonally_adjusted

    def set_seasonally_adjusted(self, seasonally_adjusted: bool):
        self._seasonally_adjusted = seasonally_adjusted

    # NATIONAL EXISTS ---------------------------------------------------------
    def get_national_exists(self):
        return self._national_exists

    # CODES -------------------------------------------------------------------
    def get_state_codes_dict(self):
        return self._state_codes

    def get_state_codes_list(self):
        return list(self._state_codes.values())

    def get_st_code(self, ST):
        return self._state_codes[ST]

    def set_st_code(self, ST, code):
        # codes: list
        self._state_codes.update(ST=code)

    def set_all_st_codes(self, state_codes: dict):
        self._state_codes = state_codes

    # STATES PULLED -----------------------------------------------------------
    def get_state_pulled(self, ST):
        # return self._state_pulled[ST]
        return "01-01-2022"

    def set_state_pulled(self, ST, today):
        self._state_pulled.update({ST: today})

    def reset_pulled(self):
        """resets all codes to being last pulled on 01-01-2011"""
        last_pulled = {}
        for key in self.get_state_codes_dict():
            last_pulled[key] = "11-11-2022"
        self._state_pulled = last_pulled

    # INFO --------------------------------------------------------------------
    def set_from_info(self, df):
        """takes the df from pull_codes_release and sets global variables"""
        # set
        self.set_frequency(df.loc[0].at["frequency"])
        self.set_units(df.loc[0].at["units"])
        self.set_seasonally_adjusted(
            any(
                ele in df.loc[0].at["seasonal_adjustment_short"]
                for ele in ["SA", "SAAR"]
            )
        )
        # make dict
        state_codes = pd.Series(df.id.values, index=df.State).to_dict()
        self.set_all_st_codes(state_codes)
        self.save_yml()

    def pull_codes_release(self):
        """pulls Fred by the release, filters, saves, & returns df"""
        # todo: save release in yml
        dfFred = self.fred.search_by_release(self._release)
        df = dfFred[dfFred.eval(self.release_filter)]
        df.reset_index(inplace=True)

        final_df = self.state_map(df, "id")
        final_df["Name"] = self.get_name()
        final_df = self.sort_info(final_df)
        # set
        self.set_from_info(final_df)

        return final_df

    def get_info(self):
        """returns info stored in the class"""
        info = {
            "Name": self.get_name(),
            "Frequency": self.get_frequency(),
            "Geography": self.get_geography(),
            "Units": self.get_units(),
            "Seasonally Adjusted": self.get_seasonally_adjusted(),
            "National Existed": self.get_national_exists(),
            "State Codes": self.get_state_codes_dict(),
        }
        return info

    def pull_codes_info(self):
        """
        Pulls ALL codes info & includes state
        ! significantly slower than pull_codes_release!!!
        """
        df = (
            pd.DataFrame(
                {
                    series: self.fred.get_series_info(series)
                    for series in self.get_state_codes_list()
                }
            )
            .transpose()
            .reset_index()
            .rename(columns={"index": "code"})
        )
        df = self.state_map(df, "id")  # add state code
        df["Name"] = self.get_name()
        # sort df
        final_df = self.sort_info(df)

        return final_df.sort_values(by="State")  # easier to find what's missing

    def pull_ST_info(self, ST):
        df = (
            pd.DataFrame(self.fred.get_series_info(self.get_st_code(ST)))
            .transpose()
            .reset_index()
            .rename(columns={"index": "code"})
        )
        df["State"] = ST
        df["Name"] = self.get_name()
        # sort df
        cols_to_move = ["State", "Name"]
        df = df[cols_to_move + [x for x in df.columns if x not in cols_to_move]]

        return df

    def sort_info(self, df):
        cols_to_move = [
            "Name",
            "State",
            "title",
            "frequency",
            "units",
            "seasonal_adjustment",
            "last_updated",
            "id",
            "notes",
        ]
        df = df[cols_to_move + [x for x in df.columns if x not in cols_to_move]]
        return df

    # DATA --------------------------------------------------------------------
    def pull_st_data(self, ST):
        """pulls data for that state"""
        column = self.get_name()
        st_code = self.get_st_code(ST)
        last_pulled = self.get_state_pulled(ST)

        # pull
        list_state = []
        state_data = self.fred.get_series(st_code, observation_start=last_pulled)
        for index, value in state_data.items():
            list_state.append([ST, index, value])
        # update last pulled
        today = date.today()
        self.set_state_pulled(ST, today)

        # todo: append data to master df and save
        df = pd.DataFrame(list_state, columns=["State", "Date", column])
        # adding dates
        if len(df):
            # adding dates
            df["Year"] = df["Date"].dt.year
            df["Quarter"] = df["Date"].dt.quarter
            df["Month"] = df["Date"].dt.month
            df.drop(["Date"], axis=1, inplace=True)
        else:
            df.columns = df.columns + ["Year", "Quarter", "Month"]

        return df

    def pull_all_states_data(self):
        """pulls every state"""
        column = self.get_name()
        self.reset_pulled()

        # pull
        list_data = []
        for ST in self.get_state_codes_dict():
            st_code = self.get_st_code(ST)
            last_pulled = self.get_state_pulled(ST)
            state_data = self.fred.get_series(st_code, observation_start=last_pulled)
            for index, value in state_data.items():
                list_data.append([ST, index, value])
            # update last pulled per ST
            today = date.today()
            self.set_state_pulled(ST, today)

        # todo: append data to master df and save
        df = pd.DataFrame(list_data, columns=["State", "Date", column])

        # get national average
        if not self.get_national_exists():
            us_df = df.groupby("Date")[column].mean().reset_index()
            us_df["State"] = "US"
            df = df.append(us_df)

        if len(df):
            # adding dates
            df["Year"] = df["Date"].dt.year
            df["Quarter"] = df["Date"].dt.quarter
            df["Month"] = df["Date"].dt.month
            df.drop(["Date"], axis=1, inplace=True)
        else:
            df.columns = df.columns + ["Year", "Quarter", "Month"]

        return df


# # SUBCLASSES
# unemployment ✅
# constructionWages_df ✅
# ConstructionEmployees_df ✅
# TotalNAICs_df ✅
# TotalHighPropensityNAICs_df  ✅
# RealGDP_df ✅
# HousePriceIdx_df ✅
# NewHousingPermits_df ✅
# BusinessApplications_df ✅
# HighPropensityBusinessApplications_df ✅
# ZillowHomeValue_df ✅
