import pandas as pd
from fred_resources.fredAPI import fredAPI


# BUSINESS APPLICATIONS -------------------------------------------------------
class BusinessApplications(fredAPI):
    def __init__(
        self,
        yml,
    ):
        super().__init__(yml)
        self.release_filter = "title.str.contains('Business Applications for ') & units.str.match('Number') & ~title.str.contains('High-Propensity|DISCONTINUED|Census|MSA')"

    def state_map(self, df, code_column):
        """How to extract/apply the state name to a dataframe"""
        df["State"] = df[code_column].apply(lambda x: x[-2:])

        return df

    def make_monthly(self, df):
        # calculate sum of values, grouped by week
        if len(df):
            df = (
                df.groupby(["State", "Year", "Quarter", "Month"])
                .agg({self.get_name(): ["mean"]})
                .reset_index()
            )
            df.columns = ["State", "Year", "Quarter", "Month", self.get_name()]

        return df

    def pull_st_data(self, ST):
        df = super().pull_st_data(ST)
        final_df = self.make_monthly(df)

        return final_df

    def pull_all_states_data(self):
        df = super().pull_all_states_data()
        final_df = self.make_monthly(df)

        return final_df


# CONSTRUCTION EMPLOYEES ------------------------------------------------------
class ConstructionEmployees(fredAPI):
    def __init__(
        self,
        yml,
    ):
        super().__init__(yml)
        self.release_filter = "title.str.contains('All Employees: Mining, Logging, and Construction in ') & seasonal_adjustment_short.str.match('SA') & frequency.str.match('M') & ~title.str.contains('MSA|DISCONTINUED|MD|NECTA|City|County|U.S.|NJ|NY|Northern|Puerto Rico')"

    def state_map(self, df, code_column):
        """How to extract/apply the state name to a dataframe"""
        # import states
        states_df = pd.read_csv(r"../../data/raw/states&abreviations.csv")
        # merge
        df["full_state"] = df["title"].apply(lambda x: x.split("in ", 1)[1])
        df = df.merge(states_df, on="full_state")

        return df


# CONSTRUCTION WAGES ----------------------------------------------------------
class ConstructionWages(fredAPI):
    def __init__(
        self,
        yml,
    ):
        super().__init__(yml)
        self.release_filter = "title.str.contains('Construction Wages and Salaries in ') & seasonal_adjustment_short.str.match('SA') & ~title.str.contains('Census|DISCONTINUED|Division|division|Region|Puerto Rico')"

    def state_map(self, df, code_column):
        """How to extract/apply the state name to a dataframe"""
        df["State"] = df[code_column].apply(lambda x: x[:2] if x != "US" else "US")

        return df


# HIGH PROPENSITY BUSINESS APPLICATOINS ---------------------------------------
class HPBusinessApplications(fredAPI):
    def __init__(
        self,
        yml,
    ):
        super().__init__(yml)
        self.release_filter = "title.str.contains('High-Propensity Business Applications for ') & units.str.match('Number') & ~title.str.contains('United States|DISCONTINUED|Census|MSA')"

    def state_map(self, df, code_column):
        """How to extract/apply the state name to a dataframe"""
        df["State"] = df[code_column].apply(lambda x: x[-2:])

        return df

    def make_monthly(self, df):
        # calculate sum of values, grouped by week
        if len(df):
            df = (
                df.groupby(["State", "Year", "Quarter", "Month"])
                .agg({self.get_name(): ["mean"]})
                .reset_index()
            )
            df.columns = ["State", "Year", "Quarter", "Month", self.get_name()]

        return df

    def pull_st_data(self, ST):
        df = super().pull_st_data(ST)
        final_df = self.make_monthly(df)

        return final_df

    def pull_all_states_data(self):
        df = super().pull_all_states_data()
        final_df = self.make_monthly(df)

        return final_df


# HOUSE PRICE INDEX------------------------------------------------------------
class HousePriceIdx(fredAPI):
    def __init__(
        self,
        yml,
    ):
        super().__init__(yml)
        self.release_filter = "title.str.contains('All-Transactions House Price Index for ') & ~frequency_short.str.match('A') & ~title.str.contains('DISCONTINUED|Census|County|MSA|Borough|Municipality|Parish|city|City')"

    def state_map(self, df, code_column):
        """How to extract/apply the state name to a dataframe"""
        df["State"] = df[code_column].apply(lambda x: x[:2])

        return df


# NEW HOUSING PERMITS ---------------------------------------------------------
class NewHousingPermits(fredAPI):
    def __init__(
        self,
        yml,
    ):
        super().__init__(yml)
        self.release_filter = "title.str.contains('New Private Housing Units Authorized by Building Permits for ') & seasonal_adjustment_short.str.match('SA') & ~title.str.contains('DISCONTINUED|Census|MSA')"

    def state_map(self, df, code_column):
        """How to extract/apply the state name to a dataframe"""
        df["State"] = df[code_column].apply(lambda x: x[:2])

        return df


# REAL GDP --------------------------------------------------------------------
class RealGDP(fredAPI):
    def __init__(
        self,
        yml,
    ):
        super().__init__(yml)
        self.release_filter = "title.str.contains('Real Gross Domestic Product: All Industry Total in ') & seasonal_adjustment_short.str.match('SAAR') & ~title.str.contains('DISCONTINUED|Region|United States')"

    def state_map(self, df, code_column):
        """How to extract/apply the state name to a dataframe"""
        df["State"] = df[code_column].apply(lambda x: x[:2])

        return df


# TOTAL HIGH PROPENSITY NAICS -------------------------------------------------
class TotalHighPropensityNAICs(fredAPI):
    def __init__(
        self,
        yml,
    ):
        super().__init__(yml)
        self.release_filter = "title.str.contains('High-Propensity Business Applications: Total for All NAICS in') & seasonal_adjustment_short.str.match('SA') & ~title.str.contains('DISCONTINUED|Region|United States')"

    def state_map(self, df, code_column):
        """How to extract/apply the state name to a dataframe"""
        df["State"] = df[code_column].apply(lambda x: x[-2:])
        df = df.reset_index()

        return df


# TOTAL NAICS -----------------------------------------------------------------
class TotalNAICs(fredAPI):
    def __init__(
        self,
        yml,
    ):
        super().__init__(yml)
        self.release_filter = "title.str.contains('Business Applications: Total for All NAICS in ') & seasonal_adjustment_short.str.match('SA') & ~title.str.contains('High-Propensity|DISCONTINUED|Region|United States')"

    def state_map(self, df, code_column):
        """How to extract/apply the state name to a dataframe"""
        df["State"] = df[code_column].apply(lambda x: x[-2:])
        df = df.reset_index()

        return df


# UNEMPLOYMENT ----------------------------------------------------------------
class Unemployment(fredAPI):
    def __init__(
        self,
        yml,
    ):
        super().__init__(yml)
        self.release_filter = "title.str.contains('Unemployment Rate in ') & seasonal_adjustment_short.str.match('SA') & ~title.str.contains('Census|DISCONTINUED|Division|division|Region|Puerto Rico')"

    def state_map(self, df, code_column):
        """How to extract/apply the state name to a dataframe"""
        df["State"] = df[code_column].apply(lambda x: x[:2] if x != "UNRATE" else "US")

        return df

    def pull_codes_release(self):
        codes = fredAPI.pull_codes_release(self)
        # National variable was not in release, have to extract this way
        nat = self.fred.get_series_info("UNRATE")
        final_df = pd.concat([codes, nat.to_frame().T], ignore_index=True)
        final_df = self.state_map(final_df, "id")
        final_df["Name"] = self.get_name()
        # set
        self.set_from_info(final_df)

        return final_df


# ZILLOW ----------------------------------------------------------------------
class ZillowHomeValue(fredAPI):
    def __init__(
        self,
        yml,
    ):
        super().__init__(yml)
        self.release_filter = "title.str.contains('Zillow')"

    def state_map(self, df, code_column):
        """How to extract/apply the state name to a dataframe"""
        df["State"] = df[code_column].apply(lambda x: x[:2] if x != "US" else "US")

        return df
