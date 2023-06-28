###################################################################################################################
#
#   "Framework" for pTE Challenge
#
import configparser
import random
import time
from hdbcli import dbapi
import pandas as pd
from sklearn.metrics import r2_score

class pTEFramework:
    ###################################################################################################################

    def connectHANA(self):
        config = configparser.ConfigParser()
        config.read("config.ini")

        conn = dbapi.connect(
            address=config["Connect"]["address"],
            port=config["Connect"]["port"],
            user=config["Connect"]["user"],
            password=config["Connect"]["password"],
        )

        return conn

    ###################################################################################################################

    def loadListOfPartsToBePredicted(self):
        config = configparser.ConfigParser()
        config.read("config.ini")
        num_records = config["parts"]["num_records"]
        part_ids = config["parts"]["part_ids"]
        min_num_movements = config["parts"]["min_num_movements"]
        conn = self.connectHANA()
        query = f"""
SELECT TOP {num_records}
    PART_I_D,
    COUNT(*) AS Anzahl
FROM
    PTE."pa_export"
WHERE
    PART_TYPE = 10
AND
    MVMT_OBJECT_I_D IS NOT NULL
AND
    MVMT_USAGE IS NOT NULL
GROUP BY
    PART_I_D
HAVING
    COUNT(*) >= {min_num_movements}
ORDER BY
    CASE WHEN PART_I_D in {part_ids} THEN 0 ELSE 1 END,
    RAND()
    ;
"""
        parts = pd.read_sql(query, conn)
        return parts

    ###################################################################################################################

    def loadHistoricMovementDataAsBaseForPrediction(self, parts: pd.DataFrame):
        conn = self.connectHANA()
        config = configparser.ConfigParser()
        config.read("config.ini")
        date_min = config["timeline"]["date_min"]
        date_end = config["timeline"]["date_end"]
        parts_list = parts["PART_I_D"].to_list()
        parts_list = str(parts_list).replace("[", "(").replace("]", ")")
        query = f"""
WITH first_agg AS (
	SELECT 
		AVG(MVMT_USAGE) AS MVMT_USAGE, 
		MAX(PART_I_D) AS PART_I_D, 
		process_date, 
		mvmt_object_i_d 
	FROM PTE."pa_export" 
	WHERE 
        PART_I_D IN {parts_list}
	    AND PROCESS_DATE  >= '{date_min}'
	    AND PROCESS_DATE  <= '{date_end}'
	GROUP BY 
		process_date,
		PART_I_D, 
		mvmt_object_i_d) 
SELECT 
	PROCESS_DATE, 
	COALESCE(SUM(MVMT_USAGE), 0) AS part_consumption, 
	PART_I_D 
FROM first_agg 
GROUP BY 
	PROCESS_DATE, 
	PART_I_D
ORDER BY 
    PROCESS_DATE 
;
"""
        movements = pd.read_sql(query, conn)
        return movements

    ###################################################################################################################

    def loadComparissonMovementData(self, parts: pd.DataFrame):
        conn = self.connectHANA()
        config = configparser.ConfigParser()
        config.read("config.ini")
        date_end = config["timeline"]["date_end"]
        date_pred = config["timeline"]["date_pred"]
        parts_list = parts["PART_I_D"].to_list()
        parts_list = str(parts_list).replace("[", "(").replace("]", ")")
        query = f"""
WITH first_agg AS (
	SELECT 
		avg(mvmt_usage) AS mvmt_usage, 
		max(part_i_d) AS PART_I_D, 
		PROCESS_DATE, 
		mvmt_object_i_d 
	FROM PTE."pa_export" 
	WHERE 
        PART_I_D IN {parts_list}
	    AND PROCESS_DATE  > '{date_end}'
        AND PROCESS_DATE <= '{date_pred}'
	GROUP BY 
		PROCESS_DATE,
		PART_I_D, 
		mvmt_object_i_d) 
SELECT 
	PROCESS_DATE, 
	COALESCE(sum(mvmt_usage), 0) AS PART_CONSUMPTION, 
	PART_I_D 
FROM first_agg 
GROUP BY 
	PROCESS_DATE, 
	PART_I_D
ORDER BY 
    PROCESS_DATE 
;        
"""
        data = pd.read_sql(query, conn)

        # List of unique part IDs
        part_numbers = data["PART_I_D"].unique()

        reality_sum = pd.DataFrame(
            columns=["PART_I_D", "REALITY_VALUE_1M", "REALITY_VALUE_2M", "REALITY_VALUE_3M"]
        )

        # iterate over each unique part ID
        for part_number in part_numbers:
            part_data = data[
                data["PART_I_D"] == part_number
            ]  # Filter the data of the current part ID

            # Calculate the sum of the values for the first 30, 60, and 90 days
            date_1m = (pd.Timestamp(date_end) + pd.DateOffset(days=30)).date()
            date_2m = (pd.Timestamp(date_end) + pd.DateOffset(days=60)).date()
            date_3m = (pd.Timestamp(date_end) + pd.DateOffset(days=90)).date()
            sum_1m = part_data[part_data["PROCESS_DATE"] <= date_1m]["PART_CONSUMPTION"].sum()
            sum_2m = part_data[part_data["PROCESS_DATE"] <= date_2m]["PART_CONSUMPTION"].sum()
            sum_3m = part_data[part_data["PROCESS_DATE"] <= date_3m]["PART_CONSUMPTION"].sum()

            # Add the values to the DataFrame reality_sum
            reality_sum_part = pd.DataFrame(
                {
                    "PART_I_D":         [part_number],
                    "REALITY_VALUE_1M": [sum_1m],
                    "REALITY_VALUE_2M": [sum_2m],
                    "REALITY_VALUE_3M": [sum_3m],
                }
            )
            reality_sum = pd.concat([reality_sum,reality_sum_part],ignore_index=True)

        return reality_sum

    ###################################################################################################################

    def loadNEMOPrediction(self, parts: pd.DataFrame):
        conn = self.connectHANA()
        parts_list = parts["PART_I_D"].to_list()
        parts_list = str(parts_list).replace("[", "(").replace("]", ")")
        query = f"""
SELECT 
	GROUP_BY_VALUE AS PART_I_D,
	PREDICTION_1_VALUE AS NEMO_FORECAST_VALUE_1M,
	PREDICTION_2_VALUE AS NEMO_FORECAST_VALUE_2M,
	PREDICTION_3_VALUE AS NEMO_FORECAST_VALUE_3M
FROM 
	PTE.PA_FORECAST_VALIDATION
WHERE 
	GROUP_BY_VALUE IN {parts_list}
    ;        
"""
        data = pd.read_sql(query, conn)
        return data

    ###################################################################################################################

    def fill0valuesForPrediction(self, movements: pd.DataFrame):
        config = configparser.ConfigParser()
        config.read("config.ini")
        date_min = config["timeline"]["date_min"]
        date_end = config["timeline"]["date_end"]

        # Create a DataFrame with all possible combinations of PROCESS_DATE and PART_I_D
        all_dates = pd.date_range(start=date_min, end=date_end, freq="D")
        all_parts = movements["PART_I_D"].unique()
        all_combinations = pd.MultiIndex.from_product(
            [all_dates, all_parts], names=["PROCESS_DATE", "PART_I_D"]
        )
        full_df = pd.DataFrame(index=all_combinations).reset_index()

        # Change the data type of the "PROCESS_DATE" column to the same type in both DataFrames.
        full_df["PROCESS_DATE"] = pd.to_datetime(full_df["PROCESS_DATE"])
        movements["PROCESS_DATE"] = pd.to_datetime(movements["PROCESS_DATE"])

        # Do a left join to expand the original DataFrame
        merged_df = full_df.merge(
            movements, how="left", on=["PROCESS_DATE", "PART_I_D"]
        )

        # Set the missing values in the PART_CONSUMPTION column to 0.
        merged_df["PART_CONSUMPTION"] = merged_df["PART_CONSUMPTION"].fillna(0)

        # return Result DataFrame
        return merged_df

    ###################################################################################################################

    def print_status(self, idx,part_id,parts):
        current_time = time.time()
        total_time = current_time - start_time
        speed = total_time / idx if idx > 0 else 0
        complete = idx / len(parts) * 100

        remaining_parts = len(parts) - idx + 1
        average_time_per_part = speed if remaining_parts == 0 else total_time / (idx + 1)
        estimated_remaining_time = average_time_per_part * remaining_parts
        estimated_end_time = current_time + estimated_remaining_time

        print(time.strftime("%d.%m.%Y %H:%M:%S"),
            ": ",
            idx + 1,
            "/",
            len(parts),
            f"({complete:.2f}%)"
            ": ",
            part_id, 
            f"speed: {speed:.2f} seconds per part",
            "Estimated end time:",
            time.strftime("%d.%m.%Y %H:%M:%S", time.localtime(estimated_end_time)))

    ###################################################################################################################

    def print_results(self, reality: pd.DataFrame, NEMOPrediction : pd.DataFrame, MYPrediction : pd.DataFrame):

        # print a table with 3 main clusters:
        # - reality
        # - NEMO prediction
        # - MY prediction

        dfMerged = pd.merge(reality, NEMOPrediction, on='PART_I_D')
        dfMerged = pd.merge(dfMerged, MYPrediction, on='PART_I_D')
        dfMerged = dfMerged.set_index("PART_I_D")

        print("*"*80)
        print("reality vs. prediction")
        print(dfMerged)
        print("*"*80)

        # calculate R^2 now to see how well we have done

        dfMerged = pd.merge(reality, NEMOPrediction, on='PART_I_D')
        reality_values = dfMerged[['REALITY_VALUE_1M', 'REALITY_VALUE_2M', 'REALITY_VALUE_3M']]
        prediction_values = dfMerged[['NEMO_FORECAST_VALUE_1M', 'NEMO_FORECAST_VALUE_2M', 'NEMO_FORECAST_VALUE_3M']]

        r2NEMO = r2_score(reality_values, prediction_values)

        print("r^2 of NEMO: ", r2NEMO)

        dfMerged = pd.merge(reality, MYPrediction, on='PART_I_D')
        reality_values = dfMerged[['REALITY_VALUE_1M', 'REALITY_VALUE_2M', 'REALITY_VALUE_3M']]
        prediction_values = dfMerged[['MY_FORECAST_VALUE_1M', 'MY_FORECAST_VALUE_2M', 'MY_FORECAST_VALUE_3M']]

        r2ME = r2_score(reality_values, prediction_values)

        print("r^2 of ME:   ", r2ME)

        print("the winner is","NEMO" if r2NEMO>r2ME else "ME!!")
        print("              ****")

    ###################################################################################################################

    def do_the_magic_stuff(self, movements: pd.DataFrame):
        # it's your turn. Have fun!

        # we give a little help...
        part_numbers = movements["PART_I_D"].unique()
        predictions = pd.DataFrame(columns=["PART_I_D","MY_FORECAST_VALUE_1M","MY_FORECAST_VALUE_2M","MY_FORECAST_VALUE_3M"])

        for idx,part_number in enumerate(part_numbers):
            self.print_status(idx,part_number,part_numbers)
            part_data = movements[movements["PART_I_D"] == part_number]
            part_data = part_data.set_index("PROCESS_DATE")
            part_data = part_data.asfreq("D")

            ##################################
            # magic forecasting algorithm here
            forecast = [random.randint(0, 5) for _ in range(90)]
            ##################################

            part_predictions_sum = pd.DataFrame(
                {
                    "PART_I_D": [part_number],
                    "MY_FORECAST_VALUE_1M" : [round(sum(forecast[:30]))],
                    "MY_FORECAST_VALUE_2M" : [round(sum(forecast[:60]))],
                    "MY_FORECAST_VALUE_3M" : [round(sum(forecast[:90]))],
                }
            )
            predictions = pd.concat([predictions, part_predictions_sum], ignore_index=True)
        
        return predictions

###################################################################################################################
# MAIN
###################################################################################################################

if __name__ == "__main__":

    # initialize "framework"
    pf = pTEFramework()

    # get list of parts to be predicted
    parts = pf.loadListOfPartsToBePredicted()

    # get material movements for these parts
    movements = pf.loadHistoricMovementDataAsBaseForPrediction(parts)

    # for time series predictions, we need to fill "gaps" in these movements,
    # we do it for you
    movements_with_0 = pf.fill0valuesForPrediction(movements)

    # export this file as a fallback for other non-coding users
    movements_with_0.to_csv("movements.csv", index=False, encoding="UTF-8", sep=";")

    # get "reality" values for comparison
    reality = pf.loadComparissonMovementData(parts)

    # get NEMO predictions for comparison
    NEMOPrediction = pf.loadNEMOPrediction(parts)

    start_time = time.time()

    ###################################################################################################################
    # NOW WE GO TO THE CHALLENGE
    MYPrediction = pf.do_the_magic_stuff(movements_with_0)
    ###################################################################################################################

    # calculate quality of prediction
    end_time = time.time()
    total_time = end_time - start_time
    print(f"total time: {total_time:.2f} sec")
    print(f"time per part: {total_time / len(parts):.2f} sec")

    pf.print_results(reality=reality,NEMOPrediction=NEMOPrediction,MYPrediction=MYPrediction)

