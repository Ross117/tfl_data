# use poetry?
# type check

import os
import json
import datetime
import requests
import snowflake.connector

class TFLData:
    """A class containing methods to get data from the TFL API
    and to write that data to a Snwoflake database"""

    def get_api_data(self) -> tuple[list[dict], datetime.datetime]:
        """Fetches data from the API"""

        modes: str = "tube, dlr"
        url: str = f"https://api.tfl.gov.uk/Line/Mode/{modes}/Disruption"
        headers: dict[str] = {"app_key": os.getenv("app_key")}

        try:
            response = requests.get(url, headers=headers, timeout=5)

            if response.status_code == 200:
                data: list[dict] = response.json()
                timestamp = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                return data, timestamp

            # log error if call unsuccessful
            raise Exception(f"API call unsuccessful. Status code {response.status_code}.")

        except:
            raise Exception("Error when trying to make API call")

    def get_connection(self):
        """Tries to connect to Snowflake.
        If successful, returns a connection object.
        If unsuccessful, raises an exception."""

        try:
            conn = snowflake.connector.connect(
                user=os.getenv("db_user"),
                password=os.getenv("db_password"),
                account=os.getenv("db_account"),
                warehouse=os.getenv("db_warehouse"),
                database=os.getenv("db"),
                schema=os.getenv("schema"),
            )
            return conn
        except:
            raise Exception("Failed to connect to the database")

    def write_data(self, data, timestamp) -> None:
        """Writes data to a Snowflake database"""

        conn = self.get_connection()

        # will want to append to this table - create an id field?
        try:
            conn.cursor().execute(
                """CREATE TABLE IF NOT EXISTS
                disruption(
                    response VARIANT, 
                    time_received TIMESTAMP
                );"""
            )
        except Exception as e:
            print(e)
            conn.close()
            return

        for msg in data:
            msg_jsonstr = json.dumps(msg)
            try:
                conn.cursor().execute(
                    f"""INSERT INTO disruption(response, time_received) 
                    SELECT PARSE_JSON('{msg_jsonstr}'), '{timestamp}';"""
                )
            except Exception as e:
                print(e)
                conn.close()
                return
                
        conn.close()

        return

        # https://quickstarts.snowflake.com/guide/getting_started_with_python/index.html?index=..%2F..index#2
        # working with JSON in snowflake: https://docs.snowflake.com/en/user-guide/tutorials/json-basics-tutorial

if __name__ == "__main__":
    tfl_data = TFLData()
    api_data, time_received = tfl_data.get_api_data()
    tfl_data.write_data(api_data, time_received)
