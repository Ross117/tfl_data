# use poetry?
# type check
# could look at using something SQLAlchemy if I end up writing to multiple tables

import os
import json
import datetime
import requests
import snowflake.connector

class TFLData:
    """A class containing methods to get data from the TFL API
    and to write that data to a Snowflake database"""
    
    def log_data(self, timestamp: datetime.datetime, http_code: int, error_text: str | None, disruption_count: int | None):
        """Logs metadata about the API call to a database table"""
        
        conn = self.get_connection()
        # need to protect against SQL injection?
        
        if not error_text:
            error_text = 'null'
            
        if not disruption_count:
            disruption_count = 'null'
        
        try:
            conn.cursor().execute(
                f"""INSERT INTO api_call_log (timestamp, http_code, error_text, disruption_count)
                VALUES('{timestamp}', {http_code}, {error_text}, {disruption_count})"""
            )
        except Exception as e:
            print(f'Error when trying to insert data into the db: {e}')
        
        conn.close()
        
    def get_api_data(self) -> tuple[list[dict], datetime.datetime]:
        """Fetches data from the API"""

        modes: str = "tube, dlr"
        url: str = f"https://api.tfl.gov.uk/Line/Mode/{modes}/Disruption"
        headers: dict[str] = {"app_key": os.getenv("app_key")}

        try:
            response = requests.get(url, headers=headers, timeout=5)
            # does it matter if this isn't 100% precise?
            timestamp = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')

            if response.status_code == 200:
                data: list[dict] = response.json()
                self.log_data(timestamp, response.status_code, None, len(data))
                return data, timestamp

            # log error if call unsuccessful
            self.log_data(timestamp, response.status_code, response.text, None)
            raise Exception(f"API call unsuccessful. Status code {response.status_code}.")

        except:
            raise Exception("Error when trying to make API call")

    def get_connection(self):
        """Tries to connect to Snowflake.
        If successful, returns a connection object.
        If unsuccessful, raises an exception."""

        try:
            conn = snowflake.connector.connect(
                user=os.getenv("user"),
                password=os.getenv("password"),
                account=os.getenv("account"),
                warehouse=os.getenv("warehouse"),
                database=os.getenv("db"),
                schema=os.getenv("schema"),
                authenticator='username_password_mfa'
            )
            return conn
        except Exception as e:
            raise Exception(f"Error when trying to make db connection: {e}")

    def write_data(self, data, timestamp) -> None:
        """Writes data to a Snowflake database"""

        conn = self.get_connection()

        for msg in data:
            msg_json = json.dumps(msg)
            try:
                conn.cursor().execute(
                    f"""INSERT INTO disruption(response, time_received) 
                    SELECT PARSE_JSON('{msg_json}'), '{timestamp}';"""
                )
            except Exception as e:
                print(f'Error when trying to insert data into the db: {e}')
                conn.close()
                return
                
        conn.close()

        return
