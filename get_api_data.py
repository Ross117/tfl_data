# use poetry?
# could look at using something SQLAlchemy if I end up writing to multiple tables

import os
import json
import datetime
import requests
import snowflake.connector

class TFLData:
    """A class containing methods to get data from the TFL API
    and to write that data to a Snowflake database"""
    
    def get_latest_log_id(self, timestamp: datetime.datetime) -> int:
        """Returns the api_call_log_id linked to the given timestamp.
        If no record is returned or an error occurs when trying to
        fetch the data, returns -1."""
        
        conn = self.get_connection()
        
        api_call_log_id: int = -1
        
        try:
            api_call_log_id = conn.cursor().execute(
                f"""SELECT api_call_log_id
                FROM api_call_log
                WHERE timestamp = '{timestamp}'
                """
            ).fetchone()[0]  
        except Exception as e:
            conn.close()
            print(f'Error when trying to get the api_call_log_id: {e}')
        
        return api_call_log_id
        
    
    def log_data(self, timestamp: datetime.datetime, http_code: int, error_text: str | None, disruption_count: int | None) -> int:
        """Logs metadata about the API call to the api_call_log table.
        If data is successfully inserted, returns the id of the new record,
        otherwise returns -1."""
        
        conn = self.get_connection()
        
        if not error_text:
            error_text = 'null'
            
        if not disruption_count:
            disruption_count = 'null'
        
        # need to protect against SQL injection?
        try:
            conn.cursor().execute(
                f"""INSERT INTO api_call_log (timestamp, http_code, error_text, disruption_count)
                VALUES('{timestamp}', {http_code}, {error_text}, {disruption_count})"""
            )
        except Exception as e:
            print(f'Error when trying to insert log data into the db: {e}')

        conn.close()

        
    def get_api_data(self) -> tuple[list[dict], datetime.datetime, int]:
        """Attempts to fetch disruption data from the API then
        calls two others methods which respectively log & then retrieve 
        metadata about the API call."""

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
                api_call_log_id: int = self.get_latest_log_id(timestamp)
                return data, timestamp, api_call_log_id

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

    def write_data(self, data, timestamp: datetime.datetime, api_call_log_id: int) -> None:
        """Inserts disruption data into the disruption table."""
        
        conn = self.get_connection()

        for msg in data:
            # need to escape apostrophes
            msg_json = json.dumps(msg).replace("'", "\\'")
            try:
                conn.cursor().execute(
                    f"""INSERT INTO disruption(response, time_received, api_call_log_id) 
                    SELECT PARSE_JSON('{msg_json}'), '{timestamp}', {api_call_log_id};"""
                )
            except Exception as e:
                print(f'Error when trying to insert disruption data into the db: {e}')
                conn.close()
                return
                
        conn.close()

        return
