from get_api_data import TFLData

if __name__ == '__main__':
    tfl_data = TFLData()
    api_data, time_received, api_call_log_id = tfl_data.get_api_data()
    tfl_data.write_data(api_data, time_received, api_call_log_id)