# use ruff
# use virtual env?
# requirements file?

# refactor to a class

import requests

modes = 'tube, dlr'
url: str = f'https://api.tfl.gov.uk/Line/Mode/{modes}/Disruption'

# will need to be masked
headers: dict[str] = {'app_key': '0eead3de56f042b2a702ef581ce66b59'}

try:
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data: list[dict] = response.json()
        print(data)
    else:
        # log error
        raise Exception('API call unsuccessful')
except:
    raise Exception('Error when trying to make API call')
