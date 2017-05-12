import requests


def get_json_result(address):
    key = "AIzaSyA5urZZyH_X76Pf5Bz0c0qDkOfDLZTRr2k"
    base = "https://maps.googleapis.com/maps/api/geocode/json?"
    params = "address={address},&key={key}".format(
        address=address,
        key=key
    )
    url = "{base}{params}".format(base=base, params=params)
    response = requests.get(url)
    return dict(response.json())