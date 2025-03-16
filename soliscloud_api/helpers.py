"""Helper class

For more information: https://github.com/hultenvp/soliscloud_api
"""
from __future__ import annotations

from soliscloud_api import SoliscloudAPI


class Helpers:

    @staticmethod
    async def get_station_ids(api: SoliscloudAPI, key, secret, nmi=None) -> tuple:
        """
        Parses response from get_station_list and returns all station IDs.
        Handles pagination to ensure all data is retrieved.
        """
        page_no = 1
        stations = ()

        while True:
            response = await api.user_station_list(
                key, secret, page_no=page_no, page_size=100, nmi_code=nmi
            )

            if not response:  # Stop if response is empty
                break

            # Append new station IDs to the tuple
            stations += tuple(int(element['id']) for element in response)

            page_no += 1  # Go to the next page

        return stations

    @staticmethod
    async def get_inverter_ids(api: SoliscloudAPI, key, secret, station_id: int = None, nmi=None) -> tuple:
        """
        Parses response from get_inverter_list and returns all inverter IDs.
        If a station_id is given, then a list of inverters for that station_id is returned.
        Handles pagination to ensure all data is retrieved.
        """
        page_no = 1
        inverters = ()

        while True:
            response = await api.inverter_list(
                key, secret, page_no=page_no, page_size=100,
                station_id=station_id, nmi_code=nmi
            )

            if not response:  # Stop if response is empty
                break

            # Append new inverter IDs to the tuple
            inverters += tuple(int(element['id']) for element in response)

            page_no += 1  # Go to the next page

        return inverters
