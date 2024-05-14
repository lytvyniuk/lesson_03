from typing import List, Dict, Any, Tuple
from flask import request
import os
import json
import requests

token = os.environ.get('AUTH_TOKEN')

def get_sales(date: str, page: int) -> Tuple[List[Dict[str, Any]], str]:
    """
    Get data from sales API for specified date and page.

    :param date: The date to retrieve data for.
    :param page: The page number.
    :return: A tuple containing the data and an error message (if any).
    """
    response = requests.get(
        url='https://fake-api-vycpfa6oca-uc.a.run.app/sales',
        params={'date': date, 'page': page},
        headers={'Authorization': token},
    )
    if response.status_code == 200:
        return response.json(), None
    else:
        return None, response.json().get('message', 'Unknown error')


def clean_and_fetch_sales(content: Dict[str, Any]) -> None:
    """
    Clean folder contents and fetch sales data for specified date and page.
    """
    content = request.get_json()
    date = content.get('date')
    raw_dir = content.get('raw_dir')

    clean_folder(raw_dir)
    page = 1

    while True:
        sales_data, error_message = get_sales(date, page)
        store_data(sales_data, raw_dir, date, page)
        if error_message:
            break
        if not sales_data:
            break
        page += 1


def clean_folder(raw_dir):
    """
    Clean folder contents for specified date
    """

    if os.path.exists(raw_dir):
        for filename in os.listdir(raw_dir):
            file_path = os.path.join(raw_dir, filename)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                elif os.path.isdir(file_path):
                    clean_folder(file_path)
                    os.rmdir(file_path)
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')

def store_data(data, raw_dir, date, page):
    """
    Store data in folders.

    :param data: The data to store.
    :param raw_dir: The directory to store the data in.
    :param date: The date of the data.
    :param page: The page number of the data.
    """
    os.makedirs(raw_dir, exist_ok=True)

    if not data:
        print("No data to store.")
        return

    file_path = os.path.join(raw_dir, f'sales_{date}_{page}.json')
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)
