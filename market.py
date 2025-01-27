import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)

'''Скрипт предназначен для автоматизации процессов управления остатками и ценами товаров на платформе Яндекс Маркет. 
    Он включает в себя функции для получения информации о товарах, их остатках и ценах, а также для обновления этих данных на платформе.
    Скрипт обеспечивает удобный и эффективный способ управления товарными запасами и ценами, минимизируя ручной труд и вероятность ошибок в процессе работы.
'''
def get_product_list(page, campaign_id, access_token):
"""Получает список товаров по заданной кампании из Яндекс Маркета.
    
    Args:
        page (str): Токен страницы для постраничного доступа. Он может быть пустой, чтобы получить первую страницу.
        campaign_id (str): Идентификатор кампании для запроса товаров.
        access_token (str): Токен доступа для авторизации при взаимодействии 
                            с API Яндекс Маркета.

    Returns:
        list: Список объектов товаров, полученных из API. Если товаров нет, 
              то список будет пустым.

    Examples:
        Корректное использование:
        >>> get_product_list('', 'your_campaign_id', 'your_access_token')
        [{'id': '1', 'name': 'Товар 1'}, {'id': '2', 'name': 'Товар 2'}, ...]

        Некорректное использование:
        >>> get_product_list('invalid_page', 'wrong_campaign_id', 'wrong_access_token')
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url: ...
"""
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
"""Обновляет остатки товаров для заданной кампании на Яндекс Маркете.

    Args:
        stocks (list): Список товаров, остатки которых необходимо обновить.
        campaign_id (str): Идентификатор кампании для обновления остатков.
        access_token (str): Токен доступа для авторизации при взаимодействии с API Яндекс Маркета.

    Returns:
        dict: Ответ от API, содержащий информацию об обновлении остатков. 
              Включает детали о выполненных операциях и возможные сообщения об ошибках.

    Examples:
        Корректное использование:
        >>> update_stocks(['sku_1', 'sku_2'], 'your_campaign_id', 'your_access_token')
        {'success': True, 'updated_count': 2}

        Некорректное использование:
        >>> update_stocks(['sku_1', 'sku_2'], 'wrong_campaign_id', 'wrong_access_token')
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url: ...
"""
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
"""Обновляет цены товаров для заданной кампании на Яндекс Маркете.

    Args:
        prices (list): Список объектов, содержащих информацию о ценах 
                       для обновления.
        campaign_id (str): Идентификатор кампании для обновления цен.
        access_token (str): Токен доступа для авторизации при взаимодействии 
                            с API Яндекс Маркета.

    Returns:
        dict: Ответ от API, содержащий информацию об обновлении цен. 
              Включает статус операции и подробности обновленных предложений.

    Examples:
        Корректное использование:
        >>> update_price([{"id": "sku_1", "price": {"value": 1500, "currencyId": "RUR"}}, 
                           {"id": "sku_2", "price": {"value": 2000, "currencyId": "RUR"}}], 
                          'your_campaign_id', 'your_access_token')
        {'success': True, 'updated_count': 2}

        Некорректное использование:
        >>> update_price([{"id": "sku_1", "price": {"value": 1500, "currencyId": "RUR"}}], 
                          'wrong_campaign_id', 'wrong_access_token')
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url: ...
"""
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить артикулы товаров Яндекс маркета

    Args:
        campaign_id (str): Идентификатор рекламной кампании.
        market_token (str): Токен для доступа к API Яндекс Маркета.

    Returns:
        list: Список артикулов товаров, полученных из ответа API.

    Examples:
        >>> offer_ids = get_offer_ids("12345", "abcdef123456")
        >>> print(offer_ids)
        ['sku1', 'sku2', 'sku3']

        >>> offer_ids = get_offer_ids("67890", "ghijkl789012")
        >>> print(offer_ids)

    Raises:
        ValueError: Если данные не могут быть получены или если ответ не содержит ключей.
"""
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
"""Создать список запасов товаров для указанного склада.

    Args:
        watch_remnants (list): Список остатков товаров. Каждый элемент должен содержать
            ключи "Код" и "Количество".
        offer_ids (list): Список идентификаторов товаров, загруженных в Яндекс Маркет.
        warehouse_id (str): Уникальный идентификатор склада.

    Returns:
        list: Список запасов товаров, готовый для загрузки на склад.

    Examples:
        >>> watch_remnants = [{"Код": "sku1", "Количество": "15"}, {"Код": "sku2", "Количество": "1"}]
        >>> offer_ids = ["sku1", "sku3"]
        >>> warehouse_id = "warehouse_123"
        >>> stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
        >>> print(stocks)
        [{'sku': 'sku1', 'warehouseId': 'warehouse_123', 'items': [{'count': 15, 'type': 'FIT', 'updatedAt': '2023-10-10T12:34:56Z'}]},
         {'sku': 'sku3', 'warehouseId': 'warehouse_123', 'items': [{'count': 0, 'type': 'FIT', 'updatedAt': '2023-10-10T12:34:56Z'}]}]

    Raises:
        ValueError: Если формат остатков товаров неверный или отсутствуют необходимые ключи.
"""
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
 """Создать список цен на товары.

     Args:
        watch_remnants (list): Список остатков товаров. Каждый элемент должен содержать
            ключи "Код" и "Цена".
        offer_ids (list): Список идентификаторов товаров, загруженных в Яндекс Маркет.

    Returns:
        list: Список цен на товары.

    Examples:
        >>> watch_remnants = [{"Код": "sku1", "Цена": "1500"}, {"Код": "sku2", "Цена": "2500"}]
        >>> offer_ids = ["sku1", "sku3"]
        >>> prices = create_prices(watch_remnants, offer_ids)
        >>> print(prices)
        [{'id': 'sku1', 'price': {'value': 1500, 'currencyId': 'RUR'}}]

    Raises:
        ValueError: Если формат цены неверен во входных данных.
"""
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
