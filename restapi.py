import pandas as pd
import requests
import json, math
import datetime as dt
import threading
import time
import numpy as np


# Function to extract data from url
def data_from_url(urls):
    df = pd.DataFrame()
    url1 = list()
    for url in urls:
        retries = 3
        for attempt in range(retries):
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    print(url)
                    data = pd.DataFrame(json.loads(response.text),
                                        columns=['unix', 'low', 'high', 'open', 'close', 'volume'])
                    data['date'] = pd.to_datetime(data['unix'], unit='s')
                    df = pd.concat([df, data], ignore_index=True)
                    break
                elif response.status_code == 429:
                    print(f'{response.status_code} -- Error in 429 ' + url + '\n')
                    url1.append(url)
                    break
                else:
                    print("Did not receive OK response from Coinbase API:" + url)
                    break
            except ConnectionResetError as e:
                print(f"Connection was reset for {url}: {e}. Retrying...")
                time.sleep(2)  # Retry after waiting for 2 seconds
            except requests.exceptions.RequestException as e:
                print(f"Error on attempt {attempt + 1} for {url}: {e}")
                time.sleep(2)

    if len(url1) > 0:
        data_from_url(url1)

    print('END of urls')
    return df


# Function generated all urls for given currency
def generate_urls_from_symbol_name(range_end, symbol, epoch_startDate, epoch_endDate):
    # List to store all url for single symbol
    urls = []
    for i in range(0, range_end):
        # minutes = 300*interval
        url_endDate1 = int((dt.datetime.fromtimestamp(epoch_startDate) + dt.timedelta(minutes=300 * 5)).timestamp())

        # convert to readable format
        url_startDate = dt.datetime.utcfromtimestamp(epoch_startDate).strftime('%Y-%m-%dT%H:%M:%SZ')
        url_endDate = dt.datetime.utcfromtimestamp(url_endDate1).strftime('%Y-%m-%dT%H:%M:%SZ')

        # below if else to handle edge case
        if epoch_endDate > url_endDate1:
            urls.append(
                f'https://api.exchange.coinbase.com/products/{symbol}/candles?start={url_startDate}&end={url_endDate}&granularity=900')
            epoch_startDate = url_endDate1
        elif epoch_endDate < url_endDate1:
            epoch_endDate = dt.datetime.utcfromtimestamp(epoch_endDate).strftime('%Y-%m-%dT%H:%M:%SZ')
            urls.append(
                f'https://api.exchange.coinbase.com/products/{symbol}/candles?start={url_startDate}&end={epoch_endDate}&granularity=900')
    # return all urls for single crypto_currency , e.g. returns 273 urls for range (6/1/2021 - 3/11/2022)
    return urls


def fetch_daily_data(pair, range_end, epoch_startDate, epoch_endDate, n):
    for p in pair:
        # store all url
        urls = generate_urls_from_symbol_name(range_end, p, epoch_startDate, epoch_endDate)
        # extraxt data from urls
        data = data_from_url(urls)

        # if we failed to get any data, print an error...otherwise write the file
        if data is None:
            print("Did not return any data from Coinbase for this symbol:" + p)
        else:
            data.to_csv(f'C:\\Users\\harsh\\coinbase_data\\Coinbase_{p}_dailydata.csv', index=False)
            # n is worker number
            print(f'Coinbase_{p}_dailydata.csv', n)


if __name__ == "__main__":
    # we set which pair we want to retrieve data for
    # pair = ["ZRX-USD","1INCH-USD","AAVE-USD"]
    pair = ["ETH-USD", "DOGE-USD", "BTC-USD"]
    epoch_startDate = dt.datetime(2024, 12, 5).timestamp()
    epoch_endDate = dt.datetime.today().timestamp()

    # (300*60*interval_in_minutes
    range_end = math.ceil((epoch_endDate - epoch_startDate) / (300 * 300))

    parallel_threads = 3
    # Splitting Pair files for each thread
    parallel_arrays = np.array_split(pair, parallel_threads)
    workers = list()

    start = dt.datetime.now()
    print(start)
    # Process Pair Files parallely
    for k in range(parallel_threads):
        # Implemented MultiThreading to process 3 threads in parallel
        wp = threading.Thread(target=fetch_daily_data, name='worker%d' % (k + 1),
                              args=(list(parallel_arrays[k]), range_end, epoch_startDate, epoch_endDate, k + 1,))
        workers.append(wp)
        wp.start()

    for wp in workers:
        wp.join()

    end = dt.datetime.now()
    # Print total Time to extract Data
    print('END of program')
    print(start, end, end - start)