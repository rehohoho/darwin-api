# -*- coding: utf-8 -*-

"""
Created on Mon Oct 29 17:24:20 2018
Script: dwx_tickdata_download.py (Python 3)
--
Downloads tick data from the Darwinex tick data server. This code demonstrates
how to download data for one specific date/hour combination, but can be 
extended easily to downloading entire assets over user-specified start/end 
datetime ranges.
Requirements: Your Darwinex FTP credentials.
Result: Dictionary of pandas DataFrame objects by date/hour.
        (columns: float([ask, size]), index: millisecond timestamp)
        
Example code:
    > td = DWX_Tick_Data(dwx_ftp_user='very_secure_username', 
                         dwx_ftp_pass='extremely_secure_password',
                         dwx_ftp_hostname='mystery_ftp.server.com', 
                         dwx_ftp_port=21)
    
    > td._download_hour_(_asset='EURNOK', _date='2018-10-22', _hour='00')
    
    > td._asset_db['EURNOK-2018-10-22-00']
    
                                           ask       size
     2018-10-22 00:00:07.097000+00:00  9.47202  1000000.0
     2018-10-22 00:00:07.449000+00:00  9.47188   750000.0
     2018-10-22 00:01:08.123000+00:00  9.47201   250000.0
     2018-10-22 00:01:10.576000+00:00  9.47202  1000000.0
                                  ...        ...
@author: Darwinex Labs
@twitter: https://twitter.com/darwinexlabs
@web: http://blog.darwinex.com/category/labs
"""

from ftplib import FTP 
from io import BytesIO
import pandas as pd
import gzip

from datetime import datetime, timedelta
import os
import config
import multiprocessing
from itertools import repeat

import logging
from logger import logger_init, worker_init


class DWX_Tick_Data():
    
    def __init__(self, dwx_ftp_user='<insert your Darwinex username>', 
                     dwx_ftp_pass='<insert your Darwinex password>',
                     dwx_ftp_hostname='<insert Darwinex Tick Data FTP host>', 
                     dwx_ftp_port=21):
        
        # Dictionary DB to hold dictionary objects in FX/Hour format
        self._asset_db = {}
        
        self._ftpObj = FTP(dwx_ftp_hostname)                            
        self._ftpObj.login(dwx_ftp_user, dwx_ftp_pass)   

        self._virtual_dl = None
        
    #########################################################################
    # Function: Downloads and stored currency tick data from Darwinex FTP
    #           Server. Object stores data in a dictionary, keys being of the
    #           format: CURRENCYPAIR-YYYY-MM-DD-HH
    #########################################################################
    
    def _download_hour_(self, _dst, _asset='EURUSD', _date='', _hour='22',
                   _ftp_loc_format='{}/{}_ASK_{}_{}.log.gz'):
        
        _file = _ftp_loc_format.format(_asset, _asset, _date, _hour)
        _key = '{}-{}-{}'.format(_asset, _date, _hour)
        
        self._virtual_dl = BytesIO()
        logging.info(F'[INFO] Retrieving file \'{_file}\' from Darwinex Tick Data Server...')
        
        try:
            if not os.path.exists(os.path.dirname(_dst)):
                os.makedirs(os.path.dirname(_dst))

            with gzip.open(_dst, 'wb') as out:
                self._ftpObj.retrbinary("RETR {}".format(_file), out.write)
            
            logging.info(F'[SUCCESS] {_asset} tick data for {_date} (hour {_hour}) saved to {_dst}.')
        
        # Case: if file not found
        except Exception as ex:
            _exstr = "Exception Type {0}. Args:\n{1!r}"
            _msg = _exstr.format(type(ex).__name__, ex.args)
            logging.warning(F'[Error] {_msg}')
            os.remove(_dst)
    
    def _download_and_inspect_hour_(self, _price_type, _asset='EURUSD', _date='', _hour='22'):
        
        _ftp_loc_format='{}/{}_%s_{}_{}.log.gz' % _price_type
        _file = _ftp_loc_format.format(_asset, _asset, _date, _hour)
        _key = '{}-{}-{}-{}'.format(_asset, _price_type, _date, _hour)
        
        self._virtual_dl = BytesIO()
        logging.info(F'[INFO] Retrieving file \'{_file}\' from Darwinex Tick Data Server...')
        
        try:
            self._ftpObj.retrbinary("RETR {}".format(_file), self._virtual_dl.write)
            # print("ftpobj retrieved binary")
            
            self._virtual_dl.seek(0)
            _log = gzip.open(self._virtual_dl)
            # print("gzip opened")
                
            # Get bytes into local DB as list of lists
            tick_data = [line.strip().decode().split(',') for line in _log]
            # print("asset_db updated")
            
            # Construct DataFrame
            _temp = tick_data
            tick_data = pd.DataFrame({'ask': [l[1] for l in _temp],
                                'size': [l[2] for l in _temp]}, 
                                index=[pd.to_datetime(l[0], unit='ms', utc=True) for l in _temp])
            # print("dataframe constructed")
            
            # Sanitize types
            tick_data = tick_data.astype(float)
            logging.info(F'[SUCCESS] {_asset} tick data for {_date} (hour {_hour}) stored in self._asset_db dict object.')
            
            return tick_data
        
        # Case: if file not found
        except Exception as ex:
            _exstr = "Exception Type {0}. Args:\n{1!r}"
            _msg = _exstr.format(type(ex).__name__, ex.args)
            logging.warning(F'[Error] {_msg}')
    
    #########################################################################


def check_if_not_trading_day(date):
    """
    Forex trading stops at Friday 2159 and starts Sunday 2200
    Adds one hour tolerance in case
    """
    return date.weekday() == 4 and date.hour > 22 or \
        date.weekday() == 5 or \
        date.weekday() == 6 and date.hour < 21


def download_tick_data(start_date, end_date, delta, hours, asset):
    
    for i in range(delta.days + 1):
        
        for hour in hours:
            date = start_date + timedelta(days=i, hours=hour)
            if check_if_not_trading_day(date):
                continue
            date = date.date()
            
            ask_path_name = os.path.join(config.TICK_DATA_PATH, asset, 
                '{}-{}-{}-{}.csv'.format(asset, "ASK", date, hour)
            )
            bid_path_name = os.path.join(config.TICK_DATA_PATH, asset, 
                '{}-{}-{}-{}.csv'.format(asset, "BID", date, hour)
            )

            if os.path.exists(ask_path_name) and os.path.exists(bid_path_name):
                logging.info(F'[INFO] Bid and ask tick data for {ask_path_name} already downloaded.')
                continue

            td = DWX_Tick_Data(
                dwx_ftp_user=config.DWX_FTP_USER,
                dwx_ftp_pass=config.DWX_FTP_PASS,
                dwx_ftp_hostname=config.DWX_FTP_HOSTNAME, 
                dwx_ftp_port=21
            )
            ask = td._download_and_inspect_hour_(_asset=asset, _price_type="ASK", _date='%s' %date, _hour='%02d' %hour)
            bid = td._download_and_inspect_hour_(_asset=asset, _price_type="BID", _date='%s' %date, _hour='%02d' %hour)

            if ask is not None:
                dir_name = os.path.dirname(ask_path_name)
                if not os.path.exists(dir_name):
                    os.makedirs(dir_name)
                ask.to_csv(ask_path_name)
                logging.info(F'[INFO] Wrote to {ask_path_name}')
            
            if bid is not None:
                bid.to_csv(bid_path_name)
                logging.info(F'[INFO] Wrote to {bid_path_name}')


if __name__ == "__main__":
    
    start_date = datetime(2021, 5, 21)
    end_date = datetime(2021, 6, 21)
    delta = end_date - start_date
    hours = range(24)
    num_workers = 4

    q_listener, q = logger_init(os.path.join(config.TICK_DATA_PATH, 'download_1m_data.log'))

    pool = multiprocessing.Pool(num_workers, worker_init, [q])
    pool_zip = zip(
        repeat(start_date),
        repeat(end_date),
        repeat(delta),
        repeat(hours),
        config.G8_TICKERS
    )

    # for i in pool_zip:
    #     print(i)
    
    pool.starmap(download_tick_data, pool_zip)
