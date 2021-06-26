# -*- coding: utf-8 -*-
"""
    Sample code:
    dwx_tick_data_io.py
    --
    @author: Darwinex Labs (www.darwinex.com)
    
    Copyright (c) 2017-2019, Darwinex. All rights reserved.
    
    Licensed under the BSD 3-Clause License, you may not use this file except 
    in compliance with the License. 
    
    You may obtain a copy of the License at:    
    https://opensource.org/licenses/BSD-3-Clause
"""

from pathlib import Path
import os

import pandas as pd
import numpy as np
import gzip
import matplotlib.pyplot as plt

import config

import logging
from logger import single_thread_logger

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class DWX_TICK_DATA_IO():
    
    def __init__(self,
                 _format='{}_{}_{}_{}',
                 _extension='.log.gz',
                 _delimiter=',',
                 _path='<INSERT_PATH_TO_TICK_DATA_GZIPS_HERE>'):
        
        self._format = _format
        self._extension = _extension
        self._delimiter = _delimiter
        self._path = _path
        self._symbol_df = None
        
    ##########################################################################
    
    # Return list of files for BID and ASK each.
    def _find_symbol_files_(self, _symbol,
                                  _date='',
                                  _hour=''):
        logging.info(F'Finding symbol files for \'{_symbol}/*{self._extension}\'')
        
        if _date == '':
            _fs = [filename.name for filename in Path(self._path).glob('{}/*{}'
                                .format(_symbol, self._extension))]
        else:
            if _hour == '':
                _fs = [filename.name for filename in Path(self._path).glob('{}/*{}'
                                    .format(_symbol, self._extension)) 
                                        if _date in filename.name]
                
            else:
                _fs = [filename.name for filename in Path(self._path).glob('{}/*{}'
                                    .format(_symbol, self._extension)) 
                                        if _date in filename.name
                                            and _hour in filename.name]
    
        if len(_fs) > 0:
            logging.info(F'{len(_fs)} files found.')
            
            return (['{}/{}/{}'.format(self._path, _symbol, _f) for _f in _fs if 'BID' in _f], 
                ['{}/{}/{}'.format(self._path, _symbol, _f) for _f in _fs if 'ASK' in _f])
            
        else:
            logging.warning(F'No files found for {_symbol} - {_date} - {_hour}')
            return None, None
    
    ##########################################################################
    
    def _construct_data_(self, _filename):
        
        if _filename.endswith('gz'):
            _df = pd.DataFrame([line.strip().decode().split(self._delimiter) 
                    for line in gzip.open(_filename) if len(line) > 10])
        elif _filename.endswith('csv'):
            _df = pd.read_csv(_filename)
        else:
            raise ("Unknown file type %s" % _filename)
        if 'BID' in _filename:
            _df.columns = ['timestamp','bid_price','bid_size']
        elif 'ASK' in _filename:
            _df.columns = ['timestamp','ask_price','ask_size']
            
        _df.set_index('timestamp', drop=True, inplace=True)
        
        return _df.apply(pd.to_numeric)
    
    ##########################################################################
    
    def _get_symbol_as_dataframe_(self, _symbol,
                                        _date='',
                                        _hour='',
                                        _convert_epochs=True,
                                        _check_integrity=False,
                                        _reindex=['ask_price','bid_price']):
        
        """
        See http://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html
        for .resample() rule / frequency strings.        
        """

        _bid_files, _ask_files = self._find_symbol_files_(_symbol,_date,_hour)
        if _bid_files is None or _ask_files is None:
            logging.warning(F'No files found for \'{_symbol}/*{self._extension}\'')
            return

        logging.info(F'Processing BID ({len(_bid_files)}) / ASK ({len(_ask_files)}) files...')
        
        # BIDS
        if len(_bid_files) != 0:
            _bids = pd.concat([self._construct_data_(_bid_files[i]) 
                for i in range(0, len(_bid_files)) if (
                    print('\rBIDS: {} / {} - {}'
                        .format(i+1,len(_bid_files),_bid_files[i]), end="", flush=True) 
                    or 1==1
            )], axis=0, sort=True)
            print('')
        
        # ASKS
        if len(_ask_files) != 0:
            _asks = pd.concat([self._construct_data_(_ask_files[i]) 
                for i in range(0, len(_ask_files)) if (
                    print('\rASKS: {} / {} - {}'
                        .format(i+1,len(_ask_files),_ask_files[i]), end="", flush=True)
                    or 1==1
            )], axis=0, sort=True)
        
        # must ensure that timestamp is ascending and have no gaps before fillna
        _df = _asks.merge(_bids, how='outer', left_index=True, right_index=True, copy=False).fillna(method='ffill').dropna()
        
        # Convert timestamps?
        if _convert_epochs:
            _df.index = pd.to_datetime(_df.index)
        
        # Reindex to selected columns?
        if len(_reindex) > 0:
            _df = _df.reindex(_reindex, axis=1)
        
        # Check data integrity?
        if _check_integrity:
            
            logging.info('\n\nChecking data integrity..')
            self._integrity_check_(_df, _symbol)
        
        return _df

    ##########################################################################

    @staticmethod
    def _get_resampled_data(_df,
                            _precision='tick',
                            _na_handling=None,
                            _calc_spread=False,
                            _daily_start=22,
                            _symbol_digits=5):

        if _precision != 'tick':
            _df['mid_price'] = round((_df.ask_price + _df.bid_price) / 2, _symbol_digits)
            
            if _precision not in ['B','C','D','W','24H']:
                _resampling_fn = lambda x: x.resample(rule=_precision)
            else:
                _resampling_fn = lambda x: x.resample(rule=_precision, base=_daily_start) #.dropna()

            _resampled = _resampling_fn(_df.mid_price)
            _df_ohlc =_resampled.ohlc() #get ohlc of resampled bins
            _df_volume = _resampled.count().rename('volume') #get volume of resampled bins (number of tick change)

            if _calc_spread:
                _df['spread'] = abs(np.diff(_df[['ask_price','bid_price']]))
                _df_spread = _resampling_fn(_df.spread).mean() #get mean of resampled bins
                _df = _df_ohlc.merge(_df_spread, how='outer', left_index=True, right_index=True, copy=False)

            _df = _df.merge(_df_volume, how='outer', left_index=True, right_index=True, copy=False)

            if _na_handling is not None:
                _df = _na_handling(_df)
        
        return _df
    
    ##########################################################################
    
    def _integrity_check_(self, _df, _symbol):
        """ Requires dataframe to have bid, ask, spread
        """

        if isinstance(_df, pd.DataFrame) == False:
            
            logging.warning('[ERROR] Input must be a Pandas DataFrame')
            
        else:
            
            _diff = _df.index.to_series().diff()
            
            logging.info('\n[TEST #1] Data Frequency Statistics\n--')
            logging.info(_diff.describe())
            
            logging.info('\n[TEST #2] Mode of Gap Distribution\n--')
            logging.info(_diff.value_counts().head(1))
            
            logging.info('\n[TEST #3] Hourly Spread Distribution. This requires more than one hour of data.\n--')
            _df.groupby(_df.index.hour).spread.mean().plot(
                    xticks=range(0,24), 
                    title='Average Spread by Hour (UTC)')
            plt.savefig(os.path.join(self._path, _symbol + '.png'))
            
    ##########################################################################


def append_or_create_csv(_df, csv_path_name):
    dir_name = os.path.dirname(csv_path_name)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    
    if os.path.exists(csv_path_name):
        logging.info(F"{csv_path_name} exists. Appending to old data.")
        old_data = pd.read_csv(csv_path_name, index_col="timestamp")
        logging.info(F"Number of lines before appending {len(old_data)}")
        combined_data = pd.concat([old_data, _df], join="inner") # intersection of columns
        combined_data.index = pd.to_datetime(combined_data.index, utc=True)
        combined_data = combined_data[~combined_data.index.duplicated(keep='first')]
        logging.info(F"Number of lines after appending {len(combined_data)}")
        combined_data.to_csv(csv_path_name)
    else:
        logging.info(F"{csv_path_name} does not exist. Creating new file.")
        _df.to_csv(csv_path_name)


def write_resampled_data(_df, precision, path, calc_spread, na_handling):
    _df = DWX_TICK_DATA_IO._get_resampled_data(
        _df,
        _precision=precision,
        _na_handling=na_handling,
        _calc_spread=calc_spread
    )
    append_or_create_csv(_df, os.path.join(path, asset) + ".csv")    


if __name__ == "__main__":

    single_thread_logger(os.path.join(config.TICK_DATA_PATH, 'process_1m_data.log'))
    # na_handling = lambda x: x.fillna(method='ffill').dropna()
    na_handling = lambda x: x.dropna()
    _io = DWX_TICK_DATA_IO(_path=config.TICK_DATA_PATH,
                           _extension=".csv")
    
    for asset in config.G8_TICKERS:
        for month in range(5, 6):
            _df = _io._get_symbol_as_dataframe_(
                _symbol=asset,
                _date="2021-%02d-"%month,
                _hour="",
                _convert_epochs=True,
                _check_integrity=True, # requires df to have bid, ask, spread
                _reindex=["ask_price", "bid_price", "spread"]
            )
            _df = _df[_df.index >= '2020-5-20'] # only start after 2020 december, inclusive

            if _df is None: continue
            write_resampled_data(
                _df, 
                precision='min', 
                path=config.MINUTE_DATA_PATH, 
                calc_spread=True,
                na_handling=na_handling)
            
            write_resampled_data(
                _df, 
                precision='H', 
                path=config.HOUR_DATA_PATH, 
                calc_spread=True,
                na_handling=na_handling)