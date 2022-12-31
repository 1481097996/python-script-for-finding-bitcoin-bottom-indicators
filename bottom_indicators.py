import requests
import json
import pandas as pd
import xlsxwriter
import simplejson
import matplotlib.pyplot as plt
import seaborn as sns


#API ---------------------------- ---------------------------------------------------
#有时候 request return 404 可能要更新APIkey
#https://cryptoquant.com/settings/api
API_KEY_CryptoQuant = '3N8DbzcGXV984NIfTbquZ4fIre6vC0ce1GOAYKyD'

# https://studio.glassnode.com/settings/api
API_KEY_glassNode = '2C6yuVMWbfBn4HSfnALfyumnLHm'



#request data function ------------------------------------------------------------------------

#这两个是查找相应数据的函数 返回 dataframe
def request_data_from_glassNode(API , URL):
    res = requests.get(URL,
    params={'a': 'BTC', 'api_key': API, 's' :'1354291200'})
    df = pd.read_json(res.text, convert_dates=['t'])
    return df

def request_data_from_cryptoQuant(API , URL):
    response = requests.get(URL, params={"api_key": API_KEY_CryptoQuant})
    df = pd.DataFrame.from_dict(response.json()['result']['data'])
    return df




# process glassNode data ----------------------------------------------------------

#  main logic
#dataframe short as df, 这个就和Excel一样，但是Pyhon的Excel，可以对他进行一些计算，设置一些条件
def process_rcap_hodl_waves(df):
    #这些函数是输入一个df,输出一个处理好的df
    #URL = 'https://api.glassnode.com/v1/metrics/supply/rcap_hodl_waves'

    # rename
    df = df.rename(columns={"v": "balanced_price_usd"})
    #这个是改名加创建一个1—2年的新列
    df["rcap_hodl_waves_1-2year"] = df["o"].apply( lambda x:  x['1y_2y'])

    # calculate diff 计算同一列中，一个值与下一个值的差 并写入新的一列
    df["rcap_hodl_waves_diff"] = df['rcap_hodl_waves_1-2year'].diff()

    # decrease in countinue 5 days == True
    df['signal_rcap_hodl_waves_1-2year'] =  (df['rcap_hodl_waves_diff'].rolling(window=5).max() < 0).to_numpy()

    # drop 掉不用的列
    df = df.drop(columns=['o', 'rcap_hodl_waves_diff'])
    return df

def process_balanced_price_usd(df_balanced_price, df_bit_price):
    
    #URL = 'https://api.glassnode.com/v1/metrics/indicators/balanced_price_usd'
    df_balanced_price = df_balanced_price.rename(columns={"v": "balanced_price_usd"})
   
    df_bit_price = df_bit_price.rename(columns={"v": "coin_price"})

    #这个是根据时间顺序将两个表合并
    df_bit_price= df_bit_price.join(df_balanced_price.set_index('t'),on='t')
    
    # calculate <0 part  diff smaller than 0 == true/ bigger than 0  == false
    def func_max(value01,value02):
        return value01 - value02 < 0
    df_bit_price['balanced_price_signal'] = df_bit_price.apply(lambda x:func_max(x['coin_price'],x['balanced_price_usd']),axis=1) # diff <0 true else diff >0 false
    return df_bit_price


def process_price_hash():
    pd.io.json._json.loads = lambda s, *a, **kw: simplejson.loads(s)
    pd.io.json._json.loads = lambda s, *a, **kw: pd.json_normalize(simplejson.loads(s))



    # 这个就没按上面写，因为有点麻烦，所以这里直接得到request and then transform to df, and handle df data.
    res = requests.get('https://api.glassnode.com/v1/metrics/indicators/hash_ribbon',
    params={'a': 'BTC', 'api_key': API_KEY_glassNode, 's' :'1354291200'})
    df = pd.read_json(res.text, convert_dates=['t'])
    df = df.sort_values(by='t',ascending=True)

    # create diff coloumn
    df['hash_diff'] = df['o.ma30'] - df['o.ma60']

    # shift function
    df['in_diff'] = df['hash_diff'].shift(1) # dataframe
    #df['hash_increase'] =  (df['hash_diff'].rolling(window=2).min() > 0).to_numpy()


    # another request for price data
    res1 = requests.get('https://api.glassnode.com/v1/metrics/market/price_usd_close',
    params={'a': 'BTC', 'api_key': API_KEY_glassNode, 's' :'1354291200'})

    # convert to pandas dataframe
    df1 = pd.read_json(res1.text, convert_dates=['t'])

    df1 = df1.rename(columns={"v": "price"})
    # sma 30
    df1['price_ma_30'] = df1.price.rolling(30).mean()
    # sma 60
    df1['price_ma_60'] = df1.price.rolling(60).mean()
    # calculate diff
    df1['price_diff'] = df1['price_ma_30'] - df1['price_ma_60']

    #shift funciton
    df1['price_diff_1'] = df1['price_diff'].shift(1)
    
    # merge two df
    df = df.join(df1.set_index('t'),on='t')

    #  calculate two indicators by using df.apply funciton
    df['hash_indicator'] = df.apply(lambda x: x['in_diff'] <=0 and x['hash_diff'] >= 0 and x['price_diff'] >=0 ,axis=1)
    df['price_indicator'] = df.apply(lambda x: x['price_diff'] >0 and x['price_diff_1']<=0 and x['hash_diff'] >= 0,axis=1)
    df = df.drop(columns=['in_diff', 'hash_diff','price_diff','price_diff_1'])
    return df
    
    
    

def process_glassNode__indicators():
    print('start to process glass node data')
    # get hodl df
    df_rcap_hodl_waves = process_rcap_hodl_waves(request_data_from_glassNode(API_KEY_glassNode,'https://api.glassnode.com/v1/metrics/supply/rcap_hodl_waves'))

    # get bit_price df
    df_bit = request_data_from_glassNode(API_KEY_glassNode,'https://api.glassnode.com/v1/metrics/market/price_usd_close')

    # get blanced price df
    df_balanced_price_usd = process_balanced_price_usd(request_data_from_glassNode(API_KEY_glassNode,'https://api.glassnode.com/v1/metrics/indicators/balanced_price_usd'),df_bit)

    #get price and hash df
    df_price_hash = process_price_hash()

    # merge these dfs
    df_rcap_hodl_waves = df_rcap_hodl_waves.join(df_balanced_price_usd.set_index('t'),on='t')
    df_rcap_hodl_waves = df_rcap_hodl_waves.join(df_price_hash.set_index('t'),on='t')
    

    # rename and sort by date
    df_rcap_hodl_waves = df_rcap_hodl_waves.rename(columns={"t": "date"})
    df_rcap_hodl_waves = df_rcap_hodl_waves.sort_values(by='date',ascending=False)
    print('head of glass node data:')
    print(df_rcap_hodl_waves.head(5))
    return df_rcap_hodl_waves


# process cryptoQuant data----------------------------------------------------------------------------------------------------------------------------------------------------

# main logic
# same as glassnode, use different funciton to handle the df and return the df with indicator
def process_mvrv(df):
    #URL = "https://api.cryptoquant.com/v1/btc/market-indicator/mvrv?window=day&from=20121130&limit=100000"
    df['mvrv_diff'] = df['mvrv'].diff(-1)
    df['trend'] = (df['mvrv_diff'].rolling(window=2).min() >  0).shift(-1).to_numpy()
    df['closer to 1'] = df['mvrv'].apply(lambda x: abs(x-1) < 0.1)
    df['mvrv_signal'] = df.apply(lambda x: x["closer to 1"] and x['trend'],axis=1)
    #df = df.sort_values(by='date',ascending=True)
    #df['mvrv_signal_range'] = df.mvrv_signal.rolling(8).max()
    #df['mvrv_signal_range'] = df['mvrv_signal_range'].astype(bool)
    df = df.sort_values(by='date',ascending=False)
    df = df.drop(columns=['trend', 'closer to 1'])
    return df

def process_puell_multiple(df):
    #URL = "https://api.cryptoquant.com/v1/btc/network-indicator/puell-multiple?window=day&from=20121130&limit=100000"
    df['puell-multiple_diff'] = df['puell_multiple'].diff(-1)
    df['pm_trend'] = (df['puell-multiple_diff'].rolling(window=3).min() >  0).shift(-2).to_numpy()
    df['pm_closerto_0.6'] = df['puell_multiple'].apply(lambda x: 0.58< x <0.65)
    df['puell_multiple_signal'] = df.apply(lambda x: x["pm_trend"] and x['pm_closerto_0.6'],axis=1)
    df = df.drop(columns=['puell-multiple_diff', 'pm_trend', 'pm_closerto_0.6'])
    return df

def process_difficulty(df):
    #URL = "https://api.cryptoquant.com/v1/btc/network-data/difficulty?window=day&from=20121130&limit=100000"
    df['difficulty_diff'] = df['difficulty'].diff(-14)
    df['difficulty_signal'] = df['difficulty_diff'].apply(lambda x : x>0)
    df = df.drop(columns=['difficulty_diff'])
    return df

def process_lth_sopr(df):
    #URL = "https://api.cryptoquant.com/v1/btc/market-indicator/sopr?window=day&from=20121130&limit=100000"
    df['sopr_signal'] = df['lth_sopr'].apply(lambda x : 1<= x < 1.1)
    df = df.drop(columns=['sopr','a_sopr','sth_sopr'])
    return df

def process_cryptoQuant_indicators():
    print('start to process cryptoQuant ')

    # get these df
    df_mvrv = process_mvrv(request_data_from_cryptoQuant(API_KEY_CryptoQuant,"https://api.cryptoquant.com/v1/btc/market-indicator/mvrv?window=day&from=20121130&limit=100000"))
    df_puell_multiple = process_puell_multiple(request_data_from_cryptoQuant(API_KEY_CryptoQuant,"https://api.cryptoquant.com/v1/btc/network-indicator/puell-multiple?window=day&from=20121130&limit=100000"))
    df_difficulty = process_difficulty(request_data_from_cryptoQuant(API_KEY_CryptoQuant,"https://api.cryptoquant.com/v1/btc/network-data/difficulty?window=day&from=20121130&limit=100000"))
    df_lth_sopr = process_lth_sopr(request_data_from_cryptoQuant(API_KEY_CryptoQuant,"https://api.cryptoquant.com/v1/btc/market-indicator/sopr?window=day&from=20121130&limit=100000"))


    # merge them together
    df_mvrv = df_mvrv.join(df_puell_multiple.set_index('date'),on='date')
    df_mvrv = df_mvrv.join(df_difficulty.set_index('date'),on='date')
    df_mvrv = df_mvrv.join(df_lth_sopr.set_index('date'),on='date')
    df_mvrv['date'] = pd.to_datetime(df_mvrv['date'])
    print('head of cryptoQuant data:')
    print(df_mvrv.head(5))
    return df_mvrv


#Count total True-------------------------------------------------------
def count_total(df):
    df['total indicators:'] = (df == True).astype(int).sum(axis=1)
    return df


# update excel --------------------------------------------------------------------------------------------------------------------------------------------------------------------

def update_excel(df_from_glassNode, df_from_cryptoQuant):
    #first merge glassnode data and cryptoquant data
    df_from_glassNode = df_from_glassNode.join(df_from_cryptoQuant.set_index('date'),on='date')

    
    # order means 你按什么顺序展示这些顺序

    order = ['date', 'coin_price', 'rcap_hodl_waves_1-2year', 'signal_rcap_hodl_waves_1-2year', 'balanced_price_usd', 'balanced_price_signal','mvrv','mvrv_signal', 'puell_multiple', 'puell_multiple_signal', 'difficulty', 'difficulty_signal', 'lth_sopr', 'sopr_signal',
            'o.ma30', 'o.ma60','hash_indicator','price_ma_30','price_ma_60' ,'price_indicator' ]
    df_from_glassNode = df_from_glassNode[order]
    
    #去掉日期总得小时分秒
    df_from_glassNode['date']=df_from_glassNode['date'].dt.date


    # create a new indicator
    df_from_glassNode['price&hash&puell_multiple_indicator'] = df_from_glassNode.apply(lambda x: x['price_indicator'] or x['hash_indicator'] and x['puell_multiple_signal'], axis = 1)
    # create a new indicator
    df_from_glassNode = count_total(df_from_glassNode)
    # create a new indicator
    df_from_glassNode['total_reminder'] = df_from_glassNode['total indicators:'].apply(lambda x: True if x>=3 else False)
    print("head of total data:")
    print(df_from_glassNode.head(5))


    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter('bottom_indicators.xlsx', engine='xlsxwriter')

    # Convert the dataframe to an XlsxWriter Excel object.
    df_from_glassNode.to_excel(writer, sheet_name='Sheet1',index = False)

    # Get the xlsxwriter workbook and worksheet objects.
    workbook  = writer.book
    worksheet = writer.sheets['Sheet1']

    # Get the dimensions of the dataframe.
    max_row, max_col = df_from_glassNode.shape
    print((max_row, max_col))
    format1 = workbook.add_format({'bg_color':   '#f0a1a8',
                               'font_color': '#9C0006'})

    # Apply a conditional format to the required cell range.
    worksheet.conditional_format(0, 0,max_row, max_col ,
                              {'type':     'text',
                                       'criteria': 'containing',
                                       'value':    'TRUE',
                                       'format':   format1})

    for column in df_from_glassNode:
        column_length = max(df_from_glassNode[column].astype(str).map(len).max(), len(column))
        col_idx = df_from_glassNode.columns.get_loc(column)
        writer.sheets['Sheet1'].set_column(col_idx, col_idx, column_length)


    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    #df_from_glassNode.to_excel('bottom_indicators.xlsx',index=False)


# main funciton -------------------------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":

    update_excel(process_glassNode__indicators(), process_cryptoQuant_indicators())

