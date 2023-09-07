import requests
from datetime import datetime
import csv
import pandas as pd

# 获取 Defillama 的所有 dex
def getalldex():
  # 预置一个 uniswap，以便在首行添加日期
  dexlist = ['uniswap']
  # 爬取 Defillama 上所有的 dex
  result = requests.get('https://api.llama.fi/overview/dexs?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyVolume').json()
  for i in range(0,len(result['protocols'])):
    dexlist.append(result['protocols'][i]['name'].replace(' ', '-'))
  return dexlist
# 获取他们的交易量
def getvolume(dexlist,iteral=30):
  unix_time = []  # 日期
  writedate = False # 首次循环先把日期添加好，再做交易量的添加
  volume = []
  for j in range(0,len(dexlist)):
    result = requests.get('https://api.llama.fi/summary/dexs/'+ dexlist[j] +'?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyVolume').json()
    print("processing:" + dexlist[j])
    # 有些情况下，result 会爬出错，此时长度会变成 1
    if len(result) != 1:
      # 判断是否完成了第一行的日期写入
      if writedate == False:
        for i in range(0,iteral):
          date = datetime.utcfromtimestamp(result['totalDataChart'][-(iteral-i)][0]).strftime('%Y-%m-%d %H:%M:%S')
          unix_time.append(date[:-9])
        with open('coingecko_volume_daily.csv','w') as f:
          write = csv.writer(f)
          write.writerow(['name'] + unix_time)
          writedate = True
      else:
          # 若已完成，添加交易量之前，添加 dex 的名字
          volume.append(dexlist[j])
          try:
            for i in range(0,iteral):
              volume.append(result['totalDataChart'][-(iteral-i)][1])

            with open('coingecko_volume_daily.csv', 'a', newline='') as csvfile:
              writer = csv.writer(csvfile)
              writer.writerow(volume)
              # 写入后对 list 清零
              volume = []
          except:
              volume = []
# 将有四个子协议的交易量加起来
def Add4together(filename,nameA,nameB,nameC,nameD,name):
  data = pd.read_csv(filename)

  # 找到名字是A和B的两行，并将它们的值加起来
  row_A = data.loc[data['name'] == nameA].iloc[:, 1:]
  row_B = data.loc[data['name'] == nameB].iloc[:, 1:]
  row_C = data.loc[data['name'] == nameC].iloc[:, 1:]
  row_D = data.loc[data['name'] == nameD].iloc[:, 1:]

  print("comebining"+nameA+'、'+nameB+'、'+nameC+'、'+nameD+' to '+name)

  row_A_index = int(row_A.index[0])
  row_B_index = int(row_B.index[0])
  row_C_index = int(row_C.index[0])
  row_D_index = int(row_D.index[0])
  data.to_csv(filename, index=False)

  row_new_value = [name]
  for i in range(1, data.shape[1]):
      row_new_value.append(str(data.iloc[row_A_index, i] + data.iloc[row_B_index, i]+data.iloc[row_C_index, i]+data.iloc[row_D_index, i]))

  data.drop(row_A_index, inplace=True)
  data.drop(row_B_index, inplace=True)
  data.drop(row_C_index, inplace=True)
  data.drop(row_D_index, inplace=True)
  data.to_csv(filename, index=False)

  # Read the existing data from the CSV file
  with open(filename, 'r', newline='') as csvfile:
      reader = csv.reader(csvfile)
      lines = list(reader)

  # Insert the new row at the desired location (after row_A_index)
  lines.insert(row_A_index + 1, row_new_value)

  # Write the updated data back to the CSV file
  with open(filename, 'w', newline='') as csvfile:
      writer = csv.writer(csvfile)
      writer.writerows(lines)
# 将有三个子协议的交易量加起来    
def Add3together(filename,nameA,nameB,nameC,name):
  data = pd.read_csv(filename)

  # 找到名字是A和B的两行，并将它们的值加起来
  row_A = data.loc[data['name'] == nameA].iloc[:, 1:]
  row_B = data.loc[data['name'] == nameB].iloc[:, 1:]
  row_C = data.loc[data['name'] == nameC].iloc[:, 1:]

  print("comebining"+nameA+'、'+nameB+'、'+nameC+'、'+' to '+name)

  row_A_index = int(row_A.index[0])
  row_B_index = int(row_B.index[0])
  row_C_index = int(row_C.index[0])

  row_new_value = [name]
  for i in range(1, data.shape[1]):
      row_new_value.append(str(data.iloc[row_A_index, i] + data.iloc[row_B_index, i]+data.iloc[row_C_index, i]))
  
  data.drop(row_A_index, inplace=True)
  data.drop(row_B_index, inplace=True)
  data.drop(row_C_index, inplace=True)
  data.to_csv(filename, index=False)

  # Read the existing data from the CSV file
  with open(filename, 'r', newline='') as csvfile:
      reader = csv.reader(csvfile)
      lines = list(reader)

  # Insert the new row at the desired location (after row_A_index)
  lines.insert(row_A_index + 1, row_new_value)

  # Write the updated data back to the CSV file
  with open(filename, 'w', newline='') as csvfile:
      writer = csv.writer(csvfile)
      writer.writerows(lines)
# 将有两个子协议的交易量加起来
def Add2together(filename,nameA,nameB,name):

  data = pd.read_csv(filename)

  # 找到名字是A和B的两行，并将它们的值加起来
  row_A = data.loc[data['name'] == nameA].iloc[:, 1:]
  row_B = data.loc[data['name'] == nameB].iloc[:, 1:]

  print("comebining"+nameA+'、'+nameB+' to '+name)

  row_A_index = int(row_A.index[0])
  row_B_index = int(row_B.index[0])
  row_new_value = [name]
  for i in range(1, data.shape[1]):
      row_new_value.append(str(data.iloc[row_A_index, i] + data.iloc[row_B_index, i]))

  data.drop(row_A_index, inplace=True)
  data.drop(row_B_index, inplace=True)
  data.to_csv(filename, index=False)

  # Read the existing data from the CSV file
  with open(filename, 'r', newline='') as csvfile:
      reader = csv.reader(csvfile)
      lines = list(reader)

  # Insert the new row at the desired location (after row_A_index)
  lines.insert(row_A_index + 1, row_new_value)

  # Write the updated data back to the CSV file
  with open(filename, 'w', newline='') as csvfile:
      writer = csv.writer(csvfile)
      writer.writerows(lines)
def ChangeName(filename,nameA,Newname):

  data = pd.read_csv(filename)

  # 找到名字是A和B的两行，并将它们的值加起来
  row_A = data.loc[data['name'] == nameA].iloc[:, 1:]
  print("comebining"+ nameA +' to '+ Newname)

  row_A_index = int(row_A.index[0])
  row_new_value = [Newname]
  for i in range(1, data.shape[1]):
      row_new_value.append(str(data.iloc[row_A_index, i] ))

  data.drop(row_A_index, inplace=True)
  data.drop(Newname, inplace=True)
  data.to_csv(filename, index=False)

  # Read the existing data from the CSV file
  with open(filename, 'r', newline='') as csvfile:
      reader = csv.reader(csvfile)
      lines = list(reader)

  # Insert the new row at the desired location (after row_A_index)
  lines.insert(row_A_index + 1, row_new_value)

  # Write the updated data back to the CSV file
  with open(filename, 'w', newline='') as csvfile:
      writer = csv.writer(csvfile)
      writer.writerows(lines)
def Intergate():
  Add3together(filename = 'coingecko_volume_daily.csv',nameA = 'Uniswap-V1',nameB = 'Uniswap-V2',nameC = 'Uniswap-V3',name='Uniswap')

  Add4together(filename = 'coingecko_volume_daily.csv',nameA = 'PancakeSwap-AMM-V1',nameB = 'PancakeSwap-AMM',nameC = 'PancakeSwap-StableSwap',nameD = 'PancakeSwap-AMM-V3', name='PancakeSwap')

  Add3together(filename = 'coingecko_volume_daily.csv',nameA = 'Trader-Joe-DEX',nameB = 'Joe-V2',nameC = 'Joe-V2.1',name='Trader Joe')

  Add2together(filename = 'coingecko_volume_daily.csv',nameA = 'Balancer-V1',nameB = 'Balancer-V2',name='Balancer')

  Add2together(filename = 'coingecko_volume_daily.csv',nameA = 'SUNSwap-V1',nameB = 'SUNSwap-V2',name='SUNSwap')

  Add2together(filename = 'coingecko_volume_daily.csv',nameA = 'Quickswap-Dex',nameB = 'Quickswap-V3',name='Quickswap')

  Add3together(filename = 'coingecko_volume_daily.csv',nameA = 'SushiSwap',nameB = 'Sushi-Trident',nameC = 'SushiSwap-V3',name='Sushiswap')

  Add2together(filename = 'coingecko_volume_daily.csv',nameA = 'PulseX-V1',nameB = 'PulseX-V2',name='PulseX')

  Add2together(filename = 'coingecko_volume_daily.csv',nameA = 'KyberSwap-Classic',nameB = 'KyberSwap-Elastic',name='KyberSwap')

  Add2together(filename = 'coingecko_volume_daily.csv',nameA = 'Thena-V1',nameB = 'THENA-FUSION',name='Thena')

  Add2together(filename = 'coingecko_volume_daily.csv',nameA = 'Camelot-V2',nameB = 'Camelot-V3',name='Camelot')

  Add2together(filename = 'coingecko_volume_daily.csv',nameA = 'Ramses-V1',nameB = 'Ramses-V2',name='Ramses')

  Add2together(filename = 'coingecko_volume_daily.csv',nameA = 'Arbitrum-Exchange-V2',nameB = 'Arbitrum-Exchange-V3',name='Arbitrum-Exchange')

  Add3together(filename = 'coingecko_volume_daily.csv',nameA = 'Zyberswap-AMM',nameB = 'Zyberswap-V3',nameC = 'ZyberSwap-Stableswap',name='Zyberswap')

  ChangeName(filename = 'coingecko_volume_daily.csv',nameA = 'Curve-DEX',Newname='Curve')
  
  ChangeName(filename = 'coingecko_volume_daily.csv',nameA = 'Maverick-Protocol',Newname='Maverick')


if __name__ == "__main__":
  INTERAL = 30
  dexlist = getalldex()
  getvolume(dexlist,iteral = INTERAL)
  Intergate()