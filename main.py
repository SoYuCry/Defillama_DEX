import requests
from datetime import datetime
import csv
import os
import pandas as pd


# 每次使用之前，删除原先的 coingecko_volume_daily.csv 和 progress.txt

def getalldex():
    # 添加 Defi LLaMA 所有的 dex 名称
    dexlist = ['uniswap']
    result = requests.get('https://api.llama.fi/overview/dexs?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyVolume').json()
    for i in range(0, len(result['protocols'])):
        dexlist.append(result['protocols'][i]['name'].replace(' ', '-'))
    return dexlist

def getvolume(dexlist, iteral=30):
    unix_time = []  
    writedate = False  
    volume = []  

    # 利用 progress.txt 来判断是否已经爬取过
    if os.path.exists('progress.txt'):
        with open('progress.txt', 'r') as f:
            last_dex = f.readline().strip()
            start_idx = dexlist.index(last_dex)
            dexlist = dexlist[start_idx + 1:]
            writedate = True
    else:
        writedate = False

    for j in range(0, len(dexlist)):
        try:
            # 如果需要更换数据源（比如 TVL），只需要更换这一行的链接即可
            result = requests.get('https://api.llama.fi/summary/dexs/' + dexlist[j] + '?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyVolume').json()
            print("processing:" + dexlist[j])

            if len(result) != 1:
                if not writedate:
                    for i in range(0, iteral):
                        date = datetime.utcfromtimestamp(result['totalDataChart'][-(iteral - i)][0]).strftime('%Y-%m-%d %H:%M:%S')
                        unix_time.append(date[:-9])
                    with open('coingecko_volume_daily.csv', 'w') as f:
                        write = csv.writer(f)
                        write.writerow(['name'] + unix_time)
                        writedate = True
                else:
                    volume.append(dexlist[j])
                    try:
                        for i in range(0, iteral):
                            volume.append(result['totalDataChart'][-(iteral - i)][1])

                        with open('coingecko_volume_daily.csv', 'a', newline='') as csvfile:
                            writer = csv.writer(csvfile)
                            writer.writerow(volume)
                            volume = []
                        # 保存断点，以便在重启程序时，从上次断点处继续爬取
                        with open('progress.txt', 'w') as f:
                            f.write(dexlist[j])
                    except Exception as e:
                        print(f"Error processing volume for {dexlist[j]}: {e}, skipping to next DEX.")
                        volume = []

        except requests.exceptions.SSLError as e:
            print(f"SSL Error for {dexlist[j]}: {e}, skipping to next DEX.")
        except Exception as e:
            print(f"Error processing {dexlist[j]}: {e}, skipping to next DEX.")
            continue


# 将同一个协议的不同版本的交易量加起来/更改为更简短的名字
# 将有四个子协议的交易量加起来
def Add4together(filename,nameA,nameB,nameC,nameD,name):
  data = pd.read_csv(filename)

  if not all(x in data['name'].values for x in [nameA, nameB, nameC,nameD]):
      print(f"One or more of {nameA}, {nameB}, {nameC},{nameD} not found in {filename}. Skipping.")
      return
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

  if not all(x in data['name'].values for x in [nameA, nameB, nameC]):
    print(f"One or more of {nameA}, {nameB}, {nameC} not found in {filename}. Skipping.")
    return

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

  if not all(x in data['name'].values for x in [nameA, nameB]):
    print(f"One or more of {nameA}, {nameB} not found in {filename}. Skipping.")
    return

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
  if not all(x in data['name'].values for x in [nameA]):
    print(f"{nameA} not found in {filename}. Skipping.")
    return

  # 找到名字是A和B的两行，并将它们的值加起来
  row_A = data.loc[data['name'] == nameA].iloc[:, 1:]
  print("comebining"+ nameA +' to '+ Newname)

  row_A_index = int(row_A.index[0])
  row_new_value = [Newname]
  for i in range(1, data.shape[1]):
      row_new_value.append(str(data.iloc[row_A_index, i] ))

  data.drop(row_A_index, inplace=True)
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

  Add2together(filename = 'coingecko_volume_daily.csv',nameA = 'Lighter-V1',nameB = 'Lighter-V2', name='Lighter')


  Add2together(filename = 'coingecko_volume_daily.csv',nameA = 'Arbitrum-Exchange-V2',nameB = 'Arbitrum-Exchange-V3',name='Arbitrum-Exchange')

  Add3together(filename = 'coingecko_volume_daily.csv',nameA = 'Zyberswap-AMM',nameB = 'Zyberswap-V3',nameC = 'ZyberSwap-Stableswap',name='Zyberswap')

  ChangeName(filename = 'coingecko_volume_daily.csv',nameA = 'Curve-DEX',Newname='Curve')
  
  ChangeName(filename = 'coingecko_volume_daily.csv',nameA = 'Maverick-Protocol',Newname='Maverick')
  ChangeName(filename = 'coingecko_volume_daily.csv',nameA = 'Astroport-Classic',Newname='Astroport')
  ChangeName(filename = 'coingecko_volume_daily.csv',nameA = 'Vertex-Protocol',Newname='Vertex')
  ChangeName(filename = 'coingecko_volume_daily.csv',nameA = 'WOOFi',Newname='WOOFi')
  ChangeName(filename = 'coingecko_volume_daily.csv',nameA = 'Lifinity-V1',Newname='Lifinity')    
if __name__ == "__main__":
    # 从今天爬去到 30 天前的数据，如果想要爬取更多天的数据，可以将 30 改为更大的数字
    INTERAL = 30
    dexlist = getalldex()

    # 如果想更改数据源，只需要更改该函数中的 Request 链接即可
    getvolume(dexlist, iteral=INTERAL)
    Intergate()
