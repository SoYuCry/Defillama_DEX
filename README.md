**使用说明**
在代码运行中，会生成两个文件，一个是‘progress.txt’，用于记录下一个爬取的 DEX 的名称；另一个是‘coingecko_volume_daily.csv’，存放已经爬取的 DEX 的数据。每次使用前，删除之前的这两个文件。

在成功拿到所有的 DEX 的数据后，使用‘DEX_双周报模板2.xlsx’，可以快速生成图表。

**第一步**
将所有数据粘贴到‘RawData’的表单中，并根据黄色列降序排列，取前 20 名粘贴到‘新新工作区表单’的最左侧

**第二步**
根据该表单中的橙色的指令完成粘贴和排序。在‘交易量变化’、‘环比变化’、‘市场份额’中得到图表。

注：‘Get2023VolumeByDefillama.py’是用于爬起2023一整年的交易量，作为示例。通过更换 API 可以轻易修改为爬取 TVL 的脚本
