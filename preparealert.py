import pandas as pd
import datetime
def normalize(x):
    x = str(x)
    result = '2019/'+x.replace('-',' ')
    return result
csv_data = pd.read_csv("./normalalert.csv",names=['timestamp','msg','ip_src','src_port','ip_dst','dst_port'])
csv_data['timestamp'] = csv_data['timestamp'].apply(lambda x:normalize(x))
csv_data.to_csv("./alert.csv",index=False,encoding='utf-8')
