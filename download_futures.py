import dolphindb as ddb
import tushare as ts
import pandas as pd
import datetime

# 配置 Tushare token (请替换为您自己的 token)
TS_TOKEN = 'YOUR_TUSHARE_TOKEN'
ts.set_token(TS_TOKEN)
pro = ts.pro_api()

# DolphinDB 连接配置
DDB_HOST = 'localhost'
DDB_PORT = 8848
DDB_USER = 'admin'
DDB_PASS = '123456'

def download_futures_daily(trade_date):
    """
    下载指定日期的期货日线行情
    """
    try:
        # 获取所有交易所的期货日线行情
        df = pro.fut_daily(trade_date=trade_date)
        if df.empty:
            print(f"No data for {trade_date}")
            return None
        
        # 数据清洗：转换日期格式
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        return df
    except Exception as e:
        print(f"Error downloading data: {e}")
        return None

def save_to_dolphindb(df, db_path, table_name):
    """
    将数据保存到 DolphinDB
    """
    if df is None or df.empty:
        return

    s = ddb.session()
    try:
        s.connect(DDB_HOST, DDB_PORT, DDB_USER, DDB_PASS)
        
        # 如果数据库不存在，则创建
        # 这里使用分布式数据库 (DFS)
        create_db_script = f"""
        if(!existsDatabase("{db_path}")){{
            db = database("{db_path}", VALUE, 2020.01M..2030.12M)
        }}
        """
        s.run(create_db_script)
        
        # 上传数据到 DolphinDB
        s.upload({'temp_df': df})
        
        # 将临时表数据追加到 DFS 表
        save_script = f"""
        db = database("{db_path}")
        if(!existsTable("{db_path}", "{table_name}")){{
            db.createPartitionedTable(temp_df, "{table_name}", "trade_date")
        }}
        loadTable("{db_path}", "{table_name}").append!(temp_df)
        """
        s.run(save_script)
        print(f"Successfully saved {len(df)} records to {db_path}/{table_name}")
        
    except Exception as e:
        print(f"Error saving to DolphinDB: {e}")
    finally:
        s.close()

if __name__ == "__main__":
    # 示例：下载昨天的数据
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    print(f"Downloading futures data for {yesterday}...")
    
    data = download_futures_daily(yesterday)
    if data is not None:
        save_to_dolphindb(data, "dfs://futures_db", "daily_market")
