import dolphindb as ddb
import akshare as ak
import pandas as pd
import datetime

# DolphinDB 连接配置
DDB_HOST = 'localhost'
DDB_PORT = 8848
DDB_USER = 'admin'
DDB_PASS = '123456'

def download_futures_daily_ak(symbol="RB2210"):
    """
    使用 akshare 下载指定品种的期货日线行情
    """
    try:
        # 获取期货日线行情 (示例：螺纹钢)
        df = ak.futures_zh_daily_sina(symbol=symbol)
        if df.empty:
            print(f"No data for {symbol}")
            return None
        
        # 数据清洗
        df['date'] = pd.to_datetime(df['date'])
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
        
        # 创建数据库和表
        script = f"""
        dbPath = "{db_path}"
        tableName = "{table_name}"
        if(!existsDatabase(dbPath)){{
            db = database(dbPath, VALUE, 2010.01.01..2030.12.31)
        }}
        db = database(dbPath)
        if(!existsTable(dbPath, tableName)){{
            // 根据 akshare 返回的列定义 schema
            schema = table(1:0, `date`open`high`low`close`volume`hold, [DATE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE])
            db.createPartitionedTable(schema, tableName, `date)
        }}
        """
        s.run(script)
        
        # 上传并追加数据
        s.upload({'temp_df': df})
        s.run(f'loadTable("{db_path}", "{table_name}").append!(temp_df)')
        print(f"Successfully saved {len(df)} records to {db_path}/{table_name}")
        
    except Exception as e:
        print(f"Error saving to DolphinDB: {e}")
    finally:
        s.close()

if __name__ == "__main__":
    # 示例：下载螺纹钢连续合约数据
    # 注意：akshare 的 symbol 格式可能因接口而异
    data = download_futures_daily_ak(symbol="RB0") 
    if data is not None:
        save_to_dolphindb(data, "dfs://futures_ak", "daily_rb")
