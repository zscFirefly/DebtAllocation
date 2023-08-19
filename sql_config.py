import sys
sys.path.append("..")
from sqlalchemy import create_engine


class sqlConfig:
    db_name = "convert_bond"           # 数据库名
    db_user = "root"              # 用户名
    db_host = "43.143.142.50"         # IP地址
    db_port = 3307                # 端口号
    db_passwd = "Aa123456"            # 密码

class sqlExecute():
    """docstring for sqlExecute"""
    db_info = {
        'user': sqlConfig.db_user,
        'password': sqlConfig.db_passwd,
        'host': sqlConfig.db_host,
        'port': sqlConfig.db_port,
        'database': sqlConfig.db_name
    }
    engine = create_engine(
        'mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)d/%(database)s?charset=utf8' % db_info,
        encoding='utf-8'
    )