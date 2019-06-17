import pymysql.cursors
import configparser


class DataSource:

    def __init__(self, conf):
        """
        初始化一个数据源连接
        :param conf: dict或者str
        dict是一个配置字典
        str为配置文件路径
        """
        if isinstance(conf, dict):
            self.conf = conf
        elif isinstance(conf, str):
            print('正在读取DB配置文件...')
            self.conf = {}
            self._read_conf_file(conf)
            print('读取DB配置文件结束...')
        else:
            raise Exception('conf参数必须为dict或str类型')
        self.conn = pymysql.connect(**self.conf)
        self._close = False

    def _read_conf_file(self, file_path):
            cp = configparser.ConfigParser()
            cp.read(file_path, encoding='utf-8')    # 编码设置为utf-8
            self.conf['host'] = cp.get('db', 'host')
            self.conf['port'] = cp.getint('db', 'port')
            self.conf['user'] = cp.get('db', 'user')
            self.conf['password'] = cp.get('db', 'password')
            self.conf['db'] = cp.get('db', 'db')
            self.conf['charset'] = cp.get('db', 'charset')
            self.conf['cursorclass'] = pymysql.cursors.DictCursor   # 默认游标

    def query(self, sql, *args):
        """查询"""
        with self.conn.cursor() as cursor:
            cursor.execute(sql, args)
            result = cursor.fetchall()
        self.conn.commit()
        return result

    def insert(self, sql, *args, ignore_integrity_error=False):
        """参数ignore_integrity_error用于解决在插入过程中是否忽视完整性错误"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, args)
            self.conn.commit()
        except pymysql.err.IntegrityError as e:     # 完整性错误，如插入重复键
            print("Error: %s %s" % (e, type(e)))
            if ignore_integrity_error is False:
                self.conn.rollback()
            else:
                print("\tand the Error has been ignored(there was not a rollback in the transaction)")
                print("\tyou should set False to param:ignore_integrity_error, if you donnot want ignore that error..")
        except Exception as e:
            self.conn.rollback()
            raise e

    def insert_many(self, sql, *args, ignore_integrity_error=False):
        """
        e.g.
        ds.insert_many("INSERT INTO cur_data(city_code, time, aqi, pm2_5, pm10, so2, no2, co, o3, pri_pollutant) "
                         "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", args=args)
        :param sql:
        :param args:
        :param ignore_integrity_error:
        :return:
        """
        try:
            with self.conn.cursor() as cursor:
                if args is not None:
                    cursor.executemany(sql, args)
            self.conn.commit()
        except pymysql.err.IntegrityError as e:     # 完整性错误，如插入重复键
            print("Error: %s %s" % (e, type(e)))
            if ignore_integrity_error is False:
                self.conn.rollback()
            else:
                print("\tand the Error has been ignored(there was not a rollback in the transaction)")
                print("\tyou should set False to param:ignore_integrity_error, if you donnot want ignore that error..")
        except Exception as e:
            self.conn.rollback()
            raise e

    def update(self, sql, *args):
        self.insert(sql, args)

    def update_many(self, sql, *args):
        self.insert_many(sql, args)

    def delete(self, sql, *args):
        self.insert(sql, args)

    def execute(self, sql, *args):
        try:
            with self.conn.cursor() as cursor:
                ret = cursor.execute(sql, args)
            self.conn.commit()
            return ret
        except Exception as e:
            self.conn.rollback()
            raise e

    def exit(self):
        if self._close is False:
            self.conn.close()
            self._close = True

    def __del__(self):
        if self._close is False:
            self.exit()

    close = exit

