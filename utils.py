from data_source import DataSource
import xlwt


def get_data_source(conf='db.ini'):
    """获取数据源"""
    return DataSource(conf)


def _get_cur_data_data(ds: DataSource):
    """获取实时数据"""
    res = ds.query("SELECT * FROM cur_data")
    print("从数据库中读取cue_data完成")
    return res


def _get_aqi_info_data(ds: DataSource):
    """获取aqi历史数据"""
    res = ds.query("SELECT * FROM aqi_info")
    print("从数据库中读取aqi_info完成")
    return res


def _get_data(ds: DataSource, type: str):
    if type == 'cur_data':
        return _get_cur_data_data(ds)
    elif type == 'aqi_info':
        return _get_aqi_info_data(ds)
    else:
        raise ValueError('type expects cur_data or aqi_info, but %s you input' % type)


# 设置表格样式
def _set_style(name='宋体', height=220, bold=False):
    style = xlwt.XFStyle()
    font = xlwt.Font()
    font.name = name
    font.bold = bold
    font.color_index = 4
    font.height = height
    style.font = font
    return style


def _save_data_as_excel(data: list, type: str, file_name: str):
    assert type in ('cur_data', 'aqi_info'), "type expects cur_data or aqi_info, but type=%s you input" % type
    excel = xlwt.Workbook()
    style = _set_style()
    sheet = excel.add_sheet(type, cell_overwrite_ok=True)
    with open(file=type + '.txt', mode='r', encoding='utf-8') as f:
        data_attrs = f.readline().rstrip('\n').split(' ')
    row0 = data_attrs
    print("正在生成excel")
    # 写第一行
    for i in range(0, len(row0)):
        sheet.write(0, i, row0[i], style)
    cent = len(data) // 10
    for i in range(1, len(data) + 1):
        for j, key in enumerate(data_attrs):
            sheet.write(i, j, data[i - 1][key].__str__(), style)
        if cent and i % cent == 0:
            print("当前生成Excel进度:%s%%" % 10 * cent)
    excel.save(file_name + '.xls')
    print("当前生成Excel进度: complete")


def get_data_and_save_as_excel(ds: DataSource, type: str, file_name: str):
    """
    将数据导入excel
    :param ds: 数据源
    :param type: cur_data or aqi_info
    :param file_name: 生成excel文件的前缀名
    :return:
    """
    data = _get_data(ds, type)
    _save_data_as_excel(data, type, file_name)


def _yield_cur_data_table(ds: DataSource):
    ds.execute('drop table if exists cur_data;')
    ds.execute("""
        create table cur_data(
            city_code int(11) not null,
            time DATETIME not null,
            aqi int(11) not null,
            pm2_5 int(11) not null,
            pm10 int(11) not null,
            so2 float not null,
            no2 float not null,
            co float not null,
            o3 float not null,
            pri_pollutant varchar(25) not null,
            primary key (city_code, time)
        );
    """)


def _yield_prov_table(ds: DataSource):
    ds.execute('drop table if exists prov;')
    ds.execute("""
        create table prov(
            prov_name varchar(12) primary key,
            prov_cap varchar(12) not null
        );
    """)


def _yield_city_prov_table(ds: DataSource):
    ds.execute('drop table if exists city_prov;')
    ds.execute("""
        create table city_prov(
            city_code int(11) primary key,
            city_name varchar(30) not null,
            prov_name varchar(12) not null,
            longitude float not null,
            latitude float not null
        );
    """)


def _yield_aqi_info_table(ds: DataSource):
    ds.execute('drop table if exists aqi_info;')
    ds.execute("""
        create table aqi_info(
            city_code int(11) not null,
            DATE DATE not null,
            aqi int(11) not null,
            pri_pollutant varchar(25) not null,
            primary key (city_code, date)
        );
    """)


def yield_all_tables(ds: DataSource):
    _yield_aqi_info_table(ds)
    _yield_cur_data_table(ds)
    _yield_city_prov_table(ds)
    _yield_prov_table(ds)


def _complete_prov_table(ds: DataSource):
    with open(file='prov.txt', mode='r', encoding='utf-8') as f:
        sql = "insert into prov (prov_name, prov_cap) values (%s, %s);"
        values = []
        f.readline()
        s = f.readline()
        while s != '':
            item = s.rstrip('\n').split(' ')
            values.append(item)
            s = f.readline()
    ds.insert_many(sql, *values)


def _complete_city_prov_table(ds: DataSource):
    with open(file='city_prov.txt', mode='r', encoding='utf-8') as f:
        sql = "insert into city_prov (city_code, city_name, prov_name, longitude, latitude) values (%s, %s, %s, %s, %s);"
        values = []
        f.readline()
        s = f.readline()
        while s != '':
            item = s.rstrip('\n').split(' ')
            values.append(item)
            s = f.readline()
    ds.insert_many(sql, *values)


def complete_table_prov_and_city_prov(ds: DataSource):
    _complete_prov_table(ds)
    _complete_city_prov_table(ds)