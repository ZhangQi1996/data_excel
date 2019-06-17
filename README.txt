Version: 1.0
注意：
1.请勿删除或改动aqi_info.txt, cur_data.txt, prov.txt, city_prov.txt这四个文件
  删除将会影响本程序的正常运行
2.本程序基本依赖的拓展py库见requirements.txt
3.本程序依赖的数据库仅为MySQL
4.本程序的基本功能包括：
    - 在数据库中自动建立本程序相关的数据库表（包括：实时数据cur_data，历史AQI信息aqi_info, 省及区域表prov，市属关系表city_prov）
      推荐该功能在第一次建表时使用，该功能的机制是先删表再建表，谨防使用该功能造成数据丢失
      参见封装函数utils.py/yield_all_tables

    - 考虑到省及区域表prov，市属关系表city_prov 这两个表的数据是固定的，故本程序提供对该两表的数据填充
      请确保调用前两表已经建立，谨防该功能的二次调用
      参见封装函数utils.py/complete_table_prov_and_city_prov

    - 提供对实时数据cur_data，历史AQI信息aqi_info 数据的excel生成实现
      参见封装函数utils.py/get_data_and_save_as_excel
5.对数据源的配置文件见db.ini文件
6.若要获得本程序的原始版本请使用git进行clone
    - clone路径：https://github.com/ZhangQi1996/data_excel.git