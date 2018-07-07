# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.conf import settings
from scrapy import log
from scrapy.exceptions import DropItem
import pymysql
# import pymongo


class TiebaPipeline(object):

    def __init__(self):
        self.conn = None
        self.cur = None

    def open_spider(self, spider):
        self.conn = pymysql.connect(
            host='localhost',
            port=3306,
            user='root',
            password='',
            db='tieba',
            charset='utf8mb4',
        )
        self.cur = self.conn.cursor()

    def process_item(self, item, spider):
        if not hasattr(item, 'table_name'):
            return item
        # 相当于下面两行代码
        print(item.table_name)
        cols, values = zip(*item.items())
        # cols = item.keys()
        # values = [item[col] for col in cols]
        sql = "INSERT INTO `{}` ({})" \
            "VALUES ({}) ON DUPLICATE KEY UPDATE {}".format(
                item.table_name,
                ','.join(['`%s`' % col for col in cols]),
                # *号将列表内的值扩展到至N个
                ','.join(['%s'] * len(cols)),
                ','.join('`{}`=%s'.format(col) for col in cols)
            )
        # values作为参数传入，防止sql注入
        self.cur.execute(sql, values * 2)
        self.conn.commit()
        # print(self.cur._last_executed)
        return item

    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()
