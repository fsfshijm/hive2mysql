#!/usr/local/bin/python2.7
# -*- coding: utf8 -*-

import sys
import os
from datetime import datetime, timedelta
from urllib import unquote
from subprocess import check_output, CalledProcessError

basepath = os.path.realpath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(basepath+"/lib/")

from database import Connection

HIVE_CMD = '/home/hadoop/hive/bin/hive'


TO_DAYS_SQL = "select to_days(%s) as days;"

FIND_PARTITION_SQL = "SELECT * FROM information_schema.PARTITIONS WHERE table_name='activation_summary' and partition_name=%s;"

ADD_PARTITION_SQL = "ALTER TABLE activation_summary ADD PARTITION(PARTITION %s VALUES in (%d));"

def query_by_hive_cmd(hql):
    """
    query hive using hive cmd line tool, capture result.
    """
    print hql
    hql = "use bi; SET mapred.fairscheduler.pool=urgent; %s" % hql
    try:
        raw_result = check_output([HIVE_CMD, "-e", hql])
    except CalledProcessError:
        print "query hive failed"
        sys.exit(-1)

    query_result = [ line.split("\t") for line in raw_result.split("\n")[:-1] ]
    return query_result


GROUP_HQL = """
SELECT
    coalesce(searchid,0),
    coalesce(media_placement_id,0),
    coalesce(creative_id,0),
    coalesce(click_time,0),
    coalesce(act_time,0),
    act_userid_type,
    act_userid,
    act_type,
    act_id,
    coalesce(is_charge, 0),
    clk_ext_info,
    source,
    act_dt,
    act_hr,
    dt,
    hr
FROM activation where dt=%s and hr=%s
"""

INSERT_DATA_SQL = """
INSERT INTO activation_summary
SET
    searchid = '%s',
    media_placement_id = '%s',
    creative_id = '%s',
    click_time = '%s',
    act_time = '%s',
    act_userid_type = '%s',
    act_userid = '%s',
    act_type = '%s',
    act_id = '%s',
    is_charge = '%s',
    clk_ext_info = '%s',
    source = '%s',
    act_dt = '%s',
    act_hr = '%s',
    dt = '%s',
    hr = %d
"""


def load_into_mysql(time):
    time = time - timedelta(hours=1)
    print "use_time %s" % time
    dt = time.strftime("%Y%m%d")
    formated_date = time.strftime("%Y-%m-%d")
    hr = time.strftime("%H")
    days_ago_30 = (time - timedelta(days=30)).strftime("%Y-%m-%d")
    query_result = query_by_hive_cmd(GROUP_HQL % (dt, hr))
    #query_result = [[u'715795204700799065', u'1000386', u'1002331', u'1403830309', u'1403830743', u'IDFA', u'5C5F935B-64E3-428F-8348-99301469CB22', u'APP', u'com.hugenstar.tdzmIOS', u'1', u'', u'OW', u'20140627', u'08', '2014-06-27', '9']]

    bi_db = Connection(host='dreportm.db.in.xxxxxxxxx.cn', database='bi', user='bi', password='bibi')
    bi_db.execute("DELETE FROM activation_summary WHERE (dt='%s' AND hr=%d) or (dt='%s')" % (formated_date, int(hr), days_ago_30))
    
    days = bi_db.get(TO_DAYS_SQL, formated_date)
    partition_name = "p%d" % days.days
    partition_info = bi_db.get(FIND_PARTITION_SQL, partition_name)
    if not partition_info :
        bi_db.execute(ADD_PARTITION_SQL % (partition_name, days.days))

    counter = 0
    for row in query_result:
        sql = ""
        try:
            row = map(unquote, row)
            row = map(unquote, row)
            try:
                row = map(unicode, row)
            except:
                row = map(lambda f: f.decode('utf8'), row)

            if row[-5] == 'NULL':
                row[-5] = "NULL"
            if row[-6] == 'NULL':
                row[-6] = "NULL"
            row = row[:-2] # pop dt and hr
            row.append(formated_date)
            row.append(int(hr))

            sql = INSERT_DATA_SQL % tuple(row)
            #print sql			
            bi_db.execute(sql)
            counter += 1
        except Exception,e:
            print "load exception", row
            print e

    print "%s rows inserted into activation_summary" % counter



if __name__ == "__main__":
    stamp = os.environ.get("DOMINO_STAMP", None)
    if not stamp:
        time = datetime.today() - timedelta(hours=1)
    else: 
        time = datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S")

    load_into_mysql(time)

