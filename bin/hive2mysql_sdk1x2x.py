#!/usr/local/bin/python2.7
# -*- coding: utf8 -*-

import sys
import os
from datetime import datetime, timedelta
from urllib import unquote
from subprocess import check_output, CalledProcessError

basepath = os.path.realpath(os.path.dirname(__file__)+'/../')
sys.path.append(basepath+"/lib/")

from database import Connection

HIVE_CMD = '/home/hadoop/hive/bin/hive'


TO_DAYS_SQL = "select to_days(%s) as days;"

FIND_PARTITION_SQL = "SELECT * FROM information_schema.PARTITIONS WHERE table_name='event_report_sdk1x2x' and partition_name=%s;"

ADD_PARTITION_SQL = "ALTER TABLE event_report_sdk1x2x ADD PARTITION(PARTITION %s VALUES in (%d));"

def query_by_hive_cmd(hql):
    """
    query hive using hive cmd line tool, capture result.
    """
    print hql
    hql = "use user_event; SET mapred.fairscheduler.pool=urgent; %s" % hql
    try:
        raw_result = check_output([HIVE_CMD, "-e", hql])
    except CalledProcessError:
        print "query hive failed"
        sys.exit(-1)

    query_result = [ line.split("\t") for line in raw_result.split("\n")[:-1] ]
    return query_result


GROUP_HQL = """
SELECT
    coalesce(sdk_rt, 0),
    coalesce(action_type, 'unknow'),
    count(1) as action_count,
    dt,
    hr
FROM user_event_hourly where dt=%s and hr=%s
GROUP BY
    sdk_rt,
    action_type,
    dt,
    hr
"""

INSERT_DATA_SQL = """
INSERT INTO event_report_sdk1x2x
SET
    sdk_rt = '%s',
    action_type = '%s',
    action_count = '%s',
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

    bi_db = Connection(host='dreportm.db.in.xxxxxxxxx.cn', database='bi', user='bi', password='bibi')
    bi_db.execute("DELETE FROM event_report_sdk1x2x WHERE (dt='%s' AND hr=%d) or (dt='%s')" % (formated_date, int(hr), days_ago_30))
    
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

            row = row[:-2] # pop dt and hr
            row.append(formated_date)
            row.append(int(hr))

            sql = INSERT_DATA_SQL % tuple(row)
            #print sql			
            bi_db.execute(sql)
            counter += 1
        except:
            print "load exception", row

    print "%s rows inserted into event_report_sdk1x2x" % counter



if __name__ == "__main__":
    stamp = os.environ.get("DOMINO_STAMP", None)
    if not stamp:
        time = datetime.today() - timedelta(hours=1)
    else: 
        time = datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S")

    load_into_mysql(time)

