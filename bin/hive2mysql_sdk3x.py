#!/usr/local/bin/python2.7
# -*- coding: utf8 -*-

import sys
import os
from datetime import datetime, timedelta
from urllib import unquote
from subprocess import check_output, CalledProcessError
import xxxxxxxxx_pyutils.mysql_util as MU

basepath = os.path.realpath(os.path.dirname(__file__)+'/../')
sys.path.append(basepath+"/lib/")

from database import Connection

HIVE_CMD = '/home/hadoop/hive/bin/hive'


TO_DAYS_SQL = "select to_days(%s) as days;"

FIND_PARTITION_SQL = "SELECT * FROM information_schema.PARTITIONS WHERE table_name='event_report_sdk3x' and partition_name=%s;"

ADD_PARTITION_SQL = "ALTER TABLE event_report_sdk3x ADD PARTITION(PARTITION %s VALUES in (%d));"

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
    coalesce(dever_id,0),
    coalesce(media_id,0),
    coalesce(sponsor_id,0),
    coalesce(ad_plan_id,0),
    coalesce(ad_strategy_id,0),
    coalesce(ad_creative_id,0),
    coalesce(ad_placement_type, 0),
    coalesce(cost_type, 0),
    report_type,
    publish_id,
    origin,
    type,
    count(1) as action_count,
    pkg,
    vc,
    vn,
    sv,
    sdktype,
    mediatype.value,
    dt,
    hr
FROM event_report where dt=%s and hr=%s
GROUP BY
    dever_id,
    media_id,
    sponsor_id,
    ad_plan_id,
    ad_strategy_id,
    ad_creative_id,
    ad_placement_type,
    cost_type,
    report_type,
    publish_id,
    origin,
    type,
    pkg,
    vc,
    vn,
    sv,
    sdktype,
    mediatype.value,
    dt,
    hr
"""

INSERT_DATA_SQL = """
INSERT INTO event_report_sdk3x
SET
    dever_id = '%s',
    media_id = '%s',
    sponsor_id = '%s',
    ad_plan_id = '%s',
    ad_strategy_id = '%s',
    ad_creative_id = '%s',
    ad_placement_type = '%s',
    cost_type = '%s',
    report_type = '%s',
    publish_id = '%s',
    origin = '%s',
    action_type = '%s',
    action_count = '%s',
    pkg = '%s',
    vc = '%s',
    vn = '%s',
    sv = '%s',
    sdk_type = '%s',
    media_type = %s,
    use_time = '%s %s:00:00',
    dt = '%s',
    hr = %s
"""


def load_into_mysql(time):
    MU.registerConnection('bi', host="dreportm.db.in.xxxxxxxxx.cn", port=3306, user='bi', passwd='bibi', db='bi', pool_size=8)
    conn = MU.getConnection('bi')
    conn.autocommit(True)
    time = time - timedelta(hours=1)
    print "use_time %s" % time
    dt = time.strftime("%Y%m%d")
    formated_date = time.strftime("%Y-%m-%d")
    hr = time.strftime("%H")
    days_ago_30 = (time - timedelta(days=30)).strftime("%Y-%m-%d")
    query_result = query_by_hive_cmd(GROUP_HQL % (dt, hr))

    #bi_db = Connection(host='dreportm.db.in.xxxxxxxxx.cn', database='bi', user='bi', password='bibi')
    #bi_db.execute("DELETE FROM event_report_sdk3x WHERE (dt='%s' AND hr=%d) or (dt='%s')" % (formated_date, int(hr), days_ago_30))
    aaa = MU.execute('bi',"DELETE FROM event_report_sdk3x WHERE (dt='%s' AND hr=%d) or (dt='%s')" % (formated_date, int(hr), days_ago_30))
    MU.execute('bi', "commit")
    print aaa
    #MU.execute('bi', "DELETE FROM event_report_sdk3x WHERE dt='%s'", (days_ago_30))
    
    #days = bi_db.get(TO_DAYS_SQL, formated_date)
    #partition_name = "p%d" % days.days
    #partition_info = bi_db.get(FIND_PARTITION_SQL, partition_name)
    days = MU.fetchone('bi', TO_DAYS_SQL, formated_date)
    partition_name = "p%d" % days[0]
    print 'partition_name: ', partition_name
    partition_info = MU.fetchone('bi',FIND_PARTITION_SQL, partition_name)
    print 'partition_info: ', partition_info
    if not partition_info :
        print  'aaaa'
        #bi_db.execute(ADD_PARTITION_SQL % (partition_name, days.days))
        MU.execute('bi', ADD_PARTITION_SQL % (partition_name, days[0]))
        MU.execute('bi', "commit")

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

            if row[-4] == 'NULL':
                row[-4] = 1
            if row[-3] == 'NULL':
                row[-3] = 0
            else:
                row[-3] = int(row[-3])
            row = row[:-2] # pop dt and hr
            row.append(formated_date)
            row.append(hr)
            row.append(formated_date)
            row.append(int(hr))

            sql = INSERT_DATA_SQL % tuple(row)
            #print sql			
            #bi_db.execute(sql)
            MU.execute('bi', sql)
            MU.execute('bi', "commit")
            counter += 1
        except Exception,e:
            print "load exception", row
            print e

    print "%s rows inserted into event_report" % counter



if __name__ == "__main__":
    stamp = os.environ.get("DOMINO_STAMP", None)
    if not stamp:
        time = datetime.today() - timedelta(hours=1)
    else: 
        time = datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S")

    load_into_mysql(time)

