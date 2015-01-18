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

FIND_PARTITION_SQL = "SELECT * FROM information_schema.PARTITIONS WHERE table_name='imp_report_summary' and partition_name=%s;"

ADD_PARTITION_SQL = "ALTER TABLE imp_report_summary ADD PARTITION(PARTITION %s VALUES in (%d));"

def query_by_hive_cmd(hql, db_name):
    """
    query hive using hive cmd line tool, capture result.
    """
    print hql
    hql = "use %s; SET mapred.fairscheduler.pool=urgent; %s" % (db_name, hql)
    try:
        raw_result = check_output([HIVE_CMD, "-e", hql])
    except CalledProcessError:
        print "query hive failed"
        sys.exit(-1)

    query_result = [ line.split("\t") for line in raw_result.split("\n")[:-1] ]
    return query_result


GROUP_HQL = """
SELECT
    coalesce(deverid,0),
    coalesce(mediaid,0),
    coalesce(os,0),
    coalesce(sponsorid,0),
    coalesce(adcreativeid,0),
    coalesce(adplacementtype.value, 0),
    coalesce(costtype, 0),
    case when isspam then 1 else 0 end,
    case when ischarge then 1 else 0 end,
    count(1) as imp_report_count,
    isv,
    coalesce(phase, 0),
    mediatype.value,
    dt,
    hr
FROM %s where dt=%s and hr=%s
GROUP BY
    deverid,
    mediaid,
    os,
    sponsorid,
    adcreativeid,
    adplacementtype.value,
    costtype,
    isspam,
    ischarge,
    isv,
    phase,
    mediatype.value,
    dt,
    hr
"""

INSERT_DATA_SQL = """
INSERT INTO imp_report_summary
SET
    deverid = '%s',
    mediaid = '%s',
    os = '%s',
    sponsorid = '%s',
    adcreativeid = '%s',
    adplacementtype = '%s',
    costtype = '%s',
    isspam = '%s',
    ischarge = '%s',
    imp_report_count = '%s',
    sv = '%s',
    phase = '%s',
    mediatype = %d,
    dt = '%s',
    hr = '%s'
"""


def load_into_mysql(time):
    time = time - timedelta(hours=1)
    print "use_time %s" % time
    dt = time.strftime("%Y%m%d")
    formated_date = time.strftime("%Y-%m-%d")
    hr = time.strftime("%H")
    days_ago_30 = (time - timedelta(days=30)).strftime("%Y-%m-%d")
    query_result = query_by_hive_cmd(GROUP_HQL % ('imp_report_total',dt, hr), 'bi')
    #query_result_cpc = query_by_hive_cmd(GROUP_HQL % ('imp_report',dt, hr), 'bi')
    #query_result_cpm = query_by_hive_cmd(GROUP_HQL % ('cpm_imp_report',dt, hr), 'cpm')


    bi_db = Connection(host='dreportm.db.in.xxxxxxxxx.cn', database='bi', user='bi', password='bibi')
    bi_db.execute("DELETE FROM imp_report_summary WHERE (dt='%s' AND hr=%d)  or (dt='%s')" % (formated_date, int(hr), days_ago_30))
    
    days = bi_db.get(TO_DAYS_SQL, formated_date)
    partition_name = "p%d" % days.days
    partition_info = bi_db.get(FIND_PARTITION_SQL, partition_name)
    if not partition_info :
        bi_db.execute(ADD_PARTITION_SQL % (partition_name, days.days))

    counter = 0
    for row in query_result:                        #(query_result_cpc + query_result_cpm):
        sql = ""
        try:
            row = map(unquote, row)
            try:
                row = map(unicode, row)
            except:
                row = map(lambda f: f.decode('utf8'), row)

            if row[-4] == 'NULL':
                row[-4] = '0'
            else:
                row[-4] = str(row[-4])
            if row[-3] == 'NULL':
                row[-3] = 0
            else:
                row[-3] = int(row[-3])
            row = row[:-2] # pop dt and hr
            row.append(formated_date)
            row.append(hr)

            sql = INSERT_DATA_SQL % tuple(row)
            #print sql			
            bi_db.execute(sql)
            counter += 1
        except:
            print "load exception", row

    print "%s rows inserted into imp_report_summary" % counter



if __name__ == "__main__":
    stamp = os.environ.get("DOMINO_STAMP", None)
    if not stamp:
        time = datetime.today() - timedelta(hours=1)
    else: 
        time = datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S")

    load_into_mysql(time)

