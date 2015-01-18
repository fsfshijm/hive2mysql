#!/usr/local/bin/python2.7
# -*- coding: utf8 -*-

import sys
import os
from datetime import datetime, timedelta
from urllib import unquote
from subprocess import check_output, CalledProcessError

scpath=os.path.dirname(__file__)
basepath = os.path.realpath(os.path.dirname(__file__)+'/../')
print basepath
sys.path.append(basepath + "/lib/")
from database import Connection

HIVE_CMD = '/home/hadoop/hive/bin/hive'


TO_DAYS_SQL = "select to_days(%s) as days;"

FIND_PARTITION_SQL = "SELECT * FROM information_schema.PARTITIONS WHERE table_name='rtb_click' and partition_name=%s;"

ADD_PARTITION_SQL = "ALTER TABLE rtb_click ADD PARTITION(PARTITION %s VALUES in (%d));"

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
coalesce(exchange_id,0),
coalesce(ip,''),
coalesce(eip,''),
coalesce(sip,''),
coalesce(search_id,0),
coalesce(req_id,''),
coalesce(imp_id,''),
coalesce(expid,0),
coalesce(sub_expid,0),
coalesce(req_ts,0),
coalesce(bid_ts,0),
coalesce(win_ts,0),
coalesce(clk_ts,0),
coalesce(sponsor_id,0),
coalesce(campaign_id,0),
coalesce(creative_id,0),
coalesce(template_id,0),
coalesce(creative_type,0),
coalesce(ad_cost_type,0),
coalesce(ad_template_size,0),
coalesce(ad_app_bundle,''),
coalesce(ad_app_ver,''),
coalesce(adnetwork_id,''),
coalesce(publisher_id,''),
coalesce(media_id,''),
coalesce(media_channel,0),
coalesce(media_pageurl,''),
coalesce(app_bundle,''),
coalesce(app_paid,0),
coalesce(app_ver,''),
coalesce(adp_id,''),
coalesce(adp_dim,0),
coalesce(video_linearity,0),
coalesce(video_minduration,0),
coalesce(video_maxduration,0),
coalesce(bidfloor,0),
coalesce(bid_currency,0),
coalesce(bid_auctiontype,0),
coalesce(bid,0),
coalesce(nobid_reason,0),
coalesce(win_price,0),
coalesce(act_price,0),
coalesce(dev_make,''),
coalesce(dev_model,''),
coalesce(dev_osver,''),
coalesce(carrier,''),
coalesce(language,''),
coalesce(connectiontype,0),
coalesce(devicetype,0),
coalesce(screen_dim,0),
coalesce(screen_density,0.0),
coalesce(jailbreak,0),
coalesce(geo_lat,0.0),
coalesce(geo_lon,0.0),
coalesce(geo_country,0),
coalesce(geo_region,0),
coalesce(geo_city,0),
coalesce(geo_zip,0),
coalesce(geo_type,0),
coalesce(didmd5,''),
coalesce(dpidmd5,''),
coalesce(mac,''),
coalesce(macmd5,''),
coalesce(idfa,''),
coalesce(ssid,''),
coalesce(imei,''),
coalesce(imeimd5,''),
coalesce(openudid,''),
coalesce(user_id,''),
coalesce(dm_platform,0),
coalesce(dm_carrier_id,0),
coalesce(dm_device_id,0),
coalesce(dm_os_id,0),
coalesce(dm_accesstype_id,0),
coalesce(dm_geo_id,0),
coalesce(dm_uid,0),
coalesce(dm_media_id,0),
coalesce(dm_ext,''),
dt,
hr
FROM %s where dt=%s and hr=%s
"""

INSERT_DATA_SQL = """
INSERT INTO rtb_click
SET
exchangeid='%s',
ip='%s',
eip='%s',
sip='%s',
search_id='%s',
req_id='%s',
imp_id='%s',
expid='%s',
sub_expid='%s',
req_ts='%s',
bid_ts='%s',
win_ts='%s',
clk_ts='%s',
sponsor_id='%s',
campaign_id='%s',
creative_id='%s',
creative_tpl='%s',
creative_type='%s',
ad_cost_type='%s',
ad_template_size='%s',
ad_app_bundle='%s',
ad_app_ver='%s',
adnetwork_id='%s',
publisher_id='%s',
media_id='%s',
media_channel='%s',
media_pageurl='%s',
app_bundle='%s',
app_paid='%s',
app_ver='%s',
adp_id='%s',
adp_dim='%s',
video_linearity='%s',
video_minduration='%s',
video_maxduration='%s',
bidfloor='%s',
bid_currency='%s',
bid_auctiontype='%s',
bid='%s',
nobid_reason='%s',
win_price='%s',
act_price='%s',
dev_make='%s',
dev_model='%s',
dev_osver='%s',
carrier='%s',
language='%s',
connectiontype='%s',
devicetype='%s',
screen_dim='%s',
screen_density='%s',
jailbreak='%s',
geo_lat='%s',
geo_lon='%s',
geo_country='%s',
geo_region='%s',
geo_city='%s',
geo_zip='%s',
geo_type='%s',
didmd5='%s',
dpidmd5='%s',
mac='%s',
macmd5='%s',
idfa='%s',
ssid='%s',
imei='%s',
imeimd5='%s',
openudid='%s',
user_id='%s',
dm_platform='%s',
dm_carrier_id='%s',
dm_device_id='%s',
dm_os_id='%s',
dm_accesstype_id='%s',
dm_geo_id='%s',
dm_uid='%s',
dm_media_id='%s',
dm_ext='%s',
dt='%s',
hr='%s'
"""


def load_into_mysql(time):
    print "use_time %s" % time
    dt = time.strftime("%Y%m%d")
    formated_date = time.strftime("%Y-%m-%d")
    hr = time.strftime("%H")
    days_ago_30 = (time - timedelta(days=30)).strftime("%Y-%m-%d")
    query_result = query_by_hive_cmd(GROUP_HQL % ('log_clk',dt, hr), 'rtb')


    bi_db = Connection(host='dreportm.db.in.xxxxxxxxx.cn', database='bi', user='bi', password='bibi')
    bi_db.execute("DELETE FROM rtb_click WHERE (dt='%s' AND hr=%d) or (dt='%s') " % (formated_date, int(hr), days_ago_30))
    
    days = bi_db.get(TO_DAYS_SQL, formated_date)
    partition_name = "p%d" % days.days
    partition_info = bi_db.get(FIND_PARTITION_SQL, partition_name)
    if not partition_info :
        bi_db.execute(ADD_PARTITION_SQL % (partition_name, days.days))

    counter = 0
    for row in query_result:                        #(query_result_cpc + query_result_cpm):
        sql = ""
        try:
	    #print row
            row = map(unquote, row)
            try:
                row = map(unicode, row)
            except:
		try:
                   row = map(lambda f: f.decode('utf8'), row)
                except Exception as e:
                   print e

            row = row[:-2] # pop dt and hr
            row.append(formated_date)
            row.append(hr)
            #print row
            sql = INSERT_DATA_SQL % tuple(row)
            #print sql			
            bi_db.execute(sql)
            counter += 1
        except Exception as e:
	    print e
            print "load exception", row

    print "%s rows inserted into rtb_click" % counter



if __name__ == "__main__":
    stamp = os.environ.get("DOMINO_STAMP", None)
    if not stamp:
        time = datetime.today() - timedelta(hours=1)
    else: 
        time = datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S") - timedelta(hours=1)

    load_into_mysql(time)

