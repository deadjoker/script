#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import subprocess
import json
import datetime

import matplotlib
matplotlib.use('Agg')
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.header import Header


# get all buckets' information
def get_buckets():
    cmd = ['radosgw-admin', 'bucket', 'stats']
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    data = p.communicate()
    buckets = json.loads(data[0])
    return buckets


# get bucket_name, bucket_usage_in_gb, number_of_objects of the bucket
def get_bucket_stats(bucket):
    bkt = bucket.get('bucket')

    if bucket.get('usage') == {}:
        usg = 0
        obj = 0
    else:
        usg = bucket.get('usage').get('rgw.main').get('size_kb_utilized') / 1024 / 1024 
        obj = bucket.get('usage').get('rgw.main').get('num_objects')

    return bkt, usg, obj


# save date:usage:object in files
def save_file(bkt, usg, obj):
    new_line = timestamp + ':' + str(usg) + ':' + str(obj) + '\n'

    if not os.path.exists(ftxt):
        with open(ftxt, 'a') as f:
            f.write(new_line)
    else:
        with open(ftxt, 'r') as f:
            lines = f.readlines()
  
        while len(lines) > 30:
            lines = lines[1:]
  
        with open(ftxt, 'w') as f:
            for line in lines:
                f.write(line)
            f.write(new_line)


# read date, usage and object from file, return each list
def fmt_point():
    x, y, z = [], [], []

    with open(ftxt, 'r') as f:
        lines = f.readlines()

    for line in lines:
        line = line.split(':')

        x.append(line[0])
        y.append(line[1])
        z.append(line[2].strip())

    return x, y, z


# plot the image
def img_plot(bucket_name, x, y1, y2):
    x_date = [datetime.datetime.strptime(d, '%Y-%m-%d').date() for d in x]
  
    plt.figure()
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator())
    plt.xticks(pd.date_range(ts_start, timestamp, freq='1d'))
  
    plt.subplot(2,1,1)
    plt.plot(x_date, y1)
    plt.title(bucket_name)
    plt.xlabel('Date')
    plt.ylabel('Usage (GB)')
    
    plt.subplot(2,1,2)
    plt.plot(x_date, y2, color='red')
    plt.xlabel('Date')
    plt.ylabel('Files')
  
    plt.gcf().autofmt_xdate() 
    plt.savefig(fimg)
  

class Mail(object):
    def __init__(self, host, port, nickname, username, password, postfix):
        self.host = host
        self.port = port
        self.nickname = nickname
        self.username = username
        self.password = password
        self.postfix = postfix

    def send_mail(self, to_list, subject, content, cc_list=[], encode='gbk', is_html=True, tables=[], images=[]):
        me = str(Header(self.nickname, encode)) + "<" + self.username + "@" + self.postfix + ">"
        msg = MIMEMultipart()
        msg['Subject'] = Header(subject, encode)
        msg['From'] = me
        msg['To'] = ','.join(to_list)
        msg['Cc'] = ','.join(cc_list)
        if is_html:
            # 添加表
            mail_msg = '<table border=1><tr><th>bucket</th><th>usage(GB)</th><th>objects</th></tr>'
            for tab in tables:
                mail_msg = mail_msg + '<tr><td>%s</td><td>%s</td><td>%s</td></tr>' % (tab[0], tab[1], tab[2])
            mail_msg = mail_msg + '</table>'

            # 添加图片
            for i in range(len(images)):
                mail_msg += '<p><img src="cid:image%d"></p>' % (i+1)
            msg.attach(MIMEText(content + mail_msg, 'html', 'utf-8'))

            for i, img_name in enumerate(images):
                with open(img_name, 'rb') as fp:
                    img_data = fp.read()
                msg_image = MIMEImage(img_data)
                msg_image.add_header('Content-ID', '<image%d>' % (i+1))
                msg.attach(msg_image)
                # 将图片作为附件
                # image = MIMEImage(img_data, _subtype='octet-stream')
                # image.add_header('Content-Disposition', 'attachment', filename=images[i])
                # msg.attach(image)
        else:
            msg_content = MIMEText(content, 'plain', encode)
            msg.attach(msg_content)

        try:
            s = smtplib.SMTP()
            # s.set_debuglevel(1)
            s.connect(self.host, self.port)
            s.starttls()
            s.login(self.username, self.password)
            s.sendmail(me, to_list + cc_list, msg.as_string())
            s.quit()
            s.close()
            return True
        except Exception as e:
            print(e)
            return False


def send_mail(to_list, title, content, cc_list=[], encode='utf-8', is_html=True, tables=[], images=[]):
    content = '<pre>%s</pre>' % content
    nickname = 'admin'
    username = 'admin@abc.com'
    password = 'password'
    m = Mail('smtp.abc.com', '587', nickname, username, password, 'abc.com')
    m.send_mail(to_list, title, content, cc_list, encode, is_html, tables, images)


if __name__ == '__main__':

    work_dir = os.path.dirname(os.path.abspath('__file__'))
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d")
    ts_start = (datetime.datetime.now() - datetime.timedelta(days = 30)).strftime("%Y-%m-%d")
    buckets = get_buckets()
    tables = []

    for bucket in buckets:
        bkt, usg, obj = get_bucket_stats(bucket)

        ftxt = os.path.join(work_dir, bkt + '.txt')
        fimg = os.path.join(work_dir, bkt + '-' + timestamp + '.png')

        save_file(bkt, usg, obj)

        x_dt, y_usg, y_obj = fmt_point()
        img_plot(bkt, x_dt, y_usg, y_obj)

        tab = [bkt, usg, obj]
        tables.append(tab)

    images = [ i for i in os.listdir(work_dir) if i.endswith('png')]
    title = u'[daily] bucket usage - ceph 对象集群'
    content = u'至 %s 使用情况: \n' % timestamp
    send_mail(['a@abc.com'], title, content, ['b@abc.com', 'c@abc.com'], 'utf-8', True, tables, images)

    for img in images:
        os.remove(img)


