#!/usr/bin/env python
# -*- coding: utf-8 -*-

import smtplib
import sys
from email.mime.text import MIMEText 
from email.mime.multipart import MIMEMultipart

class Message(object):
    def __init__(self):
        self.peer = MIMEMultipart()
        self._message = None
    def __setitem__(self, k, v):
        self.peer[k] = v
    def __getitem__(self, k):
        return self.peer[k]
    def as_string(self):
        return self.peer.as_string()
    @property
    def message(self):
        return self._message
    @message.setter
    def message(self, msg):
        assert(self._message == None)
        self._message = msg
        self.peer.attach(MIMEText(msg))
        
class Mailer(object):
    def __init__(self):
        self.smtp = smtplib.SMTP()
    def connect(self, server):
        self.smtp.connect(server)
    def login(self, user, password):
        self.smtp.login(user, password)
    def send_message(self, message):
        #self.smtp.sendmail(message['From'], message['To'], message.as_string())
        self.smtp.sendmail(message['From'], to_list, message.as_string())
    def close(self):
        self.smtp.close()

def mail_message(message):
    mailer = Mailer()
    mailer.connect("smtp.126.com")
    mailer.login("nextomics", "nextomics-email") 
    mailer.send_message(message)
    mailer.close()


if __name__ == "__main__":

    to_list = ['lijingjing@grandomics.com','fengli@grandomics.com','liangf@grandomics.com','lien@grandomics.com','lixiaokang@grandomics.com','wangjiajia@grandomics.com','qinjh@grandomics.com','pm-receive@grandomics.com','leiyuting@grandomics.com','wangyamin@grandomics.com','julongzhen@grandomics.com','duanxiaoke@grandomics.com','wangf2@grandomics.com']
    mailer = Mailer()
    mailer.connect("smtp.126.com")
    mailer.login("nextomics", "nextomics-email") 
    message = Message()
    message["From"]    = "nextomics@126.com"
    message["To"]      = ','.join(to_list) 
    message["Subject"] = "HiFi Call CCS Cell ID"
    message.message    = open(sys.argv[1],encoding="utf-8").read()
    mailer.send_message(message)


