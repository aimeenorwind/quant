from email import encoders
from email.header import Header
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr

import smtplib

class MailSend():
    def __init__(self):
        self.from_addr = "zhenzhen.yang@dingxiang-inc.com"
        self.password = "Yangzhen123"
        self.to_addr = "zhenzhen.yang@dingxiang-inc.com"
        self.smtp_server = "smtp.exmail.qq.com"

    def _format_addr(self, s):
        name, addr = parseaddr(s)
        return formataddr((Header(name, 'utf-8').encode(), addr))

    def send_mail(self, message):
        # msg = MIMEText('hello, send by Python...', 'plain', 'utf-8')
        msg = MIMEText(message, 'plain', 'utf-8')
        msg['From'] = self._format_addr('Scheduler <%s>' % self.from_addr)
        msg['To'] = self._format_addr('杨真真 <%s>' % self.to_addr)
        msg['Subject'] = Header('WindLoad周期调度信息', 'utf-8').encode()

        server = smtplib.SMTP(self.smtp_server, 25)
        # server.set_debuglevel(1)
        server.login(self.from_addr, self.password)
        server.sendmail(self.from_addr, [self.to_addr], msg.as_string())
        server.quit()

