
import smtplib

def send_email(host:str, sender:str, reciever:str, message:str,)->None:
    try:         
        port:int=smtplib.SMTP_PORT
        if ':' in host:
            tmp = host
            host = tmp[:tmp.index(':')]
            port = int(tmp[tmp.index(':')+1:])

        smtpObj = smtplib.SMTP(host, port)
        smtpObj.sendmail(sender, reciever, message)
    except Exception as e:
        pass
