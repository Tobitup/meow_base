
import smtplib

from typing import Any, Dict

from meow_base.functionality.meow import replace_keywords

def get_notification_message_with_subs(message:str, job_id:str, 
        event:Dict[str,Any])->str:
    subbed_dict = replace_keywords(
        {"msg":message},
        job_id,
        event
    )
    
    return subbed_dict["msg"]

def send_email(host:str, sender:str, reciever:str, message:str,)->None:
    """Sends an email according to the given parameters. Note that this is a 
    very barebones proof-of-concept implementation and does not currently 
    provide any of the necessary certificates etc for this to work out of box 
    unless a user has manually set them up beforehand."""
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
