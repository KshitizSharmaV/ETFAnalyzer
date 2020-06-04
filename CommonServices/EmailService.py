"""
If ever faced with the exception:
"smtplib.SMTPAuthenticationError: (535, b'5.7.8 Username and Password not accepted)",
1. Check Username/Sender's Email and Password.
2. Check google account for if the access to less secure apps is enabled in the account settings.
"""
import os
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
import smtplib
import socket
import traceback
# the MIMEText class is used to create MIME objects of major type text
from email.mime.text import MIMEText

# A subclass of MIMEBase, this is an intermediate base class for MIME messages that are multipart
from email.mime.multipart import MIMEMultipart


class EmailSender():

    def message(self, subject="Python Notification", text="", img=None, attachment=None):
        # build message contents
        msg = MIMEMultipart()
        msg['Subject'] = subject  # add in the subject
        msg.attach(MIMEText(text))  # add text contents

        # check if we have anything given in the img parameter
        if img is not None:
            # if we do, we want to iterate through the images, so let's check that
            # what we have is actually a list
            if type(img) is not list:
                img = [img]  # if it isn't a list, make it one
            # now iterate through our list
            for one_img in img:
                img_data = open(one_img, 'rb').read()  # read the image binary data
                # attach the image data to MIMEMultipart using MIMEImage, we add
                # the given filename use os.basename
                msg.attach(MIMEImage(img_data, name=os.path.basename(one_img)))

        # we do the same for attachments as we did for images
        if attachment is not None:
            if type(attachment) is not list:
                attachment = [attachment]  # if it isn't a list, make it one
            with open(attachment, 'rb') as f:
                # read in the attachment using MIMEApplication
                file = MIMEApplication(
                    f.read(),
                    name=os.path.basename(attachment)
                )
            # here we edit the attached file metadata
            file['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment)}"'
            msg.attach(file)  # finally, add the attachment to our message object
        return msg

    def send(self, server='smtp-mail.outlook.com', port='587', msg=None, receivers='piyush888@gmail.com'):
        # contain following in try-except in case of momentary network errors
        try:
            # initialise connection to email server, the default is Outlook
            smtp = smtplib.SMTP(server, port)
            # this is the 'Extended Hello' command, essentially greeting our SMTP or ESMTP server
            smtp.ehlo()
            # this is the 'Start Transport Layer Security' command, tells the server we will
            # be communicating with TLS encryption
            smtp.starttls()

            # read email and password from file
            # with open('../data/email.txt', 'r') as fp:
            #     email = fp.read()
            email = 'ticketsoftware2020@gmail.com'
            # with open('../data/password.txt', 'r') as fp:
            #     pwd = fp.read()
            pwd = '9bg8!a41ph'

            # login to outlook server
            smtp.login(email, pwd)
            # send notification to self
            smtp.sendmail(email, receivers, msg.as_string())
            # disconnect from the server
            print('Mail Sent')
            smtp.quit()
        except socket.gaierror:
            print("Network connection error, email not sent.")
            pass
        except Exception as e:
            print(e)
            pass

if __name__=="__main__":
    emailobj = EmailSender()
    msg = emailobj.message(subject="Test Mail Subject", text="Test Mail Body")
    emailobj.send(msg= msg)