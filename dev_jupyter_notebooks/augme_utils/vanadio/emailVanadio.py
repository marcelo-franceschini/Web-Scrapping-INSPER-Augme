class AugmeMail(object):

    def __enter__(self):
        return self

    def __exit__(self, receiver, subject, body):
        return self
    
    def send_mail(receiver, subject, body):
        pass