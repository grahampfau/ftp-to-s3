import logging
import os
import threading
from queue import Queue

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer

from konfig import Konfig

log_format = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=log_format)
log= logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

konf = Konfig()

root_log_level = logging.getLevelName(konf.root_log_level)
root_log= logging.getLogger()
root_log.setLevel(root_log_level)
ftp_port = int(konf.ftp_port)
passive_port_lower = int(konf.passive_port_lower)
passive_port_upper = int(konf.passive_port_upper) + 1
passive_range = range(passive_port_lower, passive_port_upper)
s3_connection = S3Connection(konf.aws_access_key_id,
                             konf.aws_secret_access_key)
s3_bucket = s3_connection.get_bucket(konf.aws_bucket_name)

job_queue = Queue()


def process_file(filename):
    # Upload to S3, get URL
    s3_key = Key(s3_bucket)
    s3_key.key = filename.split(os.getcwd() + '/ftp')[-1]
    s3_key.set_contents_from_filename(filename)
    s3_key.set_acl('public-read')
    url = s3_key.generate_url(expires_in=86400)  # 1 day
    log.debug(("File now in S3 at: {}".format(url)))
    # Delete file
    os.unlink(filename)
    log.debug(("Deleted file: {}".format(filename)))


class FTPWorker(threading.Thread):
    def __init__(self, q):
        self.q = q
        threading.Thread.__init__(self)

    def run(self):
        log.debug("Worker online")
        while True:
            log.debug(
                "Worker waiting for job ... %s" % str(job_queue.qsize()))
            filename = job_queue.get()
            log.debug("Worker got job: %s, qsize: %s" % (
                filename,
                str(job_queue.qsize())))
            try:
                process_file(filename)
                log.debug("Task done, qsize: %s" % str(job_queue.qsize()))
            except Exception as e:
                log.error("Task failed with error: %s" % str(e))
            finally:
                job_queue.task_done()


class FTPHandler(FTPHandler):
    def on_file_received(self, filename):
        job_queue.put(filename)


def main():
    # Instantiate a dummy authorizer for managing 'virtual' users
    authorizer = DummyAuthorizer()

    # Define a new user having full r/w permissions
    authorizer.add_user(konf.ftp_username,
                        konf.ftp_password,
                        'ftp/',
                        perm='elradfmwM')

    # Instantiate FTP handler class
    handler = FTPHandler
    handler.permit_foreign_addresses = True
    handler.passive_ports = passive_range
    handler.authorizer = authorizer

    # Define a customized banner (string returned when client connects)
    handler.banner = "pyftpdlib based ftpd ready."

    # Instantiate FTP server class and listen on 0.0.0.0:2121
    address = ('', ftp_port)
    server = FTPServer(address, handler)

    # set a limit for connections
    log.debug('Max number of connections: ' + str(len(passive_range)))
    server.max_cons = len(passive_range)
    server.max_cons_per_ip = len(passive_range)

    # start ftp server
    server.serve_forever()

if __name__ == '__main__':
    # Restore directories from S3 bucket
    for item in s3_bucket.list():
        directory = 'ftp/' + item.name.rsplit('/', 1)[0]
        if not os.path.exists(directory):
            log.debug('Restoring directory: ' + directory)
            os.makedirs(directory, exist_ok=True)
    for i in range(0, 4):
        t = FTPWorker(job_queue)
        t.daemon = True
        t.start()
        log.debug("Started worker")
    main()
