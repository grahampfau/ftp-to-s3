import logging
import os
import threading
from queue import Queue

from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError
from boto.s3.key import Key
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer

from konfig import Konfig

log_format = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=log_format)
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

konf = Konfig()

root_log_level = logging.getLevelName(konf.root_log_level)
root_log = logging.getLogger()
root_log.setLevel(root_log_level)
ftp_port = int(konf.ftp_port)
passive_port_lower = int(konf.passive_port_lower)
passive_port_upper = int(konf.passive_port_upper) + 1
passive_range = range(passive_port_lower, passive_port_upper)
s3_connection = S3Connection(konf.aws_access_key_id,
                             konf.aws_secret_access_key)
s3_bucket = s3_connection.get_bucket(konf.aws_bucket_name)

job_queue = Queue()


def restore_bucket():
    # Restore contents from S3 bucket
    dirs = [i.name.rsplit('/', 1)[0] for i in s3_bucket.list()]
    for dir in dirs:
        restore_dir(dir)
    for item in s3_bucket.list():
        if not item.name.endswith('/'):
            restore_file(item)


def restore_dir(item):
    directory = 'ftp/' + item
    if not os.path.exists(directory):
        log.debug('Restoring directory: %s' % directory)
        os.makedirs(directory, exist_ok=True)


def restore_file(item):
    filename = 'ftp/' + item.name
    if not os.path.exists(filename):
        log.debug('Restoring file: %s' % filename)
        item.get_contents_to_filename(filename)


def get_local_path(path):
    exclude_path = os.getcwd() + '/ftp/'
    return path.split(exclude_path)[-1]


def upload_file(filename):
    # Upload to S3, get URL
    if not os.path.exists(filename):
        return
    s3_key = Key(s3_bucket)
    s3_key.key = filename.split(os.getcwd() + '/ftp')[-1]
    s3_key.set_contents_from_filename(filename)
    try:
        s3_key.set_acl('public-read')
        url = s3_key.generate_url(expires_in=86400)  # 1 day
        log.debug(('File now in S3 at: {}'.format(url)))
    except S3ResponseError:
        # There are race conditions during rename where a file
        # can be deleted mid-upload in S3 by another worker.
        log.warning('File deleted mid-upload: %s Retrying.' % s3_key.key)
        upload_file(filename)


def rename(src_full, dest_full):
    src = get_local_path(src_full)
    dest = get_local_path(dest_full)
    if os.path.isfile(dest_full):
        log.debug('Moving file in S3: %s to %s' % (src, dest))
        try:
            s3_bucket.copy_key(dest, s3_bucket.name, src)
        except S3ResponseError:
            log.warning('Source file not found in S3: %s' % src)
            upload_file(dest_full)
        s3_bucket.delete_key(src)
    elif os.path.isdir(dest_full):
        log.debug('Moving folder in S3: %s to %s' % (src, dest))
        # We're removing write access during folder move to avoid
        # race conditions.
        authorizer.override_perm(konf.ftp_username,
                            dest_full,
                            perm='elr',
                            recursive=True)
        for item in s3_bucket.list(prefix=src.strip('/') + '/'):
            filename = item.name.split(src)[-1]
            s3_bucket.copy_key(
                    dest + filename,
                    s3_bucket.name,
                    src + filename)
            item.delete()
            log.debug('Moved S3 item: %s' % dest + filename)
        # Now that we're done with the move, we restore access.
        authorizer.override_perm(konf.ftp_username,
                            dest_full,
                            perm='elradfmwM',
                            recursive=True)


def delete(path):
    if path:
        local_path = get_local_path(path)
        s3_bucket.delete_key(local_path)
        log.debug('Deleted S3 file: %s ' % local_path)


def mk_dir(path):
    if path:
        local_path = get_local_path(path)
        s3_dir = s3_bucket.new_key(local_path + '/')
        s3_dir.set_contents_from_string('')
        log.debug('New S3 folder: %s' % local_path)


def rm_dir(path):
    if path:
        local_path = get_local_path(path)
        for item in s3_bucket.list(prefix=local_path.strip('/') + '/'):
            item.delete()
            log.debug('Deleted S3 item: %s ' % item.name)


class FTPWorker(threading.Thread):

    def __init__(self, q, worker_id):
        self.q = q
        self.worker_id = worker_id
        threading.Thread.__init__(self)

    def run(self):
        log.debug('Worker %i online' % self.worker_id)
        while True:
            log.debug('Worker %i waiting for job ... %i' % (
                    self.worker_id,
                    job_queue.qsize()))
            func = job_queue.get()
            log.debug('Worker %i got job: %s, qsize: %i' % (
                self.worker_id,
                func,
                job_queue.qsize()))
            try:
                func()
                log.debug('Worker %i task done, qsize: %i' % (
                    self.worker_id,
                    job_queue.qsize()))
            except Exception as e:
                log.error('Worker %i task failed with error: %s' % (
                    self.worker_id,
                    str(e)))
            finally:
                job_queue.task_done()


class CustomHandler(FTPHandler):

    def on_file_received(self, filename):
        job_queue.put(lambda: upload_file(filename))

    def ftp_MKD(self, path):
        path = FTPHandler.ftp_MKD(self, path)
        job_queue.put(lambda: mk_dir(path))

    def ftp_RMD(self, path):
        FTPHandler.ftp_RMD(self, path)
        job_queue.put(lambda: rm_dir(path))

    def ftp_DELE(self, path):
        path = FTPHandler.ftp_DELE(self, path)
        job_queue.put(lambda: delete(path))

    def ftp_RNTO(self, path):
        src_full, dest_full = FTPHandler.ftp_RNTO(self, path)
        job_queue.put(lambda: rename(src_full, dest_full))


def main():
    # Instantiate a dummy authorizer for managing 'virtual' users
    # We need 'global' access for the rename function.
    global authorizer
    authorizer = DummyAuthorizer()

    # Define a new user having full r/w permissions
    authorizer.add_user(konf.ftp_username,
                        konf.ftp_password,
                        'ftp/',
                        perm='elradfmwM')

    # Instantiate FTP handler class
    handler = CustomHandler
    handler.permit_foreign_addresses = True
    handler.passive_ports = passive_range
    handler.authorizer = authorizer

    # Define a customized banner (string returned when client connects)
    handler.banner = 'pyftpdlib based ftpd ready.'

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
    job_queue.put(lambda: restore_bucket())
    for i in range(0, 4):
        t = FTPWorker(job_queue, i)
        t.daemon = True
        t.start()
        log.debug('Started worker')
    main()
