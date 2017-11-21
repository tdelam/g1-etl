import urllib2
import logging
import logging.handlers


logging.basicConfig(filename="g1-etl-members.log", level=logging.INFO)
log = logging.getLogger("g1-etl-members")


def download_images(env, user_id, pic):
    """
    Downloading MMJ with progress logging
    """
    remote = (r"https://wm-mmjmenu-images-{0}.s3.amazonaws.com/"
              "customers/pictures/{1}/large/{2}").format(env, user_id, pic)
    local = str(user_id) + ".jpg"

    url = urllib2.urlopen(remote)
    header = url.info()
    total_size = int(header["Content-Length"])

    log.info("---"
             "Downloading {0} bytes of image {1}...".format(total_size, pic))
    fp = open(local, 'wb')

    block_size = 8192
    count = 0
    while True:
        chunk = url.read(block_size)
        if not chunk:
            break
        fp.write(chunk)
        count += 1
        if total_size > 0:
            percent = int(count * block_size * 100 / total_size)
            if percent > 100:
                percent = 100
            log.info("%2d%%" % percent)
            if percent < 100:
                log.info("\b\b\b\b\b")  # Erase "NN% "
            else:
                log.info("---"
                         "{0} successfully downloaded.".format(pic))

    fp.flush()
    fp.close()
    if not total_size:
        log.error("--"
                  "Error: File {0} did not successfully download".format(pic))
