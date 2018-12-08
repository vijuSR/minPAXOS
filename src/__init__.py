import logging


# not providing the 'datefmt' will include the
# milliseconds as well which is required.
logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] [%(name)-20s] [%(levelname)-10s] (%(message)s)',
)

logging.debug('loggers ready to use!')
