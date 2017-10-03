import logging


def configure_logging(level=logging.DEBUG):
    logging.basicConfig(
        level=level,
        datefmt='%d.%m.%y %H:%M:%S',
        format='%(asctime)s %(levelname)s %(name)s: %(message)s'
    )
