import configparser


def config_factory(config_path: str='./config.ini'):
    config = configparser.ConfigParser()
    config.read(config_path)
    return config
