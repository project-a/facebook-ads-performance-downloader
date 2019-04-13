"""Make the functionalities of this package auto-discoverable by mara-app"""


def MARA_CONFIG_MODULES():
    from facebook_downloader import config
    return [config]


def MARA_CLICK_COMMANDS():
    from facebook_downloader import cli

    return [cli.download_data]
