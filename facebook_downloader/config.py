"""
Configures access to the Facebook Ads API and where to store results
"""


def data_dir() -> str:
    """The directory where result data is written to"""
    return '/tmp/facebook_ads'


def first_date() -> str:
    """The first day for which data is downloaded"""
    return '2015-01-01'


def app_id() -> str:
    """The app id obtained from the app's settings in facebook for developers

    https://developers.facebook.com/apps/<APP_ID>/settings/
    """
    return '1234567890'


def app_secret() -> str:
    """The app secret obtained from the app's settings in facebook for developers

    https://developers.facebook.com/apps/<APP_ID>/settings/
    """
    return 'aBcDeFg'


def access_token() -> str:
    """The access token of the system user with the following credentials:
    - read_insights
    - ads_read

    https://business.facebook.com/settings/system-users/<SYSTEM_USER_ID>?business_id=<BUSINESS_ID>
    """
    return 'foo'


def redownload_window() -> str:
    """The number of days for which the performance data will be redownloaded"""
    return '28'


def account_id() -> str:
    """Facebook account id for which we are requesting the data"""
    return 'me'
