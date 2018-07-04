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


def target_accounts() -> str:
    """The accounts to download, comma separated, if empty each available account will be tried"""
    return ''


def action_attribution_windows() -> [str]:
    """The action attribution windows parameter. Default as '28d_click'
    https://developers.facebook.com/docs/marketing-api/insights/#sample"""
    return ['28d_click']


def number_of_ad_performance_threads() -> str:
    """The number of threads used to download ad performance"""
    return '10'
