import csv
import datetime
import errno
import gzip
import json
import logging
import re
import shutil
import sqlite3
import tempfile
import time
from functools import wraps
from pathlib import Path
from typing import Generator, List, Union

from facebook_downloader import config
from facebookads.adobjects import user, adaccount, adsinsights
from facebookads.api import FacebookAdsApi, FacebookRequestError

OUTPUT_FILE_VERSION = 'v1'


def download_data():
    """Initializes the FacebookAdsAPI, retrieves the ad accounts and downloads the data"""
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    FacebookAdsApi.init(config.app_id(),
                        config.app_secret(),
                        config.access_token())
    ad_accounts = _get_ad_accounts()
    download_data_sets(ad_accounts)


def download_data_sets(ad_accounts: [adaccount.AdAccount]):
    """Downloads the account structure and ad performance data sets for all ad accounts

    Args:
        ad_accounts: A list of all ad accounts to download.

    """
    download_account_structure(ad_accounts)
    download_ad_performance(ad_accounts)


def download_account_structure(ad_accounts: [adaccount.AdAccount]):
    """Downloads the Facebook Ads account structure to a csv.

    Args:
        ad_accounts: A list of all ad accounts to download.

    """
    filename = Path('facebook-account-structure_{}.csv.gz'.format(OUTPUT_FILE_VERSION))
    filepath = ensure_data_directory(filename)

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_filepath = Path(tmp_dir, filename)
        with gzip.open(str(tmp_filepath), 'wt') as tmp_account_structure_file:
            header = ['Ad Id',
                      'Ad',
                      'Ad Set Id',
                      'Ad Set',
                      'Campaign Id',
                      'Campaign',
                      'Account Id',
                      'Account',
                      'Attributes']
            writer = csv.writer(tmp_account_structure_file, delimiter="\t")
            writer.writerow(header)

            for ad_account in ad_accounts:
                for row in download_account_structure_per_account(ad_account):
                    writer.writerow(row)

        shutil.move(str(tmp_filepath), str(filepath))


def download_ad_performance(ad_accounts: [adaccount.AdAccount]):
    """Download the Facebook ad performance and upserts them
    into a sqlite database per account and day

    Args:
        ad_accounts: A list of all ad accounts to download.

    """
    for ad_account in ad_accounts:
        # calculate yesterday based on the timezone of the ad account
        ad_account_timezone = datetime.timezone(datetime.timedelta(
            hours=float(ad_account['timezone_offset_hours_utc'])))
        last_date = datetime.datetime.now(ad_account_timezone).date() - datetime.timedelta(days=1)
        first_date = _first_download_date_of_ad_account(ad_account)

        # check for ad performance db on the first day the account
        current_date = last_date
        while current_date >= first_date:
            db_name = ensure_data_directory(
                Path("{date:%Y/%m/%d}/facebook/ad-performance-act_{account_id}.sqlite3"
                     .format(date=current_date,
                             account_id=ad_account['account_id'])))

            if (not db_name.is_file()
                or (last_date - current_date).days <= int(config.redownload_window())):
                ad_insights = get_account_ad_performance_for_single_day(ad_account,
                                                                        current_date)
                with sqlite3.connect(str(db_name)) as con:
                    _upsert_ad_performance(ad_insights, con)
            current_date -= datetime.timedelta(days=1)


def download_account_structure_per_account(ad_account: adaccount.AdAccount) \
        -> Generator[List, None, None]:
    """Downloads the Facebook Ads account structure for a specific account
    and transforms them to flat rows per ad

    Args:
        ad_account: An ad account to download.

    Returns:
        An iterator of campaign structure rows

    """
    campaign_data = get_campaign_data(ad_account)
    ad_set_data = get_ad_set_data(ad_account)
    ad_data = get_ad_data(ad_account)

    for ad_id, ad in ad_data.items():
        ad_set_id = ad['ad_set_id']
        ad_set = ad_set_data[ad_set_id]
        campaign_id = ad_set['campaign_id']
        campaign = campaign_data[campaign_id]

        attributes = {**campaign['attributes'],
                      **ad_set['attributes'],
                      **ad['attributes']}

        row = [ad_id,
               ad['name'],
               ad_set_id,
               ad_set['name'],
               campaign_id,
               campaign['name'],
               ad_account['account_id'],
               ad_account['name'],
               json.dumps(attributes)]

        yield row


def rate_limiting(func):
    """Wraps the function and applies an exponentially increasing sleep time
    if a rate limiting error occurs

    Args:
        func: A function that should be rate limited

    Returns:
        The result of the wrapped function

    """

    @wraps(func)
    def func_wrapper(*args, **kwargs):
        # Retry counter for rate limiting
        number_of_attempts = 0
        while True:
            try:
                return func(*args, **kwargs)
            except FacebookRequestError as e:
                # Deal with rate limiting error
                # https://developers.facebook.com/docs/marketing-api/api-rate-limiting
                if e.api_error_code() == 17 and number_of_attempts < 7:
                    duration = 60 * 2 ** number_of_attempts
                    logging.warning('Hit rate limiting. Retry #{attempt} in {duration} seconds'
                                    .format(attempt=number_of_attempts,
                                            duration=duration))
                    time.sleep(duration)
                    number_of_attempts += 1
                else:
                    raise

    return func_wrapper


@rate_limiting
def get_ad_data(ad_account: adaccount.AdAccount) -> {}:
    """Retrieves the ad data of the ad account as a dictionary

    Args:
        ad_account: An ad account for which to retrieve the ad data

    Returns:
        A dictionary with {ad_id: {'name': 1, 'ad_set_id': 2, 'attributes': {}}} format

    """
    logging.info('get ad data for account {}'.format(ad_account['account_id']))
    ads = ad_account.get_ads(
        fields=['id',
                'name',
                'adset_id',
                'adlabels'],
        params={'limit': 1000,
                'status': ['ACTIVE',
                           'PAUSED',
                           'ARCHIVED']})
    result = {}

    for ad in ads:
        result[ad['id']] = {'name': ad['name'],
                            'ad_set_id': ad['adset_id'],
                            'attributes': parse_labels(ad.get('adlabels', []))}
    return result


@rate_limiting
def get_ad_set_data(ad_account: adaccount.AdAccount) -> {}:
    """Retrieves the ad set data of the ad account as a dictionary

    Args:
        ad_account: An ad account for which to retrieve the ad set data

    Returns:
        A dictionary with {ad_set_id: {'name': 1,
                                       'campaign_id': 2,
                                       'attributes': {}}} format

    """
    logging.info('get ad set data for account {}'.format(ad_account['account_id']))
    ad_sets = ad_account.get_ad_sets(
        fields=['id',
                'name',
                'campaign_id',
                'adlabels'],
        params={'limit': 1000,
                'status': ['ACTIVE',
                           'PAUSED',
                           'ARCHIVED']})
    result = {}

    for ad_set in ad_sets:
        result[ad_set['id']] = {'name': ad_set['name'],
                                'campaign_id': ad_set['campaign_id'],
                                'attributes': parse_labels(
                                    ad_set.get('adlabels', []))}
    return result


@rate_limiting
def get_campaign_data(ad_account: adaccount.AdAccount) -> {}:
    """Retrieves the campaign data of the ad account as a dictionary

    Args:
        ad_account: An ad account for which to retrieve the campaign data

    Returns:
        A dictionary with {campaign_id: {'name': 1, 'attributes': {}}} format

    """
    logging.info('get campaign data for account {}'.format(ad_account['account_id']))
    campaigns = ad_account.get_campaigns(
        fields=['id',
                'name',
                'adlabels'],
        params={'limit': 1000,
                'status': ['ACTIVE',
                           'PAUSED',
                           'ARCHIVED']})
    result = {}

    for campaign in campaigns:
        result[campaign['id']] = {'name': campaign['name'],
                                  'attributes': parse_labels(
                                      campaign.get('adlabels', []))}
    return result


@rate_limiting
def get_account_ad_performance_for_single_day(ad_account: adaccount.AdAccount,
                                              single_date: datetime) -> adsinsights.AdsInsights:
    """Downloads the ad performance for an ad account for a given day
    https://developers.facebook.com/docs/marketing-api/insights

    Args:
        ad_account: An ad account to download.
        single_date: A single date as a datetime object

    Returns:
        A list containing dictionaries with the ad performance from the report

    """
    logging.info('download Facebook ad performance of act_{ad_account_id} on {single_date}'.format(
        ad_account_id=ad_account['account_id'],
        single_date=single_date.strftime('%Y-%m-%d')))

    ad_insights = ad_account.get_insights(
        # https://developers.facebook.com/docs/marketing-api/insights/fields
        fields=['date_start',
                'ad_id',
                'impressions',
                'actions',
                'spend',
                'action_values'],
        # https://developers.facebook.com/docs/marketing-api/insights/parameters
        params={'action_attribution_windows': ['28d_click'],
                # https://developers.facebook.com/docs/marketing-api/insights/action-breakdowns
                'action_breakdowns': ['action_type'],
                # https://developers.facebook.com/docs/marketing-api/insights/breakdowns
                'breakdowns': ['impression_device'],
                'level': 'ad',
                'limit': 1000,
                'time_range': {'since': single_date.strftime('%Y-%m-%d'),
                               'until': single_date.strftime('%Y-%m-%d')},
                # By default only ACTIVE campaigns get considered.
                'filtering': [{
                    'field': 'ad.effective_status',
                    'operator': 'IN',
                    'value': ['ACTIVE',
                              'PAUSED',
                              'PENDING_REVIEW',
                              'DISAPPROVED',
                              'PREAPPROVED',
                              'PENDING_BILLING_INFO',
                              'CAMPAIGN_PAUSED',
                              'ARCHIVED',
                              'ADSET_PAUSED']}]})

    return ad_insights


def ensure_data_directory(relative_path: Path = None) -> Path:
    """Checks if a directory in the data dir path exists. Creates it if necessary

    Args:
        relative_path: A Path object pointing to a file relative to the data directory

    Returns:
        The absolute path Path object

    """
    if relative_path is None:
        return Path(config.data_dir())
    try:
        path = Path(config.data_dir(), relative_path)
        # if path points to a file, create parent directory instead
        if path.suffix:
            if not path.parent.exists():
                path.parent.mkdir(exist_ok=True, parents=True)
        else:
            if not path.exists():
                path.mkdir(exist_ok=True, parents=True)
        return path
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


def parse_labels(labels: [{}]) -> {str: str}:
    """Extracts labels from a string.

    Args:
        labels: Labels in the form of
                [{"id": "1", "name": "{key_1=value_1}"},
                 {"id": "2", "name": "{key_2=value_2}"}]"'

    Returns:
            A dictionary of labels with {key_1 : value_1, ...} format

    """
    labels_dict = {}
    for label in labels:
        match = re.search("{([a-zA-Z|_]+)=([a-zA-Z|_]+)}", label['name'])
        if match:
            key = match.group(1).strip().lower().title()
            value = match.group(2).strip()
            labels_dict[key] = value
    return labels_dict


@rate_limiting
def _get_ad_accounts() -> [adaccount.AdAccount]:
    """Retrieves the ad accounts of the user whose access token was provided and
    returns them as a list.

    Returns:
        A list of ad accounts

    """
    system_user = user.User(fbid=config.account_id)
    ad_accounts = system_user.get_ad_accounts(fields=['account_id',
                                                      'name',
                                                      'created_time',
                                                      'timezone_offset_hours_utc'])
    return list(ad_accounts)


def _upsert_ad_performance(ad_insights: [adsinsights.AdsInsights], con: sqlite3.Connection):
    """Creates the ad performance table if it does not exists and upserts the
    ad insights data afterwards

    Args:
        ad_insights: A list of Insights objects
        con: A sqlite database connection

    """
    con.execute("""
CREATE TABLE IF NOT EXISTS ad_performance (
  date          DATE   NOT NULL,
  ad_id         BIGINT NOT NULL,
  device        TEXT   NOT NULL,
  performance   TEXT   NOT NULL,
  PRIMARY KEY (ad_id, device)
);""")
    con.executemany("INSERT OR REPLACE INTO ad_performance VALUES (?,?,?,?)",
                    _to_insight_row_tuples(ad_insights))


def _to_insight_row_tuples(ad_insights: [adsinsights.AdsInsights]) -> Generator[tuple, None, None]:
    """Transforms the Insights objects into tuples that can be directly inserted
    into the ad_performance table

    Args:
        ad_insights: A list of Insights objects for an ad on a specific day

    Returns:
        A list of tuples of ad performance data

    """
    for ad_insight in ad_insights:
        actions = ad_insight.get('actions') or []
        actions = [_floatify_values(action) for action in actions]

        action_values = ad_insight.get('action_values') or []
        action_values = [_floatify_values(action_value) for action_value in action_values]

        performance = {'impressions': int(ad_insight['impressions']),
                       'spend': float(ad_insight['spend']),
                       'actions': actions,
                       'action_values': action_values}

        ad_insight_tuple = (ad_insight['date_start'],
                            ad_insight['ad_id'],
                            ad_insight['impression_device'],
                            json.dumps(performance))

        yield ad_insight_tuple


def _floatify(value: str) -> Union[str, float]:
    try:
        return float(value)
    except ValueError:
        return value


def _floatify_values(inp: {}) -> {}:
    return {key: _floatify(value) for key, value in inp.items()}


def _first_download_date_of_ad_account(ad_account: adaccount.AdAccount) -> datetime.date:
    """Finds the first date for which the ad account's performance should be
    downloaded by comparing the first download date from the configuration and
    the creation date of the account and returning the maximum of the two.

    Args:
        ad_account: An ad account to download

    Returns:
        The first date to download the performance data for

    """
    config_first_date = datetime.datetime.strptime(config.first_date(),
                                                   '%Y-%m-%d').date()
    if 'created_time' in ad_account:
        account_created_date = datetime.datetime.strptime(ad_account['created_time'],
                                                          "%Y-%m-%dT%H:%M:%S%z").date()
        return max(config_first_date, account_created_date)
    else:
        return config_first_date
