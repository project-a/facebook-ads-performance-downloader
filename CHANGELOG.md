# Changelog

## 1.5.0 (2018-05-28)

- Fix facebook-api 'unknown error' with asynchronous requests

## 1.4.0 - 1.4.3 (2017-12-28)

- Updated `facebookads` to 2.11.1`
- catch not existing impressions and spend
- small bug fix for logging with target accounts 
- Insert unknown device if device is empty
- allow more characters in labels
- Handle 'unknown error' by retrying on all FacebookRequestError exceptions

## 1.3.0 (2017-10-05)

- Add the `target_accounts` parameter to optionally limit the accounts to download
- Updated `facebookads` to 2.10.1`

## 1.2.0 (2017-09-21)

- Made the config and click commands discoverable in [mara-app](https://github.com/mara/mara-app) >= 1.2.0


## 1.1.0 - 1.1.2 (2017-05-17)

- Updated to version 2.9.1
- Drop deprecated Placement dimension 
- Updated to version 2.9.2
- Download performance data for ads in all states except deleted ones

**required changes**

- Remove work files and run import again after downloading latest files 



## 1.0.0 - 1.0.2 (2017-03-01) 

- Initial version
- made cli and config discoverable
- Changed logic for first date. Resolves problems for the case of absent creation time for an account.
