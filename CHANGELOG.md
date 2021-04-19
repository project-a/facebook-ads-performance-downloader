# Changelog

## 3.2.0 (2021-04-19)

- Switch to current Facebook API version (10.0.0)

## 3.1.0 (2020-04-06)

- Switch to current Facebook API version (6.0.0) 


## 3.0.1 (2019-08-20)
- Switch to current Facebook API version (4.0.2) 
- Replaced deprecated function remote_read with api_get

## 3.0.0 (2019-04-13)

- Change MARA_XXX variables to functions to delay importing of imports

**required changes** 

- If used together with a mara project, Update `mara-app` to `>=2.0.0`


## 2.1.0 (2019-02-06)
- Switch to current Facebook API version (3.2.4)
  - Fix python3.7 compatibility (usage of `async` as variable name)
- Block (sleep) worker threads when request limit reached
- Fix potential deadlocks in the early exit case
  - Logging for failures now also prints the full stack trace.

## 2.0.0 - 2.0.1 (2018-09-27)

- Switch to current Facebook API version (3.0.0)
- Parallelize ad performance download
- Unify output file names, bump version to 'v2'
- Fix API bug "2601" on handling async requests

**required changes**

- Adapt ETL to new output file names


## 1.5.0 - 1.5.3 (2018-07-24)

- Fix facebook-api request and rate limit issues with asynchronous requests
- Add check for 'Job Failed' status
- Describe how to refresh the API token
- Make action_attribution_windows parameter configurable

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
