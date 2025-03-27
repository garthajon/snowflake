-- creating a integration with git so that i can integrate a git repo with snowflake 

-- this integration is being created to run the kaggle diamond dataset machine learning example (dataset here in kaggle):

-- https://www.kaggle.com/datasets/shivam2503/diamonds?resource=download

-- note i used this article as a guide to set up git integration with snowflake

-- https://docs.snowflake.com/en/developer-guide/git/git-setting-up#label-integrating-git-repository-api-integration

-- firstly create the secret for the password login
CREATE OR REPLACE SECRET my_git_secret
  TYPE = password
  USERNAME = 'garthajon'
  PASSWORD = 'fluffpassword';

  -- create integration to integrate/link with git

CREATE OR REPLACE API INTEGRATION git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/garthajon')
  ALLOWED_AUTHENTICATION_SECRETS = (my_git_secret)
  ENABLED = TRUE;

  -- then generate a personal access token using the following instructions:


  -- https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens

  -- e.g. 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   (valid for 30 days in this case for this use)
