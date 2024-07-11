# fnt-etl

## Description
This project houses the various ETL scripts required for AWS Lambda and AWS Glue. TBD on upload procedures - will use AWS CLI and Terraform to deploy serverless applications.

## Project Structure

This repo contains the following critical folders and files:

    (1) notebooks - iPython Notebooks containing ad hoc analyses or other exploratory work

    (2) lambda - primary lambda python scripts (handlers) and helper files for deploying to AWS Lambda

    (3) glue - primary glue scripts (and/or Notebooks) for deploying to AWS Glue

    (4) requirements.txt - list of the python libraries and packages required

Upon downloading this repo, the first step is to ensure you have python installed on your computer. Next - for running locally - a python virtual environment (venv) should be set up directly in the project folder (see "Virtual Environment Setup").

## Virtual Environment Setup

General instructions for installing a virtual environment can be found here: https://docs.python.org/3/library/venv.html. **NOTE: Python 3.9 must be used for Lambda functions.** To install the required packages for this project, first activate the venv using a CLI (directions in the link) and navigate to the project folder and run the following command:

```sh
python -m pip install -r requirements.txt
```

## Contributing

### Branching Strategy

The branching strategy we are using is **Feature Branching**

To start work on a feature/hotfix/bug first pull down `main` to get the lastest updated version of the code.

Create a branch from `main`, the branch name should follow the format of [JIRA-Ticket/Issue-Number]-FeatureDescription. E.g CD-123-Dashboard-Sidebar

Once you have created your branch, complete your work and commit in increments with descriptive commit messages.

If there has been new commits to `main` make sure to checkout `main` and `git pull` to pull down the latest code from `main`, then checkout to your branch and merge `main` into your branch.

Then create a pull request.

When merging your PR, make sure you to select the "squash" (default) merge strategy to keep the commits for your feature branch unified into one commit on `main`.

### PR Checklists

This checklist is for code contributors to double-check their pull requests before merging to `main`.

General

- [ ] Ticket number is specified in the title - [JIRA-Ticket/Issue-Number]-FeatureDescription. E.g CD-123-Dashboard-Sidebar
- [ ] `main` has been pulled and is merged into your feature branch
- [ ] Feature flags (if applicable)
- [ ] Logging
- [ ] Steps / Helpful info for QA written on ticket in JIRA
- [ ] Ticket moved to In PR column in JIRA
- [ ] The link to this PR has been added to your JIRA ticket as a web link
- [ ] Summary of the work added as description to Pull Request
- [ ] Tag specific people needed to review PR in GitLab
- [ ] Add screenshot/video to PR in GitLab (if applicable)

Tests

- [ ] All resources are covered by tests

Documentation

- [ ] Comments in the code
- [ ] Documentation on Confluence
- [ ] Tool documentation / README

Related pull requests

- [ ] Paste the links to the other related PRs in the pull request.
