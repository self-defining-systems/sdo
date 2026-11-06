#!/usr/bin/env bash
# Source this from a test-running script. Hosted runners have no git identity; a developer machine does, so a
# fixture that commits without setting one passes locally and fails with "Author identity unknown" on the runner.
# An empty global config, no system config and no identity variables make the local run fail too.
# Tests that commit must set user.name and user.email themselves. Test scripts only; production code never sources this.
export GIT_CONFIG_GLOBAL=/dev/null
export GIT_CONFIG_NOSYSTEM=1
unset GIT_AUTHOR_NAME GIT_AUTHOR_EMAIL GIT_COMMITTER_NAME GIT_COMMITTER_EMAIL EMAIL
