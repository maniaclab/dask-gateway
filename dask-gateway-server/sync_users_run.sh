#!/bin/bash -x
set -eo pipefail
# This script does not do significant (any) error checking.

###############################################################################
# Grab the connect provisioner 
CONNECT_DIR='/usr/local/ciconnect'

# do a one-time run at startup
pushd $CONNECT_DIR
echo "token=$API_TOKEN" > $CONNECT_DIR/token
export API_TOKEN_FILE=$CONNECT_DIR/token
# Random sleep to prevent thundering herd.
#sleep $((RANDOM%300))
#source /usr/local/ciconnect/config && ./sync_users.sh -u root.atlas-af -g root.atlas-af -e https://api.ci-connect.net:18080 
$CONNECT_DIR/sync_users.sh -u root.atlas-af -g root.atlas-af -e https://api.ci-connect.net:18080
popd

