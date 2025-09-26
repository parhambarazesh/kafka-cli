#!/bin/bash
# shared access signature token
get_sas_token() {
    local EVENTHUB_URI='https://<event-hub-namespace>.servicebus.windows.net/<event-hub-name>'
    local SHARED_ACCESS_KEY_NAME='<shared-access-key-name>'
    local SHARED_ACCESS_KEY='<shared-access-key>'
    local EXPIRY=${EXPIRY:=$((60 * 60 * 24))} # Default token expiry is 1 day

    local ENCODED_URI=$(echo -n $EVENTHUB_URI | jq -s -R -r @uri)
    local TTL=$(($(date +%s) + $EXPIRY))
    local UTF8_SIGNATURE=$(printf "%s\n%s" $ENCODED_URI $TTL | iconv -t utf8)

    local HASH=$(echo -n "$UTF8_SIGNATURE" | openssl sha256 -hmac $SHARED_ACCESS_KEY -binary | base64)
    local ENCODED_HASH=$(echo -n $HASH | jq -s -R -r @uri)

    echo -n "SharedAccessSignature sr=$ENCODED_URI&sig=$ENCODED_HASH&se=$TTL&skn=$SHARED_ACCESS_KEY_NAME"
}

# Call the function
TOKEN=$(get_sas_token)

# Print the token
echo $TOKEN