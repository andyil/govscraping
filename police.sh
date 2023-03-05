#!/bin/bash
set -euo pipefail

compose_url() {
  SKIP="$1";
  curl -s -XPOST 'https://www.gov.il/api/police/menifa/api/menifa/getDocList' -H 'Content-Type: application/json;charset=UTF-8' --data '{"skip":"'$SKIP'"}'
}

collect_jsons() {
  local SKIP=0
  while true; do
    local RES="$(compose_url "$SKIP")"
    COUNT="$(echo "$RES" | jq '.Results | length')"
    if [[ "$COUNT" == "0" ]]; then
      return
    fi
    SKIP="$(( $SKIP + $COUNT ))"
    echo "$RES" | jq -c '.Results[]'
  done
}

collect_jsons | jq '
{
  "filename": "\(.Data.Name | gsub("/";"-") | .[0:50])-\(.Data.MisparPkuda).\(.Data.fileData[0].Extension)",
  "url": .Data.fileData[0].FileName,
}
' | jq -r "\"wget \\(.url) --quiet -nc -O '\\(.filename)'\"" | parallel --bar
