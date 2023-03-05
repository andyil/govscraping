#!/bin/bash
set -euo pipefail

compose_url() {
  SKIP="$1";
  curl -s 'https://www.gov.il/he/api/DynamicCollector' -X POST -H 'Content-Type: application/json;charset=utf-8' --data '{"DynamicTemplateID":"5f3d4e58-cb49-4ab3-9248-dc85d51c072d","QueryFilters":{"skip":{"Query":'"$SKIP"'}},"From":'$SKIP'}'
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

collect_jsons | jq -c '
 {
   "filename": (.Data.file[0].FileName),
   "url": "https://www.gov.il/BlobFolder/dynamiccollectorresultitem/\(.UrlName)/he/\(.Data.file[0].FileName)"
 }
 ' | jq -r "\"wget \\(.url) --quiet -nc -O '\\(.filename)'\"" | parallel --bar
