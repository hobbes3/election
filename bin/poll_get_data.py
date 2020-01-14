#!/usr/bin/env python
# hobbes3

import logging
import json
import re
import splunk_rest.splunk_rest as sr
from splunk_rest.splunk_rest import splunk_rest, try_response
from datetime import datetime

@splunk_rest
def poll():
    logger.info("Getting data...")
    for url in urls:
        r = s.get(url)

        send_data(r)

@try_response
def send_data(r):
    # The response is warpped around "return_json( ... )".
    r_json = json.loads(re.sub(r"^return_json\((.+)\);$", r"\1", r.text))

    poll = r_json["poll"]
    poll_id = poll["id"]
    times = poll["rcp_avg"]

    data = ""
    for time in times:
        # Sun, 05 Jan 2020 00:00:00 -0600
        epoch = datetime.strptime(time["date"], "%a, %d %b %Y %H:%M:%S %z").timestamp()

        candidates = time["candidate"]

        for candidate in candidates:
            candidate.update({
                "splunk_rest": {
                    "session_id": sr.session_id,
                    "request_id": r.request_id,
                },
                "epoch": epoch,
                "poll": {
                    "id": poll_id,
                    "title": poll["title"],
                    "link": poll["link"],
                }
            })

            event = {
                "index": index,
                "sourcetype": "poll_"+poll_id,
                "source": __file__,
                "event": candidate,
            }

            data += json.dumps(event)

    s.post(hec_url, headers=hec_headers, data=data)

if __name__ == "__main__":
    script_args = sr.get_script_args()

    logger = logging.getLogger("splunk_rest.splunk_rest")
    # Status code 429 = "Too Many Requests"
    s = sr.retry_session()

    hec_url = sr.config["hec"]["url"]
    hec_headers = sr.config["hec"]["headers"]

    index = "main" if script_args.test else sr.config["realclearpolitics"]["index"]
    urls = sr.config["realclearpolitics"]["urls"]

    poll()
