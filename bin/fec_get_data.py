#!/usr/bin/env python
# hobbes3

import logging
import json
import re
import splunk_rest.splunk_rest as sr
from splunk_rest.splunk_rest import splunk_rest, try_response

@splunk_rest
def fec():
    fec_params = {
        "api_key": api_key,
        "per_page": 100,
    }

    schedule_a = [{
        "committee_id": c,
        "sourcetype": "fec_schedule_a",
    } for c in committees]
    schedule_e = [{
        "candidate_id": c,
        "sourcetype": "fec_schedule_e",
    } for c in candidates]
    fec_args = schedule_a + schedule_e

    def get_data(fec_arg):
        logger.debug("Getting data...", extra=fec_arg)

        sourcetype = fec_arg["sourcetype"]

        params = fec_params.copy()

        if sourcetype == "fec_schedule_a":
            url = "https://api.open.fec.gov/v1/schedules/schedule_a/"
            params.update({
                "committee_id": fec_arg["committee_id"],
                "is_individual": True,
                "two_year_transaction_period": 2020,
            })
        else:
            url = "https://api.open.fec.gov/v1/schedules/schedule_e/"
            params.update({
                "candidate_id": fec_arg["candidate_id"],
                "cycle": 2020,
                "is_notice": False,
                "data_type": "processed"
            })

        done = False
        page = 1
        last_indexes = True

        @try_response
        def send_data(r, sourcetype):
            r_json = r.json()
            results = r_json["results"]
            pagination = r_json["pagination"]

            meta = {
                "request_id": r.request_id,
                "pagination": pagination,
            }
            logger.debug("Pagination info received.", extra=meta)

            data = ""
            for result in results:
                result["splunk_rest"] = {
                    "session_id": sr.session_id,
                    "request_id": r.request_id,
                }

                event = {
                    "index": index,
                    "sourcetype": sourcetype,
                    "source": __file__,
                    "event": result,
                }

                data += json.dumps(event)

            if data:
                s.post(hec_url, headers=hec_headers, data=data)

            return pagination

        while not done:
            r = s.get(url, params=params)
            meta = {
                "request_id": r.request_id,
            }
            pagination = send_data(r, sourcetype)

            last_indexes = pagination["last_indexes"]
            page += 1
            if script_args.sample and page > 3:
                logger.debug("Sample mode on so limiting to 3 pages.", extra=meta)
                done = True
            elif not last_indexes:
                done = True
            else:
                for k, v in last_indexes.items():
                    params[k] = v

    sr.multiprocess(get_data, fec_args)

if __name__ == "__main__":
    script_args = sr.get_script_args()

    logger = logging.getLogger("splunk_rest.splunk_rest")
    s = sr.retry_session()

    hec_url = sr.config["hec"]["url"]
    hec_headers = sr.config["hec"]["headers"]

    index = "main" if script_args.test else sr.config["fec"]["index"]
    api_key = sr.config["fec"]["api_key"]
    committees = sr.config["fec"]["committees"]
    candidates = sr.config["fec"]["candidates"]

    fec()
