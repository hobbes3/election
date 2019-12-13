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

    schedule_a_url = "https://api.open.fec.gov/v1/schedules/schedule_a/"
    schedule_e_url = "https://api.open.fec.gov/v1/schedules/schedule_e/"

    schedule_a_params = fec_params.copy()
    schedule_a_params.update({
        "is_individual": True,
        "two_year_transaction_period": 2020,
    })

    schedule_e_params = fec_params.copy()
    schedule_e_params.update({
        "cycle": 2020,
        "is_notice": False,
        "data_type": "processed"
    })

    def get_candidate(candidate):
        logger.debug("Getting candidate...", extra={"candidate_id": candidate})

        last_indexes = True

        params = schedule_e_params.copy()
        params["candidate_id"] = candidate

        @try_response
        def send_candidate(r, *args, **kwargs):
            r_json = r.json()
            results = r_json["results"]
            pagination = r_json["pagination"]
            last_indexes = pagination["last_indexes"]

            m = meta.copy()
            m["pagination"] = pagination
            logger.debug("Pagination info received.", extra=m)

            data = ""
            for result in results:
                result["splunk_rest"] = {
                    "session_id": sr.session_id,
                    "request_id": r.request_id,
                }

                event = {
                    "index": index,
                    "sourcetype": "fec_schedule_e",
                    "source": __file__,
                    "event": result,
                }

                data += json.dumps(event)

            s.post(hec_url, headers=hec_headers, data=data)

            return last_indexes

        while last_indexes:
            r = s.get(schedule_e_url, params=params)
            meta = {
                "request_id": r.request_id,
            }
            last_indexes = send_candidate(r)

            if last_indexes:
                params["last_index"] = last_indexes["last_index"]
                params["last_expenditure_date"] = last_indexes["last_expenditure_date"]

    #sr.multiprocess(get_committee, schedule_a_committees)
    sr.multiprocess(get_candidate, schedule_e_candidates)

if __name__ == "__main__":
    script_args = sr.get_script_args()

    logger = logging.getLogger("splunk_rest.splunk_rest")
    s = sr.retry_session()

    hec_url = sr.config["hec"]["url"]
    hec_headers = sr.config["hec"]["headers"]

    index = "main" if script_args.test else sr.config["fec"]["index"]
    api_key = sr.config["fec"]["api_key"]
    schedule_a_committees = sr.config["fec"]["schedule_a_committees"]
    schedule_e_candidates = sr.config["fec"]["schedule_e_candidates"]

    fec()
