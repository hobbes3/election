#!/usr/bin/env python
# hobbes3

import logging
import json
import re
import splunk_rest.splunk_rest as sr
from splunk_rest.splunk_rest import splunk_rest, try_response

@splunk_rest
def fec():
    logger.info("Getting info about candidates and committees...")
    sr.multiprocess(get_info, candidates)

    logger.info("Combining candidates and committees for schedule e and a, respectively...")
    schedule_e = [{
        "candidate_id": c,
        "sourcetype": "fec_schedule_e",
    } for c in candidates]
    fec_args = schedule_a_zip + schedule_e

    sr.multiprocess(get_data, fec_args)

def get_info(candidate_id):
    url = "https://api.open.fec.gov/v1/candidate/{}/".format(candidate_id)
    r = s.get(url, params=fec_params)
    candidate_info.append(get_first_result(r))

    url = "https://api.open.fec.gov/v1/candidate/{}/committees/".format(candidate_id)
    params = fec_params.copy()
    params.update({
        "committee_type": "P",
    })

    r = s.get(url, params=params)
    schedule_a_zip.extend(get_committee_args(r, candidate_id))

@try_response
def get_first_result(r):
    return r.json()["results"][0]

@try_response
def get_committee_args(r, candidate_id):
    meta = {
        "request_id": r.request_id,
        "candidate_id": candidate_id,
    }

    results = r.json()["results"]

    if results:
        committee_args = []
        for result in results:
            committee_id = result["committee_id"]
            committee_args.extend([{
                "committee_id": committee_id,
                "candidate_id": candidate_id,
                "sourcetype": "fec_schedule_a_zip",
            }, {
                "committee_id": committee_id,
                "candidate_id": candidate_id,
                "sourcetype": "fec_schedule_a_size",
            }])

            url = "https://api.open.fec.gov/v1/committee/{}/".format(committee_id)
            r = s.get(url, params=fec_params)
            committee_info.append(get_first_result(r))

        m = meta.copy()
        m["committees"] = committee_args
        m["committee_count"] = len(committee_args)
        logger.debug("Found committees.", extra=m)
        return committee_args
    else:
        logger.warning("Found no committees for candidate!", extra=meta)

    return []

def get_data(fec_arg):
    logger.debug("Getting data...", extra=fec_arg)

    sourcetype = fec_arg["sourcetype"]
    candidate_id = fec_arg["candidate_id"]
    committee_id = fec_arg.get("committee_id", None)

    params = fec_params.copy()

    if sourcetype == "fec_schedule_a_zip":
        url = "https://api.open.fec.gov/v1/schedules/schedule_a/by_zip/"
        params.update({
            "committee_id": committee_id,
        })
    elif sourcetype == "fec_schedule_a_size":
        url = "https://api.open.fec.gov/v1/schedules/schedule_a/by_size/"
        params.update({
            "committee_id": committee_id,
        })
    else:
        url = "https://api.open.fec.gov/v1/schedules/schedule_e/"
        params.update({
            "candidate_id": candidate_id,
            "is_notice": False,
            "data_type": "processed"
        })

    done = False
    # Internal page count for this script
    # since API calls with last_indexes doesn't return a page key.
    page_ = 0

    while not done:
        page_ += 1
        r = s.get(url, params=params)
        meta = {
            "request_id": r.request_id,
            "candidate_id": candidate_id,
            "committee_id": committee_id,
            "sourcetype": sourcetype,
        }
        done, params = send_data(r, fec_arg, params, meta)

        if script_args.sample and page_ > 3:
            logger.debug("Sample mode on so limiting to 3 pages.", extra=meta)
            done = True

@try_response
def send_data(r, fec_arg, params, meta):
    sourcetype = fec_arg["sourcetype"]
    candidate_id = fec_arg["candidate_id"]
    committee_id = fec_arg.get("committee_id", None)

    r_json = r.json()
    results = r_json["results"]
    pagination = r_json["pagination"]

    meta["pagination"] = pagination

    logger.debug("Pagination info received.", extra=meta)

    data = ""
    for result in results:
        result["splunk_rest"] = {
            "session_id": sr.session_id,
            "request_id": r.request_id,
            "candidate_id": candidate_id,
            "candidate": next((c for c in candidate_info if c["candidate_id"] == candidate_id), {}),
        }

        if sourcetype.startswith("fec_schedule_a"):
            result["splunk_rest"]["committee_id"] = committee_id
            result["splunk_rest"]["committee"] = next((c for c in committee_info if c["committee_id"] == committee_id), {})

        index = index_slice if sourcetype.startswith("fec_schedule_a") else index_full

        event = {
            "index": index,
            "sourcetype": sourcetype,
            "source": __file__,
            "event": result,
        }

        data += json.dumps(event)

    if data:
        s.post(hec_url, headers=hec_headers, data=data)

    done = False

    if sourcetype.startswith("fec_schedule_a"):
        page = pagination["page"]
        pages = pagination["pages"]
        if page >= pages:
            done = True
        else:
            params.update({
                "page": page+1,
            })
    else:
        last_indexes = pagination.get("last_indexes", None)
        if last_indexes:
            params.update(last_indexes)
        else:
            done = True

    return done, params

if __name__ == "__main__":
    script_args = sr.get_script_args()

    logger = logging.getLogger("splunk_rest.splunk_rest")
    # Status code 429 = "Too Many Requests"
    #s = sr.retry_session(total=20, backoff_factor=5, status_forcelist=[429, 500, 502, 503, 504])
    s = sr.retry_session(status_forcelist=[429, 500, 502, 503, 504])

    hec_url = sr.config["hec"]["url"]
    hec_headers = sr.config["hec"]["headers"]

    index_slice = "main" if script_args.test else sr.config["fec"]["index_slice"]
    index_full = "main" if script_args.test else sr.config["fec"]["index_full"]
    api_key = sr.config["fec"]["api_key"]
    candidates = sr.config["fec"]["candidates"]

    schedule_a_zip = []
    fec_params = {
        "api_key": api_key,
        "per_page": 100,
        "cycle": 2020,
    }

    candidate_info = []
    committee_info = []

    fec()
