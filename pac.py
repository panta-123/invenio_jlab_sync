import requests
import glob
import json
import re
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Define log file name with timestamp and rotation
log_file = datetime.now().strftime("pacdb_sync_logs_%Y-%m-%d.log")
handler = RotatingFileHandler(log_file, maxBytes=100000, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

INVENIOHOST = "https://inveniordm.jlab.org"
TOKEN = ""
COMMUNITYID = "33017028-77cc-4709-bf42-aa0767cbe74e"

h = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
        }

division_title_id  = {"A": "ENPH-EH-HA",
                      "B": "ENPH-EH-HB",
                      "C": "ENPH-EH-HC",
                      "D": "ENPH-EH-HD"}

status_dict = {
    'A- Approved': 'A',
    'AT- Approved Test': 'AT',
    'C1- Conditionally Approve w/Technical Review': 'C1',
    'C2- Conditionally Approve 2/PAC Review': 'C2',
    'C3- Conditionally Approve, based on availability': 'C3',
    'C- Conditionally Approve': 'C',
    'D- Deffered': 'D',
    'O- Dropped': 'O',
    'S- MPS Not yet funded': 'S',
    'N- New': 'N',
    'P- Pass': 'P',
    'R- Rejected': 'R',
    'Q- Replaced': 'Q',
    'H- Run Group Proposals': 'H',
    'G- Run Group Additions': 'G',
    'U- Unknown': 'U',
    'W- Withdrawn': 'W'
}

def cleanedName(fullname):
    names = fullname.split()
    # The first name includes the middle name if present
    first_name = ' '.join(names[:-1])
    # Last name is the last element in the names list
    last_name = names[-1]
    return {"type": "personal", "given_name":first_name, "family_name":last_name}

def getAuthorDict(authors, spokesperson=False, contactPerson=False):
    author_list = []
    roleID = "researcher"
    metaKey = "creators"
    for author in authors:
        authorNameDict = {"type": "personal", "given_name":author["first_name"], "family_name":author["last_name"]}
        institution = author.get('institution',"")
        if spokesperson:
            roleID = "projectleader"
            metaKey = "contributors"
        if contactPerson:
            roleID = "contactperson"
            metaKey = "contributors"
        authdict = {"person_or_org":authorNameDict, "role": {"id": roleID}}
        if institution:
            if institution.lower() == "jefferson lab":
                        institution = "Thomas Jefferson National Accelerator Facility"
            elif institution.lower() == "jlab":
                institution = "Thomas Jefferson National Accelerator Facility"
            else:
                 institution = institution
            authdict["affiliations"] = [{"name":institution}]
        author_list.append(authdict)
    returnDict = {metaKey : author_list}
    return returnDict

def statusID(status):
    statusID = None
    try:
        pattern = r'^.*[:\-]\s*|\s*\(.+'
        # Remove the matched pattern from the string
        extracted_status = re.sub(pattern, '', status, flags=re.MULTILINE).strip()
        if extracted_status.lower() == "deferred":
            statusID = "D"
        elif extracted_status.lower() == "conditionally approved":
            statusID = "C"
        else:
            for key, value in status_dict.items():
                key_status =  re.sub(pattern, '', key, flags=re.MULTILINE).strip()
                if key_status == extracted_status:
                    statusID = status_dict[key]
                    break
    except Exception as e:
        statusID = status_dict["U- Unknown"]
    
    if not statusID:
        statusID = status_dict["U- Unknown"]

    return statusID

def getLinksDict(links):
    html_record_url = links["proposal_html_url"]
    isderivedfromdict =  { "identifier": html_record_url,
                            "scheme": "url",
                            "relation_type": {
                                "id": "isderivedfrom",
                                "title": {
                                    "de": "Wird abgeleitet von",
                                    "en": "Is derived from"
                                    }
                            }
                        }
    returnDict = {"related_identifiers": [isderivedfromdict]}
    return returnDict

def getAttachmentDict(attachments):
    isdocumentedbyList = []
    for attachment in attachments:
        attachment_url = attachment["url"]
        isdocumentedbydict = { "identifier": attachment_url,
                            "scheme": "url",
                            "relation_type": {
                                "id": "isdocumentedby",
                                "title": {
                                    "de": "Wird dokumentiert von",
                                    "en": "Is documented by"
                                }
                            }
                        }
        isdocumentedbyList.append(isdocumentedbydict)
    returnDict = {"related_identifiers": isdocumentedbyList}
    return returnDict

def getDivisionID(exp_hall):
    divisionID = None
    for key, val in division_title_id.items():
        if val.lower() == exp_hall.lower():
            divisionID = key
            break
    if not divisionID:
        divisionID = "OTHERS"
    
    return divisionID
    

def getRightsDict():
    rights = [
                {
                "icon": "cc-by-icon","id": "cc-by-4.0",
                    "props": {
                    "url": "https://creativecommons.org/licenses/by/4.0/legalcode",
                    "scheme": "spdx"
                    },
                    "title": {
                    "en": "Creative Commons Attribution 4.0 International"
                    },
                    "description": {
                    "en": "The Creative Commons Attribution license allows re-distribution and re-use of a licensed work on the condition that the creator is appropriately credited."
                    }
                }
            ]
    returnDict = {"rights": rights}
    return returnDict

def getAccessDict():
    access = {"files": "public", "record": "public", "embargo": {"active": False}}
    files = {"enabled": False}
    returnDict = {"access": access, "files": files}
    return returnDict

def transform(entry):
    inveniodict = {"metadata": {},"custom_fields": {}}
    inveniodict["communities"] =  {"ids": [COMMUNITYID]}

    inveniodict["metadata"]["title"] = entry.get("title")
    inveniodict["metadata"]["resource_type"]= {"id": "publication-proposal"}
    inveniodict["metadata"]["publication_date"]= entry.get("submitted_date")
    inveniodict["metadata"].update(getRightsDict())
    inveniodict["metadata"].update(getAccessDict())

    authors = entry.get("authors", "")
    if authors:
        authorDict = getAuthorDict(authors)
        inveniodict["metadata"].update(authorDict)
    
    spokespersons = entry["spokespersons"]
    if spokespersons:
        spokespersonDict = getAuthorDict(spokespersons, spokesperson=True)
        inveniodict["metadata"].update(spokespersonDict)
    
    contactpersons = entry.get("contactpersons","")
    if contactpersons:
        contactpersonDict = getAuthorDict(contactpersons, contactPerson=True)
        inveniodict["metadata"].update(contactpersonDict)

    if entry["links"]:
        linkDict = getLinksDict(entry["links"])
        inveniodict["metadata"].update(linkDict)
    
    if entry["attachments"]:
        attachmentDict = getAttachmentDict(entry["attachments"])
        inveniodict["metadata"].update(attachmentDict)

    inveniodict["custom_fields"].update({"pac:pac_number": int(entry.get('pac_number')),
                                         "pac:proposal_number": entry.get("proposal_number",""),
                                         "pac:pacID": entry.get("id","")
                                         })
    beam_days = entry.get("beam_days","")
    if beam_days:
        inveniodict["custom_fields"].update({"pac:beam_days" : float(beam_days)})
    rating = entry.get("rating","")
    if rating:
        inveniodict["custom_fields"].update({"pac:pac_rating": {"id": rating}})
    
    status = entry.get("status","")
    if status:
        statusID = statusID(status)
        inveniodict["custom_fields"].update({"pac:pac_status": {"id": statusID}})

    experiment_number = entry.get("experiment_number","")
    if experiment_number:
        inveniodict["custom_fields"].update({"pac:experiment_number": experiment_number})
    
    experimental_hall = entry.get("experiment_hall","")
    if experimental_hall:
        divisonid = getDivisionID(experimental_hall)
        inveniodict["custom_fields"].update({"rdm:division": [{"id": divisonid}]})
    
    return inveniodict

def uploadNew(invenioDict):
    pacID = invenioDict["custom_fields"]["rdm:pubID"]
    ifExistsUrl = f'{INVENIOHOST}/api/records?q=custom_fields.rdm\\:pubID:"{pacID}"&l=list&p=1&s=10&sort=bestmatch'
    res = requests.get(ifExistsUrl)
    if res.status_code == 200:
        if res.json()['hits']['total'] != 0:
            logger.info(f"Record with pubID {pacID} already exists")
            return False
    if res.json()['hits']['total'] == 0:
        createURL = f"{INVENIOHOST}/api/records"
        createRes = requests.post(createURL, data=json.dumps(invenioDict), headers=h,verify=True)
        if createRes.status_code == 201:
            record_id = createRes.json()['id']
            reviewURL = f'{INVENIOHOST}/api/records/{record_id}/draft/review'
            reviewData = {"receiver": { "community": COMMUNITYID},"type": "community-submission"}
            reviewRes = requests.put(reviewURL, data=json.dumps(reviewData), headers=h,verify=True)
            if reviewRes.status_code == 200:
                submitData =  {"payload": {"content": "Thank you in advance for the review.","format": "html"}}
                submitURL = reviewRes.json()['links']['actions']['submit']
                submitRes = requests.post(submitURL, data=json.dumps(submitData), headers=h,verify=True)
                if submitRes.status_code in [202, 200]:
                        logger.info("success submit for review")
                        acceptURL = submitRes.json()['links']['actions']['accept']
                        acceptData = {"payload": {"content": "You are in!", "format": "html"}}
                        acceptRes = requests.post(acceptURL, data=json.dumps(acceptData), headers=h,verify=True)
                        if acceptRes.status_code in [202, 200]:
                            logger.info("Whole upload, review, submit and accept OK")
                        else:
                            logger.info(acceptRes.status_code)
                            logger.info(acceptRes.json())
                            return False
                else:
                    logger.error(submitRes.status_code)
                    logger.error(submitRes.json())
                    return False
            else:
                logger.error(reviewRes.status_code)
                logger.error(reviewRes.json())
                return False
        else:
            logger.error(createRes.status_code)
            logger.error(createRes.json())
            return False

    return True


def uploadModify(invenioDict):
    pacID = invenioDict["custom_fields"]["rdm:pubID"]
    ifExistsUrl = f'{INVENIOHOST}/api/records?q=custom_fields.rdm\\:pubID:"{pacID}"&l=list&p=1&s=10&sort=bestmatch'
    res = requests.get(ifExistsUrl)
    if res.status_code == 200:
        if res.json()['hits']['total'] == 0:
            logger.info(f"Record with pubID {pacID} does not exist")
            logger.info("This should mean record is new")
            logger.info("This should NOT happend check with MIS group")
            uploadNew()
            return True
        if res.json()['hits']['total'] !=0:
            recordID = res.json()['hits']['hits'][0]["id"]
            createNewVersionURL = f'{INVENIOHOST}/api/records/{recordID}/versions'
            createNewVersionData = {}
            newVersionRes = requests.post(createNewVersionURL,data=json.dumps(createNewVersionData), headers=h,verify=True)
            if newVersionRes.status_code == 200:
                publishNewVersionURL = f'{INVENIOHOST}/api/records/{recordID}/draft/actions/publish'
                publishNewVersionRes= requests.post(publishNewVersionURL,headers=h,verify=True)
                if publishNewVersionRes.status_code == 202:
                    logger.info("success publish new version")
                else:
                    logger.error(publishNewVersionRes.status_code)
                    logger.error(publishNewVersionRes.json())
                    return False
            else:
                logger.error(newVersionRes.status_code)
                logger.error(newVersionRes.json())
                return False
    else:
        logger.error(res.status_code)
        logger.error(res.json())
        return False
    
    return True
    

        
def callPACDB(action, submit_date_after = '', pac_number = '', modification_date = ''):
    if not any([submit_date_after, pac_number, modification_date]):
        return "OK"
    
    isModify = False
    isNew = False

    if action == "new":
        if not submit_date_after:
            logger.error("submit_date_after is needed for action new")
            return False
        isNew = True
    elif action == "modify":
        if not modification_date:
            logger.error("modification_date is needed for action modify")
            return False
        isModify = True
    else:
        logger.error(f"action {action} not recognized. Available action: new or modify")
        return False

    invenioDictList = []
    pacDBURL = 'https://misportal.jlab.org/pacProposals/proposals/download.json'
    pacDBParams = {
        'pac_number': pac_number,
        'type_id': '',
        'submit_date_after': submit_date_after,
        'submit_date_before': '',
        'modification_date': modification_date}
    pacDBRes = requests.get(pacDBURL, params=pacDBParams)
    if pacDBRes.status_code == 200:
        data_dict = pacDBRes.json()
        data = data_dict["data"]
        for entry in data:
            invenioDict = transform(entry)
            invenioDictList.append(invenioDict)
    else:
        logger.error(pacDBRes.status_code)
        logger.error(pacDBRes.json())
        return False
    
    #think about how to call upload

yesterday = datetime.now() - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y-%m-%d")

# First call with submit_date_after of yesterday
callPACDB("new", submit_date_after=yesterday_str)
callPACDB("modify", modification_date=yesterday_str)
