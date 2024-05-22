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

INVENIOHOST = "https://inveniordm.jlab.org"
TOKEN = ""
COMMUNITYID = "7b99f013-91fa-4274-98ec-b465245ef779"
LOG_DIR = "logs/pac"
FAILED_DIR = "failed/pac"

# Define log file name with timestamp and rotation
log_file = datetime.now().strftime(f"{LOG_DIR}/pacdb_sync_logs_%Y-%m-%d.log")
handler = RotatingFileHandler(log_file, maxBytes=100000, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)



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

def getExpIDset(fullname):
    expIDSet = set()
    experimentNumberList = []
    expIDpattern = r'\d+'
    expIDSplits= re.findall(expIDpattern, fullname)
    if expIDSplits:
        expID = ''.join(expIDSplits)
        expIDSet.add(expID)
    experimentNumberList.append(fullname)
    return expIDSet

def processCreators(entry):
    creators = []
    seen_names = set()

    # Combine authors, spokespersons, and contact person into creators list
    authors = entry.get("authors", [])
    spokespersons = entry.get("spokespersons", [])
    contact_person = entry.get("contact_person", {})

    # Add authors to creators list
    for author in authors:
        name = (author["first_name"], author["last_name"])
        if name not in seen_names:
            cDict = {
                "person_or_org": {"type": "personal",
                "given_name": author["first_name"],
                "family_name": author["last_name"]},
                "role": {"id":"researcher"}
            }
            institution = author.get("institution","")
            if institution:
                if institution.lower() == "jefferson lab":
                    institution_fullname = "Thomas Jefferson National Accelerator Facility"
                elif institution.lower() == "jlab":
                     institution_fullname = "Thomas Jefferson National Accelerator Facility"
                else:
                    institution_fullname = institution
                cDict["affiliations"] = [{"name":institution_fullname}]
            creators.append(cDict)
            seen_names.add(name)

    # Add spokespersons to creators list
    for spokesperson in spokespersons:
        name = (spokesperson["first_name"], spokesperson["last_name"])
        if name not in seen_names:
            cDict = {
                "person_or_org": {"type": "personal",
                "given_name": spokesperson["first_name"],
                "family_name": spokesperson["last_name"]},
                "role": {"id":"researcher"}
            }
            institution = spokesperson.get("institution","")
            if institution:
                if institution.lower() == "jefferson jab":
                    institution_fullname = "Thomas Jefferson National Accelerator Facility"
                elif institution.lower() == "jlab":
                     institution_fullname = "Thomas Jefferson National Accelerator Facility"
                else:
                    institution_fullname = institution
                cDict["affiliations"] = [{"name":institution_fullname}]
            creators.append(cDict)
            seen_names.add(name)

    # Extract first and last names for contact person
    if contact_person:
        contact_fullname = contact_person.get("name", "")
        if contact_fullname:
            names = contact_fullname.split()
            first_name = ' '.join(names[:-1])
            last_name = names[-1]
            contact_name = (first_name, last_name)
            if contact_name not in seen_names:
                cDict = {
                    "person_or_org": {"type": "personal",
                    "given_name": first_name,
                    "family_name": last_name},
                    "role": {"id":"researcher"}
                }
                institution = contact_person.get("institution","")
                if institution:
                    if institution.lower() == "jefferson lab":
                        institution_fullname = "Thomas Jefferson National Accelerator Facility"
                    elif institution.lower() == "jlab":
                        institution_fullname = "Thomas Jefferson National Accelerator Facility"
                    else:
                        institution_fullname = institution
                    cDict["affiliations"] = [{"name":institution_fullname}]
                creators.append(cDict)
    if not creators:
        cDict = {
            "person_or_org" : {"type": "personal",
            "given_name": "None Listed",
            "family_name": "None Listed"},
            "role": {"id":"researcher"}}
        creators.append(cDict)
    return {"creators": creators}

def processProjectLeaders(entry):
    projectleaders = {"contributors": []}

    # Combine spokespersons and contact person into projectleaders list
    spokespersons = entry.get("spokespersons", [])
    contact_person = entry.get("contact_person", {})

    # Add spokespersons to projectleaders list
    for spokesperson in spokespersons:
        projectleaders["contributors"].append({
            "person_or_org":{"type": "personal",
            "given_name": spokesperson["first_name"],
            "family_name": spokesperson["last_name"]},
            "role": {"id": "projectleader"}
        })

    # Extract first and last names for contact person in projectleaders list
    if contact_person:
        contact_fullname = contact_person.get("name", "")
        if contact_fullname:
            names = contact_fullname.split()
            first_name = ' '.join(names[:-1])
            last_name = names[-1]
            projectleaders["contributors"].append({
               "person_or_org": {"type": "personal",
                "given_name": first_name,
                "family_name": last_name},
                "role": {"id": "projectleader"}
            })

    return projectleaders

def getstatusID(status):
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
                                "id": "isderivedfrom"
                            }
                        }
    returnDict = {"related_identifiers": [isderivedfromdict]}
    return returnDict

def getAttachmentDict(links):
    pdf_record_url = links["proposal_pdf_url"]
    isdocumentedbydict = { "identifier": pdf_record_url,
                                "scheme": "url",
                                "relation_type": {
                                    "id": "isdocumentedby"}
                                }
    returnDict = {"related_identifiers": [isdocumentedbydict]}
    return returnDict

def getDivisionID(exp_hall):
    divisionID = None
    for key, val in division_title_id.items():
        if key.lower() == exp_hall.lower():
            divisionID = val
            break
    if not divisionID:
        divisionID = "AORD"
    
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
    inveniodict = {"metadata": {"related_identifiers":[]},"custom_fields": {}}
    inveniodict["communities"] =  {"ids": [COMMUNITYID]}

    inveniodict["metadata"]["title"] = entry.get("title")
    inveniodict["metadata"]["resource_type"]= {"id": "publication-proposal"}
    inveniodict["metadata"]["publication_date"]= entry.get("submitted_date")
    inveniodict["metadata"].update(getRightsDict())
    inveniodict.update(getAccessDict())

    creatorsDict = processCreators(entry)
    projectLeaderDict = processProjectLeaders(entry)
    inveniodict["metadata"].update(creatorsDict)
    inveniodict["metadata"].update(projectLeaderDict)

    if entry["links"]:
        linkDict = getLinksDict(entry["links"])
        inveniodict["metadata"]["related_identifiers"]  += linkDict["related_identifiers"]
        pdflinkDict = getAttachmentDict(entry["links"])
        inveniodict["metadata"]["related_identifiers"]  += pdflinkDict["related_identifiers"]
    
    expIDs = set()
    if entry.get("proposal_number",""):
        inveniodict["custom_fields"].update({"pac:proposal_number": entry.get('proposal_number')})
        expID = getExpIDset(entry["proposal_number"])
        expIDs.update(expID)
    

    inveniodict["custom_fields"].update({"pac:pac_number": int(entry.get('pac_number')),
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
        statusID = getstatusID(status)
        inveniodict["custom_fields"].update({"pac:pac_status": {"id": statusID}})

    experiment_number = entry.get("experiment_number","")
    if experiment_number:
        inveniodict["custom_fields"].update({"rdm:experiment_number": [experiment_number]})
        expID = getExpIDset(experiment_number)
        expIDs.update(expID)

    if expIDs:
        inveniodict["custom_fields"].update({"rdm:expID": list(expIDs)})

    experimental_hall = entry.get("experiment_hall","")
    if experimental_hall:
        divisonid = getDivisionID(experimental_hall)
        inveniodict["custom_fields"].update({"rdm:division": [{"id": divisonid}]})

    return inveniodict

def writeToFile(data, file= "defaultName"):
    timestamp = datetime.now().strftime("%Y-%m-%d")
    filename = f"{FAILED_DIR}/{file}_{timestamp}.json"
    with open(filename, "w") as file:
        json.dump(data, file)

def uploadNew(invenioDict):
    pacID = invenioDict["custom_fields"]["pac:pacID"]
    ifExistsUrl = f'{INVENIOHOST}/api/records?q=custom_fields.pac\\:pacID:"{pacID}"&l=list&p=1&s=10&sort=bestmatch'
    res = requests.get(ifExistsUrl,headers=h, verify=True)
    if res.status_code == 200:
        if res.json()['hits']['total'] != 0:
            logger.info(f"Record with pacID {pacID} already exists")
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
                writeToFile(invenioDict, file="PAC_failed_to_create_record")
                return False
    return True

def uploadModify(invenioDict):
    pacID = invenioDict["custom_fields"]["pac:pacID"]
    ifExistsUrl = f'{INVENIOHOST}/api/records?q=custom_fields.pac\\:pacID:"{pacID}"&l=list&p=1&s=10&sort=bestmatch'
    res = requests.get(ifExistsUrl, headers=h,verify=True)
    if res.status_code == 200:
        if res.json()['hits']['total'] == 0:
            logger.info(f"Record with pacID {pacID} does not exist")
            logger.info("This should mean record is new")
            logger.info("This should NOT happend check with MIS group")
            uploadNew(invenioDict)
            return True
        if res.json()['hits']['total'] !=0:
            recordID = res.json()['hits']['hits'][0]["id"]
            createNewVersionURL = f'{INVENIOHOST}/api/records/{recordID}/versions'
            newVersionRes = requests.post(createNewVersionURL,data={}, headers=h,verify=True)
            new_data = newVersionRes.json()
            new_data.update(invenioDict)
            if newVersionRes.status_code in [200, 201]:
                updatedraftRecordURL =  newVersionRes.json()['links']["self"]
                updatedraftRecord = requests.put(updatedraftRecordURL,data=json.dumps(new_data), headers=h,verify=True)
                if updatedraftRecord.status_code == 200:
                    logger.info("success update draft record")
                    publishNewVersionURL =updatedraftRecord.json()['links']["publish"]
                    publishNewVersionRes= requests.post(publishNewVersionURL,headers=h,verify=True)
                    if publishNewVersionRes.status_code == 202:
                        logger.info("success publish new version")
                    else:
                        logger.error("publish error")
                        logger.error(publishNewVersionRes.status_code)
                        logger.error(publishNewVersionRes.json())
                        return False
                else:
                    logger.error("update draft")
                    logger.error(updatedraftRecord.status_code)
                    logger.error(updatedraftRecord.json())
                    return False
            else:
                logger.error(newVersionRes.status_code)
                logger.error(newVersionRes.json())
                writeToFile(invenioDict, file="PAC_failed_to_create_new_version")
                return False
    else:
        logger.error(res.status_code)
        logger.error(res.json())
        return False
    return True
   
def callPACDB(action, submit_date_after = '',
              submit_date_before = '',
              modification_date_after = '',
              modification_date_before = '',
              pac_number = ''):
    isModify = False
    isNew = False
    if action == "new":
        if not (submit_date_after and submit_date_before):
            logger.error("submit_date_after is needed for action new")
            return False
        isNew = True
    elif action == "modify":
        if not (modification_date_after and modification_date_before):
            logger.error("modification_date is needed for action modify")
            return False
        isModify = True
    else:
        logger.error(f"action {action} not recognized. Available action: new or modify")
        return False

    invenioDictList = []
    newVersionInvenioDictList = []
    pacDBURL = 'https://misportal.jlab.org/pacProposals/proposals/download.json'
    pacDBParams = {
        'pac_number': pac_number,
        'type_id': '',
        'submit_date_after': submit_date_after,
        'submit_date_before': submit_date_before,
        'updated_date_after': modification_date_after,
        'updated_date_before': modification_date_before}
    pacDBRes = requests.get(pacDBURL, params=pacDBParams)

    if pacDBRes.status_code == 200:
        dataJSON = pacDBRes.json()
        dataList = dataJSON["data"]
        if not dataList:
            logger.info("No data available for the query. Its OK.")
            return True

        for  entry in dataList:
            modification_date  = entry["updated_date"]
            submit_date = entry["submitted_date"]
            invenioDict = transform(entry)
            if isModify:
                if submit_date == modification_date:
                    logger.info("When modify is called and same submit and modify date,\
                                 do nothing")
                else:
                    newVersionInvenioDictList.append(invenioDict)
            else:
                invenioDictList.append(invenioDict)
    else:
        logger.error(pacDBRes.status_code)
        logger.error(pacDBRes.json())
        return False

    if invenioDictList:
        for invenioDict in invenioDictList:
            uploadNew(invenioDict)

    if newVersionInvenioDictList:
        for invenioDict in newVersionInvenioDictList:
            uploadModify(invenioDict)

today = datetime.now()
today_str = today.strftime("%m/%d/%Y")
yesterday = datetime.now() - timedelta(days=1)
yesterday_str = yesterday.strftime("%m/%d/%Y")

def main():
    callPACDB("new", submit_date_after=yesterday_str, submit_date_before=today_str)
    callPACDB("modify", modification_date_after=yesterday_str, modification_date_before=today_str)

if __name__ == "__main__":
    main()
