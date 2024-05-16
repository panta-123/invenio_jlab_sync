import requests
import glob
import json
import re
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler
import idutils
# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

INVENIOHOST = "https://inveniordm.jlab.org"
TOKEN = ""
COMMUNITYID = "69cf8901-1a33-44c6-83fa-04b4acf24941"
LOG_DIR = "logs/pub"
FAILED_DIR = "failed/pub"

# Define log file name with timestamp and rotation

log_file = datetime.now().strftime(f"{LOG_DIR}/pubdb_sync_logs_%Y-%m-%d.log")
handler = RotatingFileHandler(log_file, maxBytes=100000, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

h = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
        }

division_title_id = {
    "12 Gev Director's Office" : "12DO",
    "Accelerator Ops, R&D" : "AORD",
    "CFO Div Summary" : "CFO",
    "Chief Operting Officr Off" : "COO",
    "Chief Scientist Office" : "CSO",
    "Directorate" : "DIR",
    "EIC" : "EIC",
    "ES&H Division" : "ESHD",
    "Comp Sci&Tech (CST) Div" : "CSDS",
    "Engineering Division" : "ENG",
    "Exp Nuclear Physics / Technical Support Groups" : "ENPTSG-CP",
    "Exp Nuclear Physics / Physics Division Office" : "ENPH-PDO-ADMIN",
    "Exp Nuclear Physics / Experimental Halls / Physics Magnet": "ENPH-EH-PM",
    "Exp Nuclear Physics / Experimental Halls / Hall A": "ENPH-EH-HA",
    "Exp Nuclear Physics / Experimental Halls / Hall B": "ENPH-EH-HB",
    "Exp Nuclear Physics / Experimental Halls / Hall C": "ENPH-EH-HC",
    "Exp Nuclear Physics / Experimental Halls / Hall D": "ENPH-EH-HD",
    "Exp Nuclear Physics / Experimental Halls / Physics EIC": "ENPH-EHPH",
    "Exp Nuclear Physics / Experimental Halls / Hall B&D Technical Support" : "ENPH-EHBDTS",
    "Exp Nuclear Physics / OTHERS" : "ENPH-OTHER",
    "FEL & CTO": "FEL-CTO",
    "Facilities & Logistcs Mgt": "FLM",
    "Proj Mgmt & Integration" : "PMI",
    "SNS PPU" : "SPPU",
    "Theory & Comp Physics": "TCP",
    "OTHERS" : "OTHERS"
}

def cleanedName(fullname: str) -> dict[str, str]:
    """
    Splits the full name into given and family names.
    
    Args:
        fullname (str): The full name of a person.
        
    Returns:
        dict: A dictionary with given_name and family_name.
    """
    names = fullname.split()
    # The first name includes the middle name if present
    first_name = ' '.join(names[:-1])
    # Last name is the last element in the names list
    last_name = names[-1]
    return {"type": "personal", "given_name":first_name, "family_name":last_name}

def getPublicationDate(publication_date: str) -> str:
    """
    Formats the publication date to 'YYYY-MM' format.
    
    Args:
        publication_date (str): The publication date in 'Month YYYY' format.
        
    Returns:
        str: The formatted publication date.
    """

    try:
        date_obj = datetime.strptime(publication_date, '%B %Y')
        publication_date = date_obj.strftime('%Y-%m')
    except Exception as err:
        publication_date = publication_date.split()[-1]
    return publication_date

def getExperimentDict(experimentList):
    expIDList = []
    experimentNumberList = []
    for experiment in experimentList:
        experimentNumber = experiment.get('paperid',"")
        expIDpattern = r'\d+'
        expIDSplits= re.findall(expIDpattern, experimentNumber)
        if expIDSplits:
            expID = ''.join(expIDSplits)
            expIDList.append(expID)
        experimentNumberList.append(experimentNumber)
    returnDict = {"rdm:experiment_number": experimentNumberList, "rdm:expID": list(set(expIDList))}
    return returnDict


def getDivisionDict(division):
    if not division:
        return {"rdm:division": [{"id": "OTHERS"}]}
    division_split = division.split("/")
    if division_split:
        firstDiv = division_split[0].strip()
        if firstDiv.startswith("Exp"):
            try:
                divisonid = division_title_id[division]
            except Exception as err:
                divisonid = "ENPH-OTHER"
        else:
            try:
                divisonid = division_title_id[firstDiv]
            except Exception as err:
                divisonid = "OTHERS"
    
    return {"rdm:full_division": division, "rdm:division": [{"id": divisonid}]}

def getAuthorDict(authors):
    author_list = []
    for author in authors:
        author_name = author.get("name","")
        if author_name:
            authorNameDict = cleanedName(author_name)
            institution = author['institution']
            try:
                institution_fullname = author['institution_fullname'].split(",", 1)[0]
            except Exception as err:
                institution_fullname = institution
            if not institution_fullname:
                authdict = {"person_or_org":authorNameDict,
                            "role": {"id": "researcher"}}
            else:
                authdict = {"person_or_org":authorNameDict,
                            "affiliations":[{"name":institution_fullname}],
                            "role": {"id": "researcher"}}
            author_list.append(authdict)
    returnDict = {"creators" : author_list}
    return returnDict

def getLinksDict(links):
    html_record_url = links["html_record_url"]
    json_record_url = links["json_record_url"]
    isderivedfromdict =  { "identifier": html_record_url,
                            "scheme": "url",
                            "relation_type": {
                                "id": "isderivedfrom"
                            }
                        }
    returnDict = {"related_identifiers": [isderivedfromdict]}
    return returnDict

def getAttachmentDict(attachments):
    isdocumentedbyList = []
    for attachment in attachments:
        attachment_url = attachment["url"]
        attachment_name = attachment["name"]
        attachment_type = attachment["type"]
        isdocumentedbydict = { "identifier": attachment_url,
                            "scheme": "url",
                            "relation_type": {
                                "id": "isdocumentedby"
                            }
                        }
        isdocumentedbyList.append(isdocumentedbydict)

    returnDict = {"related_identifiers": isdocumentedbyList}
    return returnDict

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

def getLDRDDict(ldrd, proposals= []):
    returnDict = {}
    if ldrd.lower() == "yes":
        returnDict["rdm:isldrd"] =  True
    if proposals:
        for proposal in proposals:
            ldrd_num = proposal["proposal_num"]
            returnDict["rdm:ldrd_number"] = ldrd_num
    return returnDict

def fundingDict(funding):
    returnDict = {}
    return returnDict

def getDocumentDict(entry):
    returnDict = {"metadata":{"contributors" : []},"custom_fields":{}}
    if 'document_type' in entry:
        document_type = entry['document_type']
        if document_type.lower() == "journal article":
            returnDict["metadata"]["resource_type"] = {"id": "publication-article"}
            journal_name = entry.get("journal_name","")
            volume = entry.get('volume',"")
            issue = entry.get('issue',"")
            pages = entry.get('pages',"")
            returnDict["custom_fields"]["journal:journal"]= {"title": journal_name, "issue": issue, "volume": volume,"pages": pages}

        elif document_type.lower() == "thesis":
            returnDict["metadata"]["resource_type"] = {"id": "publication-thesis"}
            awarding_university = entry["primary_institution"].split(",")[0]
            returnDict["custom_fields"]["thesis:university"] = awarding_university
            advisorList= entry.get("theses","")

            if advisorList:
                advisor_invenio_list = []
                for advisor in advisorList:
                    advisor_name = advisor.get("advisor","")
                    advisor_affiliation = advisor.get('institution',"")
                    if advisor_name:
                        advisorNameDict = cleanedName(advisor_name)
                        advidict = {"person_or_org":advisorNameDict,
                                "role": {"id": "supervisor"}}
                        if advisor_affiliation:
                            advidict["affiliations"] = [{"name": advisor_affiliation}]
                        advisor_invenio_list.append(advidict)
                returnDict["metadata"]["contributors"]+= advisor_invenio_list

        elif document_type.lower() == "book":
            returnDict["metadata"]["resource_type"] = {"id": "publication-book"}
            book_title = entry.get("book_title","")
            returnDict["custom_fields"]["imprint:imprint"] = {"title": book_title}

        elif document_type.lower() == "meeting":
            document_subtype = entry.get("document_subtype","")
            if "talk" in document_subtype.lower():
                returnDict["metadata"]["resource_type"] = {"id": "presentation"}
            elif "poster" in document_subtype.lower():
                returnDict["metadata"]["resource_type"] = {"id": "poster"}
            elif "paper" in document_subtype.lower():
                 returnDict["metadata"]["resource_type"] = {"id": "publication-conferenceproceeding"}
            else:
                returnDict["metadata"]["resource_type"] = {"id": "other"}
            meeting_name = entry.get("meeting_name","")
            meeting_date = entry.get("meeting_date","")
            returnDict["custom_fields"]["meeting:meeting"]= {"dates": meeting_date,"title": meeting_name, 
                                                #"place": "Here","acronym": "MC",
                                                #"session_part": "1",
                                                #"session": "VI",
                                                }

        elif document_type.lower() == "Proceedings":
            returnDict["metadata"]["resource_type"] = {"id": "publication-conferenceproceeding"}
            proceeding_title = entry.get("proceeding_title","")
            if proceeding_title:
                journal_name = entry.get("publisher","")
                volume = entry.get('volume',"")
                issue = entry.get('issue',"")
                pages = entry.get('pages',"")
                returnDict["custom_fields"]["journal:journal"]= {"title": journal_name, "issue": issue,
                                                    "volume": volume,"pages": pages}

        elif document_type.lower() == "other":
            returnDict["metadata"]["resource_type"] = {"id": "other"}
        else:
            returnDict["metadata"]["resource_type"] = {"id": "other"}
    return returnDict


def transform(entry):
    inveniodict = {"metadata": {"related_identifiers":[], "identifiers":[]},"custom_fields": {}}

    submit_date = entry['submit_date']
    inveniodict["metadata"]["dates"] = [{"date": submit_date,
                                         "type": {"id": "submitted"}}]
    publication_date = entry["publication_date"]
    inveniodict["metadata"]["publication_date"] = getPublicationDate(publication_date)

    submitter_name = entry.get('submitter_name',"")
    if submitter_name:
        submitter_cleaned_name = re.sub(r'\([^)]*\)', '', submitter_name).strip()
        submitterNameDict = cleanedName(submitter_cleaned_name)
        
    
    inveniodict["metadata"]["title"] = entry['title']
    inveniodict["metadata"]["description"] = entry['abstract']

    division = entry.get('affiliation', "")
    divisionDict = getDivisionDict(division)
    inveniodict["custom_fields"].update(divisionDict)

    jlab_number = entry.get("jlab_number",None)
    if jlab_number:
        inveniodict["custom_fields"].update({"rdm:jlab_number": jlab_number})

    osti_number = entry.get("osti_number", None)
    if osti_number:
        inveniodict["custom_fields"].update({"rdm:osti_number": osti_number})
        

    lanl_number = entry.get("lanl_number", None)
    if lanl_number:
        detected_schemes = idutils.detect_identifier_schemes(lanl_number)
        if detected_schemes:
            # If other schemas are present, add them to identifiers list
            other_schemas = [schema for schema in detected_schemes if schema != "url"]
            if other_schemas:
                for schema in other_schemas:
                    inveniodict["metadata"]["identifiers"].append({"identifier": lanl_number, "scheme": schema})
            else:
                # If only "url" is detected, add the DOI link as a URL
                inveniodict["metadata"]["identifiers"].append({"identifier": lanl_number, "scheme": "url"})
        else:
            inveniodict["custom_fields"].update({"rdm:lanl_number": lanl_number})
        

    inveniodict["custom_fields"].update({"rdm:pubID": int(entry["pub_id"])})

    if "ldrd_funding" in entry:
        if entry["proposals"]:
            ldrdDict = getLDRDDict(entry["ldrd_funding"], entry["proposals"])
        else:
            ldrdDict = getLDRDDict(entry["ldrd_funding"])
        inveniodict["custom_fields"].update(ldrdDict)
    
    if entry.get("experiments",""):
        experimentDict = getExperimentDict(entry["experiments"])
        inveniodict["custom_fields"].update(experimentDict)

    if entry["attachments"]:
        attachmentDict = getAttachmentDict(entry["attachments"])
        inveniodict["metadata"]["related_identifiers"]  += attachmentDict["related_identifiers"]

    if entry["links"]:
        linkDict = getLinksDict(entry["links"])
        inveniodict["metadata"]["related_identifiers"]  += linkDict["related_identifiers"]

    inveniodict["metadata"].update(getRightsDict())
    inveniodict.update(getAccessDict())
    inveniodict["communities"] =  {"ids": [COMMUNITYID]}


    authors = entry.get("authors", "")
    if authors:
        authorDict = getAuthorDict(authors)
        inveniodict["metadata"].update(authorDict)
    if not authors:
        inveniodict["metadata"]["creators"] = {"person_or_org": {"type": "organizational",
                                                        "name": "Thomas Jefferson National Accelerator Facility"},
                                                        "role": {
                                                            "id": "other"}}

    documentDict = getDocumentDict(entry)
    inveniodict["metadata"].update(documentDict["metadata"])
    doi_link = entry.get("doi_link", "")
    if doi_link:
        # check which schema it is from
        detected_schemes = idutils.detect_identifier_schemes(doi_link)
        if detected_schemes:
            # If other schemas are present, add them to identifiers list
            other_schemas = [schema for schema in detected_schemes if schema != "url"]
            if other_schemas:
                for schema in other_schemas:
                    inveniodict["metadata"]["identifiers"].append({"identifier": doi_link, "scheme": schema})
            else:
                # If only "url" is detected, add the DOI link as a URL
                inveniodict["metadata"]["identifiers"].append({"identifier": doi_link, "scheme": "url"})

    inveniodict["custom_fields"].update(documentDict["custom_fields"])

    return inveniodict

def writeToFile(data, file= "defaultName"):
    timestamp = datetime.now().strftime("%Y-%m-%d")
    filename = f"{FAILED_DIR}/{file}_{timestamp}.json"
    with open(filename, "w") as file:
        json.dump(data, file)

def uploadNew(invenioDict):
    pubID = invenioDict["custom_fields"]["rdm:pubID"]
    ifExistsUrl = f'{INVENIOHOST}/api/records?q=custom_fields.rdm\\:pubID:"{pubID}"&l=list&p=1&s=10&sort=bestmatch'
    res = requests.get(ifExistsUrl)
    if res.status_code == 200:
        if res.json()['hits']['total'] != 0:
            logger.info(f"Record with pubID {pubID} already exists")
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
                writeToFile(invenioDict, file="failed_to_create_draft")
                return False

    return True


def uploadModify(invenioDict):
    pubID = invenioDict["custom_fields"]["rdm:pubID"]
    ifExistsUrl = f'{INVENIOHOST}/api/records?q=custom_fields.rdm\\:pubID:"{pubID}"&l=list&p=1&s=10&sort=bestmatch'
    res = requests.get(ifExistsUrl)
    if res.status_code == 200:
        if res.json()['hits']['total'] == 0:
            logger.info(f"Record with pubID {pubID} does not exist")
            logger.info("This should mean record is new")
            logger.info("This should NOT happen but we will register it as new.")
            uploadNew(invenioDict)
            return True
        if res.json()['hits']['total'] !=0:
            recordID = res.json()['hits']['hits'][0]["id"]
            createNewVersionURL = f'{INVENIOHOST}/api/records/{recordID}/versions'
            newVersionRes = requests.post(createNewVersionURL,data={}, headers=h,verify=True)
            new_data = newVersionRes.json()
            new_data.update(invenioDict)
            if newVersionRes.status_code in [200, 201]:
                updatedraftRecordURL =  newVersionRes.json()['links']["self"] #f'{INVENIOHOST}/api/records/{recordID}/draft'
                updatedraftRecord = requests.put(updatedraftRecordURL,data=json.dumps(new_data), headers=h,verify=True)
                if updatedraftRecord.status_code == 200:
                    logger.info("success update draft record")
                    publishNewVersionURL =updatedraftRecord.json()['links']["publish"]  #f'{INVENIOHOST}/api/records/{recordID}/draft/actions/publish'
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
                writeToFile(invenioDict, file="failed_to_create_new_version")
                return False
    else:
        logger.error(res.status_code)
        logger.error(res.json())
        return False
    return True


def callPUBDB(action, submit_date_after = '',
              submit_date_before = '',
              modification_date_after = '',
              modification_date_before = '',
              pub_year = ''):
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
    pubDBURL = 'https://misportal.jlab.org/sti/publications/search.json'
    pubDBParams = {
        'action': 'search',
        'commit': 'Search',
        'controller': 'publ_mains',
        'json_download': 'true',
        'search[abstract]': '',
        'search[author_name]': '',
        'search[department]': '',
        'search[division]': '',
        'search[document_number]': '',
        'search[grp]': '',
        'search[journal_id]': '',
        'search[meeting_id]': '',
        'search[proposal_num]': '',
        'search[pub_type]': '',
        'search[pub_year]': pub_year,
        'search[publ_author_ID]': '',
        'search[publ_author_NAME]': '',
        'search[publ_signer_ID]': '',
        'search[publ_signer_NAME]': '',
        'search[publ_submitter_ID]': '',
        'search[publ_submitter_NAME]': '',
        'search[published_only]': 'N',
        'search[submit_date_after]':submit_date_after,
        'search[submit_date_before]':submit_date_before,
        'search[updated_date_after]':modification_date_after,
        'search[updated_date_before]':modification_date_before,
        'search[title]': '',
        'utf8': '✓'
    }
    pubDBRes = requests.get(pubDBURL, params=pubDBParams)
    jsonRecordURLList = []
    newVersionJsonURLList = []
    if pubDBRes.status_code == 200:
        dataJSON = pubDBRes.json()
        dataList = dataJSON["data"]
        for  dat in dataList:
            json_record_url = dat["json_record_url"]
            modification_date   = dat["modification_date"]
            submit_date = dat["submit_date"]
            if isModify:
                if submit_date == modification_date:
                    logger.info("When modify is called and same submit and modify date,\
                                 do nothing")
                else:
                    newVersionJsonURLList.append(json_record_url)
            else:
                jsonRecordURLList.append(json_record_url)
    else:
        logger.error(pubDBRes.status_code)
        logger.error(pubDBRes.json())
        return False

    if newVersionJsonURLList:
        for URL in jsonRecordURLList:
            pubDBResEachJSON = requests.get(URL)
            if pubDBResEachJSON.status_code == 200:
                dataJSON = pubDBResEachJSON.json()
                invenioDict = transform(dataJSON)
                newVersionInvenioDictList.append(invenioDict)

    if jsonRecordURLList:
        for URL in jsonRecordURLList:
            pubDBResEachJSON = requests.get(URL)
            if pubDBResEachJSON.status_code == 200:
                dataJSON = pubDBResEachJSON.json()
                invenioDict = transform(dataJSON)
                invenioDictList.append(invenioDict)

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
    callPUBDB("new", submit_date_after=yesterday_str, submit_date_before=today_str)
    callPUBDB("modify", modification_date_after=yesterday_str, modification_date_before=today_str)

if __name__ == "__main__":
    main()
