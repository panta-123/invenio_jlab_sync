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
log_file = datetime.now().strftime("pubdb_sync_logs_%Y-%m-%d.log")
handler = RotatingFileHandler(log_file, maxBytes=100000, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

INVENIOHOST = "https://inveniordm.jlab.org"
TOKEN = ""
COMMUNITYID = "2133e693-e129-43e8-a436-bc35e1d49052"

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

def cleanedName(fullname):
    names = fullname.split()
    # The first name includes the middle name if present
    first_name = ' '.join(names[:-1])
    # Last name is the last element in the names list
    last_name = names[-1]
    return {"type": "personal", "given_name":first_name, "family_name":last_name}

def getPublicationDate(publication_date):
    try:
        date_obj = datetime.strptime('December 2015', '%B %Y')
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
            expID = int(''.join(expIDSplits))
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
                            "role": {"id": "researcher", 
                                    "title": 
                                    {"de": "WissenschaftlerIn",
                                    "en": "Researcher"}},}
            else:
                authdict = {"person_or_org":authorNameDict,
                            "affiliations":[{"name":institution_fullname}],
                            "role": {"id": "researcher", 
                                    "title": 
                                    {"de": "WissenschaftlerIn",
                                    "en": "Researcher"}},}
            author_list.append(authdict)
    returnDict = {"creators" : author_list}
    return returnDict

def getLinksDict(links):
    html_record_url = links["html_record_url"]
    json_record_url = links["json_record_url"]
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
        attachment_name = attachment["name"]
        attachment_type = attachment["type"]
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

def getDocumentDict(entry):
    returnDict = {"metadata":{},"custom_fields":{}}
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
            advisorList= entry["theses"]
            if advisorList:
                author_invenio_list = []
                for advisor in advisorList:
                    advisor_name = advisor["advisor"]
                    advisor_affiliation = advisor['institution']
                    if advisor_name:
                        advisorNameDict = cleanedName(advisor_name)
                        advidict = {"person_or_org":advisorNameDict,
                                "role": {"id": "supervisor",
                                        "title": {
                                            "en": "Supervisor"
                                            }},}
                        author_invenio_list.append(advidict)
                returnDict["metadata"]["contributors"].append(advisorNameDict)

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
    inveniodict = {"metadata": {},"custom_fields": {}}

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
    inveniodict["custom_fields "].update(divisionDict)

    jlab_number = entry.get("jlab_number",None)
    if jlab_number:
        inveniodict["custom_fields "].update({"rdm:jlab_number": jlab_number})

    osti_number = entry.get("osti_number", None)
    if osti_number:
        inveniodict["custom_fields "].update({"rdm:osti_number": osti_number})

    lanl_number = entry.get("lanl_number", None)
    if lanl_number:
         inveniodict["custom_fields "].update({"rdm:lanl_number": lanl_number})

    inveniodict["custom_fields "].update({"rdm:pubID": entry["pub_id"]})

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
        inveniodict["metadata"].update(attachmentDict)
    if entry["links"]:
        linkDict = getLinksDict(entry["links"])
        inveniodict["metadata"].update(linkDict)

    inveniodict["metadata"].update(getRightsDict())
    inveniodict["metadata"].update(getAccessDict())
    inveniodict["communities"] =  {"ids": [COMMUNITYID]}


    authors = entry.get("authors", "")
    if authors:
        authorDict = getAuthorDict(authors)
        inveniodict["metadata"].update(authorDict)
    if not authors:
        inveniodict["metadata"]["creators"] = {"person_or_org": {"type": "organizational",
                                                        "name": "Thomas Jefferson National Accelerator Facility"},
                                                        "role": {
                                                            "id": "other",
                                                            "title": {"en": "Other"}}}

    documentDict = getDocumentDict(entry)
    inveniodict["metadata"].update(documentDict["metadata"])
    inveniodict["custom_fields"].update(documentDict["custom_fields"])

    return inveniodict

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


def callPUBDB(action, submit_date_after = '', pub_year = '', modification_date = ''):
    if not any([submit_date_after, pub_year, modification_date]):
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
        'search[submit_date_before]':'',
        'search[modification_date]':modification_date,
        'search[title]': '',
        'utf8': '✓'
    }
    pubDBRes = requests.get(pubDBURL, params=pubDBParams)
    jsonRecordURLList = []
    if pubDBRes.status_code == 200:
        dataJSON = pubDBRes.json()
        dataList = dataJSON["data"]
        for  data in dataList:
            json_record_url = data["json_record_url"]
            modification_date   = data["modification_date"]
            submit_date = data["submit_date"]
            if submit_date == modification_date:
                isSMDateSame = False
            else:
                isSMDateSame = True
            jsonRecordURLList.append(json_record_url)
    else:
        logger.error(pubDBRes.status_code)
        logger.error(pubDBRes.json())
        return False

    for URL in jsonRecordURLList:
        pubDBResEachJSON = requests.get(URL)
        if pubDBResEachJSON.status_code == 200:
            dataJSON = pubDBResEachJSON.json()
            invenioDict = transform(dataJSON)
            invenioDictList.append(invenioDict)

    if isModify:
        if not isSMDateSame:
            for tomod in invenioDictList:
                upload_modify = uploadModify(tomod)
            return True
        else:
            logger.info("modification_date is same as submit_date. No need to do anything.")
    else:
        for toup in invenioDictList:
            upload_new = uploadNew(toup)
            logger.info("Succesfully handles new records.")
        
    return True


yesterday = datetime.now() - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y-%m-%d")

# First call with submit_date_after of yesterday
callPUBDB("new", submit_date_after=yesterday_str)
callPUBDB("modify", modification_date=yesterday_str)
