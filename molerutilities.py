import requests
import re
import configparser


config = configparser.ConfigParser()
config.read('config.ini')
MY_OC_KEY = config[ 'opencorporates' ][ "MY_OC_KEY" ]  # Opencorporates API key.

GROUP_STUB = 'https://api.opencorporates.com/v0.4/corporate_groupings//'
API_NETWORK_STUB = 'https://api.opencorporates.com/v0.4/companies/'


# LISTS

uk_country_list = ["England", "Wales", "Scotland", "Northern Ireland", "UK", "U.K.", "United Kingdom"]

uk_city_list = ["Bath", "Birmingham", "Bradford", "Brighton and Hove", "Bristol", "Cambridge", "Canterbury", "Carlisle",
                "Chester", "Chichester", "Coventry", "Derby", "Durham", "Ely", "Exeter", "Gloucester", "Hereford",
                "Kingston upon Hull", "Lancaster", "Leeds", "Leicester", "Lichfield", "Lincoln", "Liverpool",
                "City of London", "Manchester", "Newcastle", "upon", "Tyne", "Norwich", "Nottingham", "Oxford",
                "Peterborough", "Plymouth", "Portsmouth", "Preston", "Ripon", "Salford", "Salisbury", "Sheffield",
                "Southampton", "St", "Albans", "Stoke-on-Trent", "Sunderland", "Truro", "Wakefield", "Wells",
                "Westminster", "Winchester", "Wolverhampton", "Worcester", "York"]

uk_region_list = ["Bedfordshire", "Buckinghamshire", "Cambridgeshire", "Cheshire", "Cleveland", "Cornwall", "Cumbria",
                  "Derbyshire", "Devon", "Dorset", "Durham", "East Sussex", "Essex", "Gloucestershire",
                  "Greater London", "Greater Manchester", "Hampshire", "Hertfordshire", "Kent", "Lancashire",
                  "Leicestershire", "Lincolnshire", "Merseyside", "Norfolk", "North Yorkshire", "Northamptonshire",
                  "Northumberland", "Nottinghamshire", "Oxfordshire", "Shropshire", "Somerset", "South Yorkshire",
                  "Staffordshire", "Suffolk", "Surrey", "Tyne and Wear", "Warwickshire", "West Berkshire",
                  "West Midlands", "West Sussex", "West Yorkshire", "Wiltshire", "Worcestershire", "Flintshire",
                  "Glamorgan", "Merionethshire", "Monmouthshire", "Montgomeryshire", "Pembrokeshire", "Radnorshire",
                  "Anglesey", "Breconshire", "Caernarvonshire", "Cardiganshire", "Carmarthenshire", "Denbighshire",
                  "Aberdeen City", "Aberdeenshire", "Angus", "Argyll and Bute", "City of Edinburgh", "Clackmannanshire",
                  "Dumfries and Galloway", "Dundee City", "East Ayrshire", "East Dunbartonshire", "East Lothian",
                  "East Renfrewshire", "Eilean Siar", "Falkirk", "Fife", "Glasgow City", "Highland", "Inverclyde",
                  "Midlothian", "Moray", "North Ayrshire", "North Lanarkshire", "Orkney Islands", "Perth and Kinross",
                  "Renfrewshire", "Scottish Borders", "Shetland Islands", "South Ayrshire", "South Lanarkshire",
                  "Stirling", "West Dunbartonshire", "West Lothian", "Antrim", "Armagh", "Down", "Fermanagh",
                  "Derry and Londonderry", "Tyrone"]



# GEO CHECK FUNCTIONS

def check_city(city_to_check):
    if city_to_check.title () in uk_city_list:
        return True
    return False


def check_country(country_to_check):
    if country_to_check.title () in uk_country_list:
        return True
    return False


def check_region(region_to_check):
    if region_to_check.title () in uk_region_list:
        return True
    return False


def check_postal(post_code):
    post_code_pattern = "^[a-zA-Z]{1,2}([0-9]{1,2}|[0-9][a-zA-Z])\s*[0-9][a-zA-Z]{2}$"
    if re.match(post_code_pattern, post_code):
        return True
    return False


# DICTIONARY FUNCTIONS

def extend_org_dict(org_company_details):
    org_stub = {"address_street": "Not known",
                "address_locality": "Not known",
                "address_postal_code": "Not known",
                "address_region": "Not known",
                "address_country": "Not known",
                "node_id": 0,
                "parent_node": 0,
                "node_type": "Organisation",
                "found_orgs":[],
                "x_network":[],
                "x_ubo":[]}
    org_company_details.update(org_stub)
    return org_company_details


def extend_officer_dict(each_officer,org_oc_name, org_oc_url):
    officer_stub = {"org_name": org_oc_name,
                    "org_oc_url": org_oc_url,
                    "node_id": 0,
                    "parent_node": 0,
                    "alias_label": 0,
                    "alias_nodes": 0,
                    "node_type": "Officer"}
    each_officer["officer"].update (officer_stub)


def extract_sub_dict(parent_dict):
    sub_dict = {key: value for key, value in parent_dict.items()}
    return sub_dict

# Check for Officer As Entity

def check_officer_entity(address_mirror, name_to_check, found_orgs_officers):
    if detect_organisation(name_to_check):
        address_mirror.update({"node_type": "Organisation as Officer"})
        found_tuple = (name_to_check, address_mirror[ "id" ], address_mirror[ "node_type" ])
        # print(f"\nOrg as officer => {found_tuple}")
        found_orgs_officers.insert(0, found_tuple)
    else:
        pass
    return address_mirror, found_orgs_officers


# Called by check officer entity
def detect_organisation(input):
    split_name = input.split(" ")
    for every_word in split_name:
        if every_word in corporate_names:
            return True
    return False

# Used by detect_organisation to check if an Officer might be a corporate Entity.
corporate_names = ["SECRETARIES", "NOMINEES", "SERVICES", "DIRECTOR", "DIRECTORS", "SECRETARIAL", "GROUP",
              "INSTANT COMPANIES LIMITED","LLP","LP","SLP","LIMITED","LTD","LTD.","PLC","Corp.","Inc.",
              "Corporation","CORPORATION","Incorporated","INCORPORATED"]


# DATA CLEANING
# Called by clean_node_labels on name in parse officer details.
def clean_values(element_to_clean):
    x = element_to_clean.replace("NO.1", "").replace(" NO.2", "") # Remove "NO.1" etc.
    x = (re.sub(r'[^A-Za-z0-9]', ' ', x)) # Strips all punctuation from element.
    cleaned_element = re.sub( '\s+', ' ',x).strip() #Replace multiple whitespace with single
    # print(f"{element_to_clean} cleaned => {cleaned_element}")
    return cleaned_element

# General utilities


# Converts standard URL format save by Opencoporates into an API ready form
# then retrieves OC data via REST call.
def convert_url(target_url):
    converted_url = target_url.replace("//", "//api.")
    data_json = requests.get(converted_url + "?api_token=" + MY_OC_KEY).json()
    return data_json


# https://gist.github.com/douglasmiranda/5127251
# Find key in nested dict.

def find_key(key, dictionary):
    for k, v in dictionary.items():
        if k == key:
            yield v
        elif isinstance(v, dict):
            for result in find_key(key, v):
                yield result
        elif isinstance(v, list):
            for d in v:
                for result in find_key(key, d):
                    yield result

