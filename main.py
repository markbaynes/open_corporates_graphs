import molerutilities as mu
import itertools
import sys
import csv
import copy
import re
import requests
from requests.utils import quote
import json
# from IPython.display import display
import configparser
from datetime import datetime
import os
import datetime
from fuzzywuzzy import fuzz
from collections import OrderedDict
import pandas as pd
import numpy as np
import time
import networkx as nx
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Need this for NetworkX visualisation in Jupyter Notebook
# get_ipython().magic('matplotlib inline')

# Irritating warning raised when sorting DF after delete dupes.
pd.options.mode.chained_assignment = None  # default='warn'


config = configparser.ConfigParser()
config.read('config.ini')
MY_OC_KEY = config[ 'opencorporates' ][ "MY_OC_KEY" ]  # Opencorporates API key.

# GROUP = quote("FOO")
GROUP = quote ("Housing Associations - London Borough of Tower Hamlets")
# GROUP = quote("G15HA")

GROUP_STUB = 'https://api.opencorporates.com/v0.4/corporate_groupings//'
API_NETWORK_STUB = 'https://api.opencorporates.com/v0.4/companies/'



# Main organisation parsing module. Covers both Ltd and I&PS use cases.
# Called by process_group


# Build a list of the OC URLs for each org in the group
# Convert to API pattern and retrieve that organisations OC object
def retrieve_group_urls(retrieved_group):
	org_total = retrieved_group["results"]["corporate_grouping"]["companies_count"]
	# print (f"{org_total} organisations in {GROUP} to process.\n|*************************|")
	orgs_list = []
	counter = 0
	while counter < org_total:
		organisation_url = \
			retrieved_group["results"]["corporate_grouping"]["memberships"][counter]["membership"][
				"company"]["opencorporates_url"]
		orgs_list.append (mu.convert_url (organisation_url))
		print (f"\rRetrieving...{counter} of {org_total}", end="")
		# sys.stdout.flush ()
		counter += 1
	return orgs_list


def process_group(orgs_list):
	"""Converts list of group organisations data dicts into the main group_dict by building
	 basic data structure required from OC API. (Not all OC data required.)
	 Calls process_orgs on each organisations in group_dict["member_orgs"].
	 found_orgs collects officer records that are organisations."""
	group_dict = {"group_name": GROUP, "member_orgs":[]}
	org_fields_required = [ "name", "company_number", "jurisdiction_code", "incorporation_date", "company_type", "registry_url", "branch_status", "inactive", "officers", "current_status", "opencorporates_url", "registered_address", "controlling_entity", "ultimate_beneficial_owners", "network"]
	for existing_org_dict in orgs_list:
		org_company_details = existing_org_dict["results"]["company"]
		org_details = mu.extract_sub_dict (org_company_details)
		organisation_dict = {k: org_details[k] for k in org_details.keys () & org_fields_required}
		mu.extend_org_dict (organisation_dict)
		group_dict["member_orgs"].append (organisation_dict)
	for each_org in group_dict["member_orgs"]:
		process_orgs(each_org)
	# orgs_to_csv (group_dict)
	return group_dict


def process_orgs(this_dict):
	"""Called by process_group on each organisation dict and returns processed dict via group_dict
	at top level."""
	for k, v in this_dict.items ():
		if k == "ultimate_beneficial_owners":
			if v:  # Check for non-empty list
				this_dict[k] = v # Grab the UBO list.
			else:
				this_dict[k] = "None"
		elif k == "registered_address":
			for k, v in v.items ():
				if k == "locality":
					this_dict["address_locality"] = v
				elif k == "postal_code":
					this_dict["address_postal_code"] = v
				#                     print(f"Org {v}")
				elif k == "region":
					this_dict["address_region"] = "Not known"
				elif k == "country":
					this_dict["address_country"] = v
				elif k == "street_address":
					v = v.replace ('\n', ', ')
					this_dict["address_street"] = v
		elif k == "jurisdiction_code":
			if v:
				this_dict[k] = v.upper ()
		elif k == "incorporation_date":
			if v is None:
				this_dict[k] = "None"
			else:
				this_dict[k] = v
		elif k == "controlling_entity":
			if v is None:
				this_dict[k] = "None"
			elif v is not None:
				this_dict[k] = v
				# print(f"Call CE procedure with {v}\n")
		elif k == "inactive":
			if v is None:
				this_dict[k] = "None"
			else:
				this_dict[k] = "False"
		elif k == "branch_status":
			if v is None:
				this_dict[k] = "Not known"
			else:
				this_dict[k] = v
		elif k == "company_number":
			if v is None:
				this_dict[k] = "Not known"
			else:
				this_dict[k] = v
		elif k == "current_status":
			if v is None:
				this_dict[k] = "Not known"
			else:
				this_dict[k] = v
		elif k == "name":
			mu.clean_values (v)
		elif k == "company_type":
			if v is None:
				this_dict[k] = "Not known"
			else:
				this_dict[k] = v
		elif k == "opencorporates_url":
			if v is None:
				this_dict[k] = "Not known"
			else:
				this_dict[k] = v
		elif k == "registry_url":
			if v is None:
				this_dict[k] = "Not known"
	if "registered_address" in this_dict:  # Case to cover I&PS organisations
		del this_dict["registered_address"]


#  Takes group_dict as input
def process_officers(group_dict):
	"""Called by main procedure, works through each officer dict in main extends basic officer dict then calls parse officer on each"""
	for each_organisation in group_dict["member_orgs"]:
		parsed_officers =[]
		found_orgs_officers = []
		org_oc_name = each_organisation["name"]
		org_oc_url = each_organisation["opencorporates_url"]
		for each_officer in each_organisation["officers"]:
			mu.extend_officer_dict(each_officer, org_oc_name, org_oc_url)
			p_officer = parse_officer(each_officer["officer"], each_organisation["found_orgs"])
			parsed_officers.append(p_officer)
		each_organisation["officers"] = parsed_officers
		# print (f"{group_dict['member_orgs']}\n")
		# if found_orgs_officers:
		# 	found_orgs_officers.append(org_oc_name, org_oc_url)
		# 	group_dict["found_orgs"].append(found_orgs_officers)
	return group_dict


def parse_officer(each_officer, found_orgs_officers):
	"""Takes list of officers from each organisation, parses officer address. """
	officer_address = each_officer
	address_mirror = officer_address.copy ()
	for k, v in officer_address.items():
		# Original single address field needs to be formatted individually by parse_officer_address
		# the newly created dict new_address then is combined with address_mirror
		# finally the original but now redundant single address field is deleted.
		if k == "name":
			address_mirror[k] = mu.clean_values (v)
		# Check if officer is an organisation entity, not an individual.
		# If true then triggers appropriate process.
			mu.check_officer_entity(address_mirror,v, found_orgs_officers)
		elif k == "position":
			if v:
				address_mirror[k] = v.capitalize ()
			else:
				address_mirror[k] = "Not Known"
		elif k == "uid":
			if not v:
				address_mirror[k] = "Not Known"
		elif k == "occupation":
			if v:
				address_mirror[k] = v.capitalize ()
			else:
				address_mirror[k] = "Not Known"
		elif k == "end_date":
			if v is None:
				address_mirror[k] = "None"
		elif k == "start_date":
			if v is None:
				address_mirror[k] = "None"
		elif k == "inactive":
			if v is None:
				address_mirror[k] = "None"
			else:
				address_mirror[k] = "False"
		elif k == "current_status":
			if v is None:
				address_mirror[k] = "Not Known"
		elif k == "nationality":
			if v:
				address_mirror[k] = v.capitalize ()
			else:
				address_mirror[k] = "Not Known"
		elif k == "date_of_birth":
			if v is None:
				address_mirror[k] = "Not Known"
		elif k == "entity_matches":
			if v is None:
				address_mirror[k] = v
		elif k == "address":
			new_address = (parse_officer_address(officer_address, v))
			address_mirror.update (new_address)
			try:
				del address_mirror["address"]
			except KeyError:
				print ("Key 'address' not found")
	return address_mirror, found_orgs_officers


def parse_officer_address(officer_address, incoming_address):
    """
    :param officer_address: Called by parsed_officers, officer_address is one component of officer record.
    :param address_to_parse: The original one line address field from the OC API data that needs parsing.
    :return: Parsed officer address dictionary
    """
    # TODO Frequent flyer addresses such as More London.
    address_dict = {"address_street": "Not known", "address_locality": "Not known", "address_region": "Not known",
                    "address_postal_code": "Not known", "address_country": "Not known"}
    # Remove \n value frequently occuring in original OC address field.
    n_stripped = re.sub("\n", ", ", incoming_address)
    address_to_parse = re.sub("C/O ", "", n_stripped)
    split_address = [ x.strip() for x in address_to_parse.split(',') ]
    for split_element in split_address:
        if mu.check_postal(split_element):
            address_dict[ "address_postal_code" ] = split_element
        elif mu.check_region(split_element):
            address_dict[ "address_region" ] = split_element
            if address_dict[ "address_region" ].title() in mu.uk_region_list:
                address_dict[ "address_country" ] = "UNITED KINGDOM"
        elif mu.check_country(split_element):
            address_dict[ "address_country" ] = split_element
        elif mu.check_city(split_element):
            address_dict[ "address_region" ] = split_element
        else:
            address_dict[ "address_street" ] = split_address[ 0 ]
            address_dict[ "address_locality" ] = split_address[ 1 ]
    if "LONDON" in split_address:
        address_dict[ "address_region" ] = "LONDON"
        address_dict[ "address_locality" ] = "Not known"
        address_dict[ "address_country" ] = "UNITED KINGDOM"
    elif address_dict[ "address_country" ] == "ENGLAND":
        address_dict[ "address_country" ] = "UNITED KINGDOM"
    return address_dict

    # check_officer_entity (officer_list)

# http://gappyfacets.com/2015/06/25/python-snippet-to-convert-csv-into-nodes-links-json-for-d3-js-network-data/
# Only for testing validity of simple graphs and nodes data

def process_extended(group_dict):
	"""Format found officer records that are organisations and UBO records.
	Ignore Controlling Entity for the moment as may be redundant. """
	# print(f"Discovered {group_dict['found_orgs']}")
	off_org_dict = {"off_org_name": "Not known", "off_org_number": "Not known", "node_type": "Not known",
					"parent_name": "Not known", "parent_url": "Not known"}
	for each_organisation in group_dict["member_orgs"]:
		off_org_dict["parent_name"] = each_organisation["name"]
		off_org_dict["parent_url"] = each_organisation["opencorporates_url"]
		for each_found_org in each_organisation["found_orgs"]:
			off_org_dict["off_org_name"], off_org_dict["off_org_number"], off_org_dict["node_type"] = each_found_org
			each_organisation["x_network"].append (off_org_dict)
		# TODO MAKE OUTPUT MORE READABLE
		# print (f'\nAfter update orgs as officers {each_organisation["x_network"]}')

# TODOD HERE
def process_ubo_records(group_dict):
    """Collect and process UBO (Ultimate Beneficial Owner) records for each organisation."""
    for each_org in group_dict[ "member_orgs" ]:
        ubo_dict = {"ubo_name": "Not known", "ubo_oc_placeholder":"Not known",
                    "node_type": "ubo", "parent_name": "name",
                    "parent_url": each_org["opencorporates_url"]}
        for key, value in each_org.items():
            if key == "ultimate_beneficial_owners":
                # print(f"each org ubo record {value}")
                if value == "None":  # No UBO record
                    pass
                elif value:  # Check for empty list
                    pass
                elif not value:
                    ubo_dict_list = [ ]
                    for each_ubo_dict in value:
                        for key, value in each_ubo_dict.items():
                            if key == ultimate_beneficial_owner["name"]:
                                ubo_dict["ubo_name"] = value
                            elif key == ultimate_beneficial_owner["opencorporates_url"]:
                                ubo_dict["ubo_oc_placeholder"] = value
                        ubo_dict_list.append(ubo_dict)
                    print(f"ubo_dict_list is {ubo_dict_list}")
                    each_org['x_ubo'].append(ubo_dict_list)
        print (f'After update org / x_ubo {each_org["x_ubo"]}')

  # "x_network":[],
  #               "x_ubo":[]}



# Found officer record that is organisation => ALNERY INCORPORATIONS NO.1 LIMITED
#
# [ {'ultimate_beneficial_owner': {'name': 'Mr Adrian Polisano',
#                                  'opencorporates_url': 'https://opencorporates.com/placeholders/103876985'}}
#
# 		print(f"UBO {x['ultimate_beneficial_owners']}\n")

def check_graph_validity():
	print ("Exporting graph structure...")
	G = nx.Graph ()

	# Read csv for nodes and edges using pandas:
	nodes = pd.read_csv ("output/simple_nodes.csv")
	edges = pd.read_csv ("output/simple_edges.csv")

	# Dataframe to list:
	nodes_list = nodes.values.tolist ()
	edges_list = edges.values.tolist ()

	# Import id, name, and group into node of Networkx
	for i in nodes_list:
		G.add_node (i[0], name=i[1], group=i[2])

	# Import source, target, and value into edges of Networkx
	for i in edges_list:
		G.add_edge (i[0], i[1], value=i[2])

	# Visualize the network:
	nx.draw_networkx (G)
	print ("Validating graph - hang on a moment...")
	plt.show (block=True)
	nx.write_graphml (G, "output/simple.graphml")

	print ("Graph validated.")

# READ FROM DF


def create_edges(edges_header):
    print("Creating edges...")
    with open("output/group_organisations.csv", "r") as orgs_in, open("output/group_officers.csv", "r") as officers_in, open("output/simple_edges.csv", "w") as edges_out:
        org_reader = csv.DictReader(orgs_in)
        officers_reader = csv.DictReader(officers_in)
        edges_writer = csv.writer(edges_out)
        edges_writer.writerow(edges_header)

        # weight = 1

        # Default parent_id for organisations is 0. Skip these edges.
        for row in org_reader:
            new_edge = [ ]
            this_edge = row[ "node_id" ], row[ "parent_node" ], row[ "node_type" ]
            node_id, parent_id, this_type = this_edge  # Unpack tuple values required.
            new_edge = [ int(node_id), int(parent_id), str(this_type) ]
            #                             print(f"Edge for {this_edge}")

            if new_edge[ 1 ] > 0:
                edges_writer.writerow(this_edge)
            else:
                pass

        for row in officers_reader:
            this_edge = row[ "node_id" ], row[ "parent_node" ], row[ "node_type" ]
            node_id, parent_id, this_type = this_edge  # Unpack tuple values required.
            # new_edge = [node_id, parent_id, weight, this_type]
            edges_writer.writerow(this_edge)


def create_nodes():
    print("Creating nodes...")
    # unique_id = 0
    nodes_header = [ "id", "name", "node_type" ]
    edges_header = [ "source", "target", "node_type" ]

    with open("output/group_organisations.csv", "r") as orgs_in, open("output/group_officers.csv", "r") as officers_in, open("output/simple_nodes.csv", "w") as nodes_out:
        org_reader = csv.DictReader(orgs_in)
        officers_reader = csv.DictReader(officers_in)
        nodes_reader = csv.reader(nodes_out)
        nodes_writer = csv.writer(nodes_out)

        oc_group_files = [ "output/group_organisations.csv", "output/group_officers.csv" ]

        for target_file in oc_group_files:
            if target_file == "output/group_organisations.csv":
                nodes_writer.writerow(nodes_header)
                for row in org_reader:
                    this_node = row[ "node_id" ], row[ "name" ], row[ "node_type" ]
                    node_id, this_name, this_type = this_node  # Unpack tuple values required.
                    #                     print(f"Adding new node: {this_node}")
                    nodes_writer.writerow(this_node)
            elif target_file == "output/group_officers.csv":
                for row in officers_reader:
                    this_node = row[ "node_id" ], row[ "name" ], row[ "node_type" ]
                    #                     print(this_node)
                    node_id, this_name, this_type = this_node  # Unpack tuple values required.
                    #                     print(f"Adding new node: {this_node}")
                    nodes_writer.writerow(this_node)
    create_edges(edges_header)


def create_cluster_dict(grouped_list):
	deduped_dict = {"clusters": []}
	cluster_count = 0
	for cluster in grouped_list:
		cluster_dict = {"cluster_id": cluster_count, "cluster_names": [], "officer_ids": []}
		for officer_record in cluster:
			cluster_dict["cluster_names"].append(officer_record[0])
			cluster_dict["officer_ids"].append(officer_record[1])
		cluster_count += 1
		deduped_dict["clusters"].append(cluster_dict)
	print("|-----------------------------|\nOfficer names clustered\n|-----------------------------|")
	for cluster in deduped_dict["clusters"]:
		# Only print results where there are duplicates.
		if len(cluster["cluster_names"]) > 0:
			for key, value in cluster.items():
				if key == "cluster_id":
					c_id = value
				elif key == "cluster_names":
					c_names = value
				elif key == "officer_ids":
					c_ids = value
			print(f"{c_id} {c_names} {c_ids}")
		else:
			pass
	print("|-----------------------------|")

	return deduped_dict



# TODO NEED IDS!!!!!!!!!

def create_officer_list(group_dict):
	officers_list = []
	for each_org in group_dict["member_orgs"]:
		for each_officer in each_org["officers"]:
			officers_list.append((each_officer["name"],each_officer["node_id"]))
	return officers_list

def dedupe_officers(officers_list, fuzzy_threshold):
	"""Takes list of all group orgs officers from create_officer_list and resolves duplicates based on name.
	Passes deduped and grouped list through to create_cluster_dict to create node clusters for similar names.
	token_set_ratio with fuzzy_threshold=85."""
	print ("|-----------------------------|\nDeduping officers...\n|-----------------------------|")
	grouped_list = []  # groups of names with distance > 80
	for officer_record in officers_list:
		for officer_group in grouped_list:
			if all(fuzz.token_set_ratio(officer_record[0], weight) > fuzzy_threshold for weight in officer_group):
				officer_group.append(officer_record)
				break
		else:
			grouped_list.append([officer_record, ])
	return grouped_list

# TODO Need for another pass through results to solve the Deboarah Josephine UPTON problem.


def officers_to_csv(group_dict):
	"""Lorem ipsum."""
	print ("Writing Officers to CSV...\n")
	officer_header = ['oc_group', 'id', 'name', 'position', 'uid', 'start_date', 'end_date', 'opencorporates_url',
					  'occupation',
					  'inactive', 'current_status', 'nationality', 'date_of_birth', 'address_street',
					  'address_locality',
					  'address_postal_code', 'address_region', 'address_country', 'org_name', 'org_oc_url', 'node_id',
					  'parent_node', 'alias_label', 'alias_nodes', 'node_type']
	officer_file = open ("output/group_officers.csv", "w")
	with officer_file:
		writer = csv.DictWriter (officer_file, fieldnames=officer_header)
		writer.writeheader ()
		for each_org in group_dict["member_orgs"]:
			for each_officer in each_org["officers"]:
				oc_group = {"oc_group": GROUP}
				each_officer.update (oc_group)
				writer.writerow (each_officer)
		print ("Officers written.")


def number_group(group_dict):
	counter = 0
	for organisation in group_dict["member_orgs"]:
		organisation["node_id"] = counter
		counter += 1
		for each_officer in organisation["officers"]:
			counter += 1
			each_officer["node_id"] = counter
			each_officer["parent_node"] = organisation["node_id"]
	return group_dict


def orgs_to_csv(group_dict):
    # print (f"group_dict {group_dict['member_orgs']}")
    print("Writing Organisations to CSV...")
    org_header = [ 'oc_group', 'controlling_entity', 'incorporation_date', 'inactive', 'current_status', 'registry_url',
                   'jurisdiction_code', 'name', 'company_number', 'branch_status', 'company_type', 'opencorporates_url',
                   'ultimate_beneficial_owners', 'address_street', 'address_locality', 'address_postal_code',
                   'address_region', 'address_country', 'node_id', 'parent_node', 'node_type', "network"]
    organisation_file = open('output/group_organisations.csv', 'w')
    with organisation_file:
        writer = csv.DictWriter(organisation_file, fieldnames=org_header, extrasaction='ignore')
        writer.writeheader()
        for each_org in group_dict[ "member_orgs" ]:
            oc_group = {"oc_group": GROUP}
            each_org.update(oc_group)
            writer.writerow(each_org)
    print("Organisations written.")


def network_to_csv(group_dict):
    # print(group_dict)
    print("Writing Extended Network to CSV...")
    network_header = [ "root_name", "root_company_number", "parent_name", "parent_opencorporates_url", "parent_type",
                       "child_name", "child_opencorporates_url", "child_type", "relationship_type",
                       "relationship_properties" ]
    network_file = open('output/extended_networks.csv', 'w')
    with network_file:
        writer = csv.DictWriter(network_file, fieldnames = org_header, extrasaction='ignore')
        writer.writeheader()
        for each_network in group_dict["extended_network"]:
            writer.writerow(each_network)

def main(target_url):
	payload = {'api_token': MY_OC_KEY}
	# ** Check OC is alive and account ok.
	try:
		r = requests.get ("https://api.opencorporates.com/v0.4/account_status?api_token=" + MY_OC_KEY)
		r.status_code == requests.codes.ok
	except requests.exceptions.HTTPError as err:
		print (f"Computer says no because of {err}")
		sys.exit (1)
	# ** Process request
	r = requests.get (target_url, params=payload)
	# ** Visual check of URL parameters
	print(f"{r.url}\n")
	data_json = requests.get (target_url, params=payload).json ()
	print (f"data_json {data_json}")


def main(GROUP):
	"""Check if we can access the server using call to OC personal account status. If not exit and make some tea.
	If we can then retrieve the manually curated OC group """
	# start = time.time()
	try:
		r = requests.get("https://api.opencorporates.com/v0.4/account_status?api_token=" + MY_OC_KEY)
		r.status_code == requests.codes.ok
	except requests.exceptions.HTTPError as err:
		print(f"Computer says no because of {err}")
		sys.exit(1)
	print(f"\nMain function running, getting details for OC group: {GROUP}\n|-----------------------------|")
	r = requests.get(GROUP_STUB + GROUP + "?api_token=" + MY_OC_KEY)
	retrieved_group = json.loads(r.content.decode('utf-8'))
	# Main procedure calls
	orgs_list = retrieve_group_urls(retrieved_group)
	group_dict = process_group(orgs_list)
	process_officers(group_dict)
	process_extended(group_dict)
	process_ubo_records(group_dict)

	# number_group (group_dict)
	# officers_list = create_officer_list (group_dict)
	# grouped_list = dedupe_officers (officers_list, fuzzy_threshold=85)
	# create_cluster_dict (grouped_list)
	# orgs_to_csv(group_dict)
	# officers_to_csv(group_dict)
	# create_nodes()
	# check_graph_validity ()


# end = time.time()
# print(end - start)
# print(f"\rElapsed time {round(end-start, 3)}\n", end="")
	print("Process complete")


main(GROUP)

# Execute main() function
# if __name__ == '__main__':
# 	main (GROUP)



