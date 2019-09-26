#!/usr/bin/env python3

import sys
import argparse
import os
import requests
import json
import pandas
import boto3
import uuid

from collections import namedtuple

# to execute
# run >>  python3 gha.py eosio eos pat
# python3 scriptname.py <name of repo owner> <name of repo> <pat = placeholder for file containing personal access token - currenlty hardcoded> 

# script will pull data from github, store it locally in two files, .json and partially flattened .csv
# will then load the .json file onto AWS s3 

###########################################
# Helpers / defintions
def getURL():
	return "https://api.github.com/"

# personal access token - see github for details
def getPAT():
	return #put your pat here - or get from a file

# github user
def getUser():
	return #put you git username here - or get from a file

def getJsonExt():
	return ".json"

def getCsvExt():
	return ".csv"

def gets3BucketName():
	return #put your aws bucket name here

# 'path' to store the 'file' in bucket -- really an object see s3 docs 
def gets3BucketPath():
	return #put the 'path' you want to use to store the file here


###########################################
# Local files

def writeJson(data, filename):
	fname =  f"{filename}{getJsonExt()}"
	fileHandle = open(fname, "w+") 	
	fileHandle.write(data.text)

def writeCSV(data, filename):
	df = pandas.read_json(data.text)
	csvd = df.to_csv()
	fname2 = f"{filename}{getCsvExt()}"
	fileHandle2 = open(fname2, "w+") 	
	fileHandle2.write(csvd)


# write file in both json and slightly separated out .csv files
def writeFiles(data, filename):
	writeJson(data, filename)
	writeCSV(data, filename)


###########################################
# AWS S3

# upload file to s3 bucket ... note exepcts the .aws directory off ~/ to be set up containing config and credentials 
def uploadToS3(bucketname, s3path, filename):
	fname =  f"{filename}{getJsonExt()}"

	# randomise start of name to assist partitioning on s3 - please read aws s3 docs - don't think this is complete
	random_file_name = ''.join([str(uuid.uuid4().hex[:6]), fname])

	s3_resource = boto3.resource('s3')
	my_bucket = s3_resource.Bucket(name=bucketname)
	my_bucket.upload_file(Filename=fname, Key=f"{s3path}{random_file_name}")

	print(f"loaded {fname} to bucket {s3path} as {random_file_name}.")


###########################################
# Github

# Retrieves info from GitHub as a .json object, data which does not require push access.
def getOpenData(url):
	print("\n")
	print("***********************************")
	print("Get: ", url)

	response = requests.get(url)
	if response.status_code != 200:
		print("Request failed with:  ", response.status_code)

	print("Return Status: ", response.status_code)

	return response


# Retrieves info from GitHub as a .json object, data which requires push access.
def getSecureData(url, user, pat):
	print("\n")
	print("***********************************")
	print("Secure Get: ", url)

	response = requests.get(url, auth=(user, pat))

	if response.status_code != 200:
		print("Request failed with:  ", response.status_code)

	print("Return Status: ", response.status_code)

	return response


def getFromRepoOwnerRepo(url, owner, repo, name, user=None, pat=None):
	fullurl=f"{url}repos/{owner}/{repo}/{name}"
	if user == None and pat == None:
		return getOpenData(fullurl)
	else:
		return getSecureData(fullurl, user, pat)

###########################################
# Get Repository forks using getOpenData
def getForks(url, owner, repo):
	response = getFromRepoOwnerRepo(url, owner, repo, "forks")
	writeFiles(response, "forks")
	uploadToS3(gets3BucketName(), gets3BucketPath(), "forks")
		

#############################################
# Get Traffic using getSecureData
def getTrafficPopularReferrers(url, owner, repo, user, pat):
	response = getFromRepoOwnerRepo(url, owner, repo, "traffic/popular/referrers", user, pat)
	writeFiles(response, "trafficPopularReferrers")
	uploadToS3(gets3BucketName(), gets3BucketPath(), "trafficPopularReferrers")

###########################################
# Get Organizations - url is different so call things direct
def getOrganizationsOutsideCollaborators(url, owner, repo, user, pat):
	fullurl=f"{url}orgs/{owner}/outside_collaborators" 	
	response = getSecureData(fullurl, user, pat, "outsideCollaborators")
	writeFiles(response, "outsideCollaborators")
	uploadToS3(gets3BucketName(), gets3BucketPath(), "outsideCollaborators")


###########################################
# Main
def main():
	print("python version: ", sys.version)

	# set up the command line positionals/options
	parser = argparse.ArgumentParser()

	#positionals
	parser.add_argument("owner", help="The owner of the repo")
	parser.add_argument("repo", help="The repo on which to collect data")	
	parser.add_argument("filename", help="The directory and file containing a personal access token")

### drilling into forks and clones not yet imnplemented
	parser.add_argument("-f", help="Recurse through forks")
	parser.add_argument("-c", help="Recurse through clones")

	args = parser.parse_args()

	messageFlags = namedtuple('messageFlags', ['forks', 'clones'])
	messageParams = messageFlags(forks = args.f, clones = args.c)

	if messageParams.forks:
		print("Will look for data on all clones")

	if messageParams.clones:
		print("Will look for data on all forks")
### drilling into forks and clones not yet imnplemented


	try:

		print("gha version: 0.1")
		print("\n")
		print("Repo Owner: ", args.owner)
		print("Repo: ", args.repo)


		# set up parameters ... see helpers / defintion functions about
		url = getURL()
		pat2 = getPAT()
		user = getUser()

		# call to the functions to get the data
		getForks(url, args.owner, args.repo)
		getTrafficPopularReferrers(url, args.owner, args.repo, user, pat2)


	except Error as e:
			print("Main", e)
			print(sys.exc_type)	
	except NameError as e:
			print("Main", e)
			print(sys.exc_type)	
	except:
		print("Unexpected error:", sys.exc_info()[0])
	finally:
		print("finally")



if __name__=="__main__":
	main()



