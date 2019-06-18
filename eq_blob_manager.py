import os
from io import StringIO
import pandas as pd
from azure.storage.blob import BlockBlobService

account_name = 'samsmdpblobdev04'
account_key = 'Mc8egs6gA47M4NA6k40hma8W1eps+bHRDv14fdsvx1loJuk9WLHSqmfRUyRNKmPGaaBw3FYvmyi9iSN80+gocw=='
container_name = 'raw'
block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)


def create_blob_from_path(blob_name, file_path):
   block_blob_service.create_blob_from_path(container_name, blob_name, file_path)


def delete_blob(blob_name):
   block_blob_service.delete_blob(container_name, blob_name)


def get_blob_list():
   blobs = []
   generator = block_blob_service.list_blobs(container_name)

   for blob in generator:
       blobs.append('Blob Name: {}'.format(blob.name))

   return blobs

def get_blob_url(blob_name):
   return block_blob_service.make_blob_url(container_name, blob_name)


def create_container():
   block_blob_service.create_container(container_name)


def delete_container():
   block_blob_service.delete_container(container_name)


def get_container_list():
   containers = []
   generator = block_blob_service.list_containers()

   for container in generator:
       containers.append('Container Name: {}'.format(container.name))

   return containers

def create_blob_from_csv_file(blob_name,file):
    if '.csv' in blob_name:
        pass
    else:
        blob_name += '.csv'

    block_blob_service.create_blob_from_text(container_name, blob_name, file)

def open_csv_blob(blob_name):
    blobstring = block_blob_service.get_blob_to_text(container_name,blob_name).content
    df = pd.read_csv(StringIO(blobstring))
    return df