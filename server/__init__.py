from functools import partial
import h5py
import os

from girder.api import access
from girder.api.describe import autoDescribeRoute, Description
from girder.api.rest import boundHandler, setResponseHeader
from girder.constants import AccessType, AssetstoreType, TokenScope
from girder.exceptions import AccessException, RestException
from girder.models.assetstore import Assetstore
from girder.models.file import File
from girder.models.folder import Folder
from girder.models.item import Item
from girder.utility.assetstore_utilities import getAssetstoreAdapter, setAssetstoreAdapter
from girder.utility.filesystem_assetstore_adapter import FilesystemAssetstoreAdapter, BUF_SIZE
from girder.utility.progress import ProgressContext


def get_corresponding_hdf5_obj(obj, token):
    while os.path.basename(obj.name) != token:
        obj = obj.parent
    return obj

def resolve_group(root_folder, obj, user):
    tokens = [i for i in obj.name.split('/') if i]
    parent = root_folder
    for token in tokens:
        parent = Folder().createFolder(parent, token, creator=user, reuseExisting=True)

def resolve_dataset(root_folder, obj, user, assetstore):
    directory, name = os.path.split(obj.name)
    tokens = [i for i in directory.split('/') if i]
    parent = root_folder
    for token in tokens:
        hdf5_obj = get_corresponding_hdf5_obj(obj, token)
        parent = Folder().createFolder(parent, token, creator=user, reuseExisting=True)
        parent['hdf5Metadata'] = str(hdf5_obj.attrs.items())
        Folder().save(parent)
    item = Item().createItem(name=name, creator=user, folder=parent, reuseExisting=True)
    item['hdf5Metadata'] = str(obj.attrs.items())
    Item().save(item)
    File().createFile(name=name, creator=user, item=item, reuseExisting=True,
                      assetstore=assetstore, saveFile=False, size=obj.size)

def mirror_objects_in_girder(folder, progress, user, assetstore, name, obj):
    progress.update(message=name)
    if isinstance(obj, h5py.Dataset):
        resolve_dataset(folder, obj, user, assetstore)
    elif isinstance(obj, h5py.Group):
        resolve_group(folder, obj, user)

class Hdf5SupportAdapter(FilesystemAssetstoreAdapter):
    def _importHdf5(self, path, folder, progress, user):
        if not os.path.isabs(path):
            path = os.path.join(self.assetstore['root'], path)

        try:
            hdf = h5py.File(path, 'r')
            hdf.visititems(partial(mirror_objects_in_girder, folder, progress, user, self.assetstore))
        except IOError:
            raise RestException('{} is not an hdf5 file'.format(path))


@boundHandler
@access.admin(scope=TokenScope.DATA_WRITE)
@autoDescribeRoute(
    Description('Import an hdf5 file into the system.')
    .notes('This does not move or copy the existing data, it just creates '
           'references to it in the Girder data hierarchy. Deleting '
           'those references will not delete the underlying data.')
    .modelParam('id', model=Assetstore)
    .modelParam('folderId', 'Import destination folder.', model=Folder, level=AccessType.WRITE,
                paramType='formData')
    .param('path', 'Path of the hdf file to import.')
    .param('progress', 'Whether to record progress on the import.',
           dataType='boolean', default=False, required=False)
    .errorResponse()
    .errorResponse('You are not an administrator.', 403))
def _importHdf5(self, assetstore, folder, path, progress):
    user = self.getCurrentUser()
    adapter = getAssetstoreAdapter(assetstore)
    with ProgressContext(progress, user=user, title='Importing data') as ctx:
        adapter._importHdf5(path, folder, ctx, user)

def load(info):
    setAssetstoreAdapter(AssetstoreType.FILESYSTEM, Hdf5SupportAdapter)
    info['apiRoot'].assetstore.route('POST', (':id', 'hdf5_import'), _importHdf5)
