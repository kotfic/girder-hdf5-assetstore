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


class Hdf5SupportAdapter(FilesystemAssetstoreAdapter):
    def _importHdf5(self, path, folder, progress, user):
        if not os.path.isabs(path):
            path = os.path.join(self.assetstore['root'], path)

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