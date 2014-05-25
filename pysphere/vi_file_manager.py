#--
# Copyright (c) 2014, Sebastian Tello
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#   * Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
#   * Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
#   * Neither the name of copyright holders nor the names of its contributors
#     may be used to endorse or promote products derived from this software
#     without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
#--

import urllib, urllib2
import sys

from pysphere.resources import VimService_services as VI
from pysphere import VIProperty, VIMor, MORTypes
from pysphere.vi_task import VITask
from pysphere.resources.vi_exception import VIException, VIApiException, \
                                            FaultTypes
from pysphere.vi_snapshot import VISnapshot
from pysphere.vi_managed_entity import VIManagedEntity


class VIFileManager:

    def __init__(self, server, mor):
        self._server = server
        self._mor = mor
        self._properties = VIProperty(server, mor)
        self.relogin()

    def relogin(self):
        self._handler = self._build_auth_handler()

    def list_files(self, datastore, path, case_insensitive=True,
                   folders_first=True, match_patterns=[]):
        """Return a list of files inside folder @path on @datastore
        """

        ds = [k for k,v in self._server.get_datastores().items() if v == datastore][0]
        browser_mor = VIProperty(self._server, ds).browser._obj

        request = VI.SearchDatastore_TaskRequestMsg()
        _this = request.new__this(browser_mor)
        _this.set_attribute_type(browser_mor.get_attribute_type())
        request.set_element__this(_this)
        request.set_element_datastorePath("[%s] %s" % (datastore, path))

        search_spec = request.new_searchSpec()

        query = [VI.ns0.FloppyImageFileQuery_Def('floppy').pyclass(),
                 VI.ns0.FileQuery_Def('file').pyclass(),
                 VI.ns0.FolderFileQuery_Def('folder').pyclass(),
                 VI.ns0.IsoImageFileQuery_Def('iso').pyclass(),
                 VI.ns0.VmConfigFileQuery_Def('vm').pyclass(),
                 VI.ns0.TemplateConfigFileQuery_Def('template').pyclass(),
                 VI.ns0.VmDiskFileQuery_Def('vm_disk').pyclass(),
                 VI.ns0.VmLogFileQuery_Def('vm_log').pyclass(),
                 VI.ns0.VmNvramFileQuery_Def('vm_ram').pyclass(),
                 VI.ns0.VmSnapshotFileQuery_Def('vm_snapshot').pyclass()]
        search_spec.set_element_query(query)
        details = search_spec.new_details()
        details.set_element_fileOwner(True)
        details.set_element_fileSize(True)
        details.set_element_fileType(True)
        details.set_element_modification(True)
        search_spec.set_element_details(details)
        search_spec.set_element_searchCaseInsensitive(case_insensitive)
        search_spec.set_element_sortFoldersFirst(folders_first)
        search_spec.set_element_matchPattern(match_patterns)
        request.set_element_searchSpec(search_spec)
        response = self._server._proxy.SearchDatastore_Task(request)._returnval
        task = VITask(response, self._server)
        if task.wait_for_state([task.STATE_ERROR, task.STATE_SUCCESS]) == task.STATE_ERROR:
            raise Exception(task.get_error_message())

        info = task.get_result()
        # return info

        if not hasattr(info, "file"):
            return []
        # for fi in info.file:
        #     fi._get_all()
        return [{'type':fi._type,
                 'path':fi.path,
                 'size':fi.fileSize,
                 'modified':fi.modification,
                 'owner':fi.owner
                } for fi in info.file]

    def make_directory(self, datastore, path, create_parent=False):
        """Creates new directory on @datastore with given @path
        """
        try:
            request = VI.MakeDirectoryRequestMsg()
            _this = request.new__this(self._fileManager)
            _this.set_attribute_type(self._fileManager.get_attribute_type())
            request.set_element__this(_this)
            request.set_element_name("[%s] %s" % (datastore, path))
            request.set_element_createParentDirectories(create_parent)
            self._server._proxy.MakeDirectory(request)
        except (VI.ZSI.FaultException), e:
            raise VIApiException(e)

    def upload(self, datastore, local_file_path, remote_file_path):
        """Uploads @local_file_path to @remote_file_path on @datastore
        replacing existing file. Returns True if @remote_file_path was replaced
        and False otherwise.
        """
        fd = open(local_file_path, "r")
        data = fd.read()
        fd.close()
        resource = "/folder/%s" % remote_file_path.lstrip("/")
        url = self._get_url(datastore, resource)
        resp = self._do_request(url, data)
        return resp.code == 200

    def download(self, datastore, remote_file_path, local_file_path):
        """Downloads @remote_file_path from @datastore to @local_file_path
        replacing existing file.
        """
        resource = "/folder/%s" % remote_file_path.lstrip("/")
        url = self._get_url(datastore, resource)

        if sys.version_info >= (2, 6):
            resp = self._do_request(url)
            CHUNK = 16 * 1024
            fd = open(local_file_path, "wb")
            while True:
                chunk = resp.read(CHUNK)
                if not chunk: break
                fd.write(chunk)
            fd.close()
        else:
            urllib.urlretrieve(url, local_file_path)

    def _do_request(self, url, data=None):
        opener = urllib2.build_opener(self._handler)
        request = urllib2.Request(url, data=data)
        if data:
            request.get_method = lambda: 'PUT'
        return opener.open(request)

    def _get_url(self, datastore, resource, datacenter=None):
        if not resource.startswith("/"):
            resource = "/" + resource

        params = {"dsName":datastore}
        if datacenter:
            params["dcPath": datacenter]
        params = urllib.urlencode(params)

        return "%s%s?%s" % (self._get_service_url(), resource, params)

    def _get_service_url(self):
        service_url = self._server._proxy.binding.url
        return service_url[:service_url.rindex("/sdk")]

    def _build_auth_handler(self):
        service_url = self._get_service_url()
        user = self._server._VIServer__user
        password = self._server._VIServer__password
        auth_manager = urllib2.HTTPPasswordMgrWithDefaultRealm()
        auth_manager.add_password(None, service_url, user, password)
        return urllib2.HTTPBasicAuthHandler(auth_manager)
