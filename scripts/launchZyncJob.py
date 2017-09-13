#!/usr/bin/python
#
# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''Python script to deploy ZYNC render job for [geo].'''

import sys
from pprint import pprint as pp

def launch( geoData,
            zyncPath,
            instance_type='(PREEMPTIBLE) zync-16vcpu-32gb',
            frameRange='1-5400',
            verbose=0 ):
    '''Perform tasks necessary to deploy ZYNC render job. Consists mainly of
    building 'params' dict and submitting to the ZYNC object 'z'.'''

    # Append Zync Python API to path.
    sys.path.append(zyncPath)

    # Import Zync Python API.
    import zync

    # Connect to ZYNC. This will start the browser to perform an 
    # Oauth2 authorization if needed.
    z = zync.Zync()

    # Path to the Maya scene.
    scene_path = geoData['scene_path']
    objectName = geoData.keys()[0]

    # Define job params.
    params = {
        'num_instances': 10,
        'priority': 50, 
        'job_subtype': 'render', 
        'upload_only': 0, 
        'proj_name': 'cad-iot-ml', 
        'skip_check': 0, 
        'instance_type': instance_type,
        'frange': frameRange,
        'step': 1, 
        'chunk_size': 5, 
        'renderer': 'arnold', 
        'layers': 'defaultRenderLayer',
        'out_path': geoData['image_dir'],
        'camera': 'CAM:render_cam', 
        'xres': 1920, 
        'yres': 1080, 
        'project': geoData['base_path'],
        'ignore_plugin_errors': 0,
        'distributed': 0, 
        'use_vrscene': 0,
        'use_ass': 0, 
        'use_mi': 0, 
        'vray_nightly': 0, 
        'scene_info': {
            'files': [], 
            'arnold_version': '1.4.2.0', 
            'plugins': ['mtoa'],
            'file_prefix': ['', {
                'cad-iot-ml': ''
            }], 
            'padding': 4, 
            'vray_version': '2.40.01', 
            'references': [ geoData['camera_rig'],
                            geoData['light_rig'],
                            geoData['geo_path'],
            ],
            'unresolved_references': [], 
            'render_layers': [
                'defaultRenderLayer'
            ], 
            'render_passes': {}, 
            'extension': 'exr', 
            'version': '2017'
        },
        'plugin_version': 'custom'
    }

    # Print assembled params, if requested.
    if verbose == 5:
        pp(params)
    # end if

    # Launch the job. submit_job() returns the ID of the new job.
    try:
        jobId = z.submit_job( 'maya', scene_path, params=params )
        return jobId
    except zync.ZyncError, exc:
        raise Exception(exc)
    # end try

# end def
