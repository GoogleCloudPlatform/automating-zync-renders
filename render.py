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

'''
This file provides a basic example to assemble and launch renders for your
automating-zync-renders project.
'''

# Import standard Python modules.
import argparse
import logging
import os
import re
import sys

# Import Google APIs.
from google.cloud import storage

# Import custom modules.
import scripts.utils as utils
import scripts.launchZyncJob as lzj
import scripts.colorLogs

# Set up logger.
logging.basicConfig()
LOGGER = logging.getLogger('renderObjects')
LOG_LEVELS = {
    '5': logging.DEBUG,
    '4': logging.INFO,
    '3': logging.WARN,
    '2': logging.ERROR,
    '1': logging.CRITICAL
}


class RenderObjects(object):
    '''All methods necessary for rendering via ZYNC.'''

    def __init__(self):
        self.config_file = CMD_ARGS.config
        self.config_data = utils.read(self.config_file)
        self.path_objects = [
            'base_path',
            'auth_file',
            'zync_lib_path',
            'bucket_region',
            'scene_template',
            'camera_rig',
            'light_rig',
            'scene_dir',
            'parts_dir']
        self.gcp_project = self.config_data['environment']['gcp_project']

        LOGGER.setLevel(LOG_LEVELS[CMD_ARGS.verbose])

        # This dict contains all relevant data to feed to Zync for each object.
        self.render_objects = {}

    # end def


    def validate(self):
        '''Validate each item in config file.'''

        for key, value in self.config_data.iteritems():
            if key in self.path_objects:
                if os.path.exists(value):
                    LOGGER.info('Found: %s=%s' % (key, value))
                else:
                    LOGGER.error('NOT FOUND: %s=%s' % (key, value))
                # end if
            # end if

        # Authenticate before creating buckets.
        LOGGER.info('Setting envar $GOOGLE_APPLICATION_CREDENTIALS to %s.' % \
        self.config_data['auth_file'])
        os.putenv('GOOGLE_APPLICATION_CREDENTIALS', self.config_data['auth_file'])

        # Create bucket, if not already created.
        bucket_name = '%s-renders' % self.gcp_project
        bucket_name_full = 'gs://%s' % bucket_name
        if not CMD_ARGS.noop:
            LOGGER.info('Creating bucket %s.' % bucket_name_full)
            bucket = self.create_bucket(bucket_name)
        else:
            LOGGER.info('Bucket %s already exists. Skipping.' % bucket_name_full)
        # end if

        # Look for objects, create dict render_objects to iterate over.
        for geo in self.config_data['parts']:

            # Build object path.
            parts_path = os.path.join(
                self.config_data['base_path'],
                self.config_data['parts_dir'],
                geo)

            self.render_objects.update({geo: {}})

            # Ensure part exists before proceeding.
            if os.path.exists(parts_path):

                LOGGER.info('Found part: %s' % parts_path)

                if not CMD_ARGS.noop:
                    self.render_objects[geo].update({'bucket_name': bucket.name})
                # end if
            # end if
        # end for

        LOGGER.info('Found %s parts.' % len(self.render_objects.keys()))

    # end def validate


    def deploy_renders(self):
        ''' Deploy Zync render.'''

        # TODO: Determine if we hard-code framerange.
        for geo, geo_data in self.render_objects.iteritems():

            LOGGER.info('Operating on %s.' % geo)
            LOGGER.info('Deploying ZYNC job for %s.' % geo_data['geo_base'])
            if not CMD_ARGS.noop:
                try:
                    job_id = lzj.launch(
                        geo_data,
                        zyncPath=self.config_data['zync_lib_path'],
                        instance_type='(PREEMPTIBLE) zync-16vcpu-32gb',
                        frameRange='1-10',
                        verbose=CMD_ARGS.verbose)
                    LOGGER.info('Submitted ZYNC job ID: %s' % job_id.id)
                except Exception as e:
                    LOGGER.error('Unable to submit ZYNC job.')
                    raise Exception, e, sys.exc_info()[2]
                # end try
            # end if
        # end for
    # end def deploy_renders


    def build_scenes(self):
        '''Read scene template and perform substitution.'''

        # Iterate over each part found, assemble scene,
        # and submit for rendering.
        for geo in self.render_objects.keys():

            # Extract object number.
            part_id = os.path.splitext(os.path.split(geo)[-1])[0][-2:].zfill(3)
            LOGGER.info('Preparing %s (part ID: %s)' % (geo, part_id))

            # Destination file path for scene file.
            scene_path = os.path.join(
                self.config_data['base_path'],
                self.config_data['scene_dir'],
                'part_%s_render.ma' % part_id)

            # Define scene template path.
            scene_template = os.path.join(
                self.config_data['base_path'],
                self.config_data['scene_template'])

            # Ingest template, modify.
            with open(scene_template, 'r') as template:
                content = template.read()

                # Sub camera rig.
                camera_rig = os.path.join(
                    self.config_data['base_path'],
                    self.config_data['camera_rig'])
                content_new = re.sub(
                    '<<CAM_RIG>>',
                    camera_rig,
                    content,
                    flags=re.M)
                self.render_objects[geo].update({'camera_rig': camera_rig})

                # Sub light rig.
                light_rig = os.path.join(
                    self.config_data['base_path'],
                    self.config_data['light_rig'])
                self.render_objects[geo].update({'light_rig': light_rig})
                content_new = re.sub(
                    '<<LIGHT_RIG>>',
                    light_rig,
                    content_new,
                    flags=re.M)

                # Sub part filename.
                geo_path = os.path.join(
                    self.config_data['base_path'],
                    self.config_data['parts_dir'],
                    geo)
                content_new = re.sub(
                    '<<OBJECT>>',
                    geo_path,
                    content_new,
                    flags=re.M)
                self.render_objects[geo].update({'geo_path': geo_path})

                # Sub part ID.
                content_new = re.sub(
                    '<<OBJECT_NUM>>',
                    part_id,
                    content_new,
                    flags=re.M)
                self.render_objects[geo].update({'part_id': part_id})

                output_file = open(scene_path, 'w')
                output_file.write(content_new)
                LOGGER.info('Wrote %s.' % scene_path)

            # end with

            # Update object_data.
            geo_base = os.path.splitext(geo)[0]
            self.render_objects[geo].update({'geo_base': geo_base})
            self.render_objects[geo].update({'scene_path': scene_path})
            self.render_objects[geo].update({'base_path': self.config_data['base_path']})
            self.render_objects[geo].update({'image_dir': os.path.join(
                self.config_data['base_path'],
                self.config_data['image_dir'],
                geo_base)})
        # end for
    # end def build_scenes


    def create_bucket(self, bucket_name):
        '''Create storage bucket to hold renders. Note that neither region nor
        storage class is specified. If you want to override the defaults, see
        https://github.com/GoogleCloudPlatform/google-cloud-python/blob/master/storage/google/cloud/storage/bucket.py'''

        # Specify bucket, forcing project.
        gcs_client = storage.Client(project=self.gcp_project)
        bucket = storage.bucket.Bucket(
            gcs_client,
            bucket_name)

        # Ensure bucket does not already exist.
        if bucket.exists():
            LOGGER.warning('Bucket gs://%s already exists. Skipping.' % bucket.name)
            return bucket
        else:
            # Create bucket.
            if not CMD_ARGS.noop:
                bucket.create()
                LOGGER.info('Created bucket gs://%s.' % bucket.name)
                return bucket
            # end if
        # end if

    # end def create_bucket


    ####
    # Main method.
    ####
    def run(self):
        '''Top-level method.'''

        LOGGER.info('Config file data:')
        LOGGER.info(self.config_data)

        # Ensure all components are present.
        self.validate()

        # Assemble scenes.
        self.build_scenes()

        # Deploy render.
        self.deploy_renders()

        # Print object dict.
        LOGGER.debug(self.render_objects)

    # end def run

# end class RenderObjects


if __name__ == '__main__':

    CMD_USAGE = 'Script for launching renders for your automating-zync-renders project.'
    ARG_PARSER = argparse.ArgumentParser(description=CMD_USAGE)

    # Add config arg.
    ARG_PARSER.add_argument(
        '--config', '-c',
        required=True,
        help='Config file in YAML format.')

    # Add noop arg.
    ARG_PARSER.add_argument(
        '--noop', '-n', action='store_true',
        help='Run script in dry-run mode.')

    # Add verbose arg.
    ARG_PARSER.add_argument(
        '--verbose', '-v',
        default='4',
        help='Print verbose debug messages at specfied level (1=critical, 5=debug, default=4).')

    CMD_ARGS = ARG_PARSER.parse_args()
    RenderObjects().run()

# end if
