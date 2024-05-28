"""
BitBake 'Fetch' implementation for Amazon AWS S3.

Class for fetching files from Amazon S3 using the AWS Command Line Interface.
The aws tool must be correctly installed and configured prior to use.

"""

# Copyright (C) 2017, Andre McCurdy <armccurdy@gmail.com>
#
# Based in part on bb.fetch2.wget:
#    Copyright (C) 2003, 2004  Chris Larson
#
# SPDX-License-Identifier: GPL-2.0-only
#
# Based on functions from the base bb module, Copyright 2003 Holger Schurig

import os
import bb
import urllib.request, urllib.parse, urllib.error
import re
import time
from bb.fetch2 import FetchMethod
from bb.fetch2 import FetchError
from bb.fetch2 import runfetchcmd

try:
    import boto3
    import botocore.exceptions
except Exception:
    # We will test for their existence later
    pass

def convertToBytes(value, unit):
    value = float(value)
    if (unit == "KiB"):
        value = value*1024.0;
    elif (unit == "MiB"):
        value = value*1024.0*1024.0;
    elif (unit == "GiB"):
        value = value*1024.0*1024.0*1024.0;
    return value

class S3ProgressHandler(bb.progress.LineFilterProgressHandler):
    """
    Extract progress information from s3 cp output, e.g.:
    Completed 5.1 KiB/8.8 GiB (12.0 MiB/s) with 1 file(s) remaining
    """
    def __init__(self, d):
        super(S3ProgressHandler, self).__init__(d)
        # Send an initial progress event so the bar gets shown
        self._fire_progress(0)

    def writeline(self, line):
        percs = re.findall(r'^Completed (\d+.{0,1}\d*) (\w+)\/(\d+.{0,1}\d*) (\w+) (\(.+\)) with\s+', line)
        if percs:
            completed = (percs[-1][0])
            completedUnit = (percs[-1][1])
            total = (percs[-1][2])
            totalUnit = (percs[-1][3])
            completed = convertToBytes(completed, completedUnit)
            total = convertToBytes(total, totalUnit)
            progress = (completed/total)*100.0
            rate = percs[-1][4]
            self.update(progress, rate)
            return False
        return True


class BotoProgress():
    def __init__(self, d, size):
        self._data = d
        self._size = size
        self._last_time = time.time()
        self._downloaded = 0

    def progress_update(self, chunk_size):
        self._downloaded += chunk_size
        progress = 100 * self._downloaded / self._size
        if progress > 100:
            progress = 100

        if self._size:
            new_time = time.time()
            diff_time = new_time - self._last_time
            rate = chunk_size / diff_time

            self._last_time = new_time
            unit = "B/s"
            if rate > 1024:
                rate /= 1024
                unit = "KiB/s"
                if rate > 1024:
                    rate /= 1024
                    unit = "MiB/s"
                    if rate > 1024:
                        rate /= 1024
                        unit = "GiB/s"
            rate_str = f"{rate:.2f} {unit}"
        else:
            rate_str = None

        bb.event.fire(bb.build.TaskProgress(progress, rate_str), self._data)


class S3(FetchMethod):
    """Class to fetch urls via 'aws s3'"""

    def __init__(self):
        try:
            # If this fails it means boto3 wasn't available or some other issue
            # exists with boto3 so we will simply fallback to the less
            # efficient awscli method.
            self._s3 = boto3.client('s3')
        except Exception:
            self._s3 = None

    def supports(self, ud, d):
        """
        Check to see if a given url can be fetched with s3.
        """
        return ud.type in ['s3']

    def recommends_checksum(self, urldata):
        return True

    def urldata_init(self, ud, d):
        if 'downloadfilename' in ud.parm:
            ud.basename = ud.parm['downloadfilename']
        else:
            ud.basename = os.path.basename(ud.path)

        ud.localfile = ud.basename

        ud.basecmd = d.getVar("FETCHCMD_s3") or "/usr/bin/env aws s3"

    def _boto_download(self, ud, d):
        key = ud.path.lstrip("/")
        try:
            size = self._s3.head_object(Bucket=ud.host, Key=key)["ResponseMetadata"]["HTTPHeaders"]["content-length"]
            size = int(size)
        except Exception:
            size = None
        progress = BotoProgress(d, size)
        try:
            self._s3.download_file(ud.host, key, ud.localpath, Callback=progress.progress_update)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                raise FetchError(f"boto3 failed to find {ud.url}")
            elif e.response['Error']['Code'] == 403:
                raise FetchError(f"boto3 does not have access to {ud.url}")
            raise FetchError(f"boto3 failed to download {ud.url}. Error code: {e.response['Error']['Code']}")
        return True

    def download(self, ud, d):
        """
        Fetch urls
        Assumes localpath was called first
        """

        cmd = '%s cp s3://%s%s %s' % (ud.basecmd, ud.host, ud.path, ud.localpath)
        bb.fetch2.check_network_access(d, cmd, ud.url)

        if self._s3:
            return self._boto_download(ud, d)

        progresshandler = S3ProgressHandler(d)
        runfetchcmd(cmd, d, False, log=progresshandler)

        # Additional sanity checks copied from the wget class (although there
        # are no known issues which mean these are required, treat the aws cli
        # tool with a little healthy suspicion).

        if not os.path.exists(ud.localpath):
            raise FetchError("The aws cp command returned success for s3://%s%s but %s doesn't exist?!" % (ud.host, ud.path, ud.localpath))

        if os.path.getsize(ud.localpath) == 0:
            os.remove(ud.localpath)
            raise FetchError("The aws cp command for s3://%s%s resulted in a zero size file?! Deleting and failing since this isn't right." % (ud.host, ud.path))

        return True

    def _boto_checkstatus(self, ud):
        key = ud.path.lstrip("/")
        try:
            self._s3.head_object(Bucket=ud.host, Key=key)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                raise FetchError(f"boto3 failed to find {ud.url}")
            elif e.response['Error']['Code'] == 403:
                raise FetchError(f"boto3 does not have access to {ud.url}")
            raise FetchError(f"boto3 failed to access {ud.url}. Error code: {e.response['Error']['Code']}")
        return True

    def checkstatus(self, fetch, ud, d):
        """
        Check the status of a URL
        """

        cmd = '%s ls s3://%s%s' % (ud.basecmd, ud.host, ud.path)
        bb.fetch2.check_network_access(d, cmd, ud.url)

        if self._s3:
            return self._boto_checkstatus(ud)

        output = runfetchcmd(cmd, d)

        # "aws s3 ls s3://mybucket/foo" will exit with success even if the file
        # is not found, so check output of the command to confirm success.

        if not output:
            raise FetchError("The aws ls command for s3://%s%s gave empty output" % (ud.host, ud.path))

        return True
