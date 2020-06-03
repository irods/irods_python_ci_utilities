# This code is part of Ansible, but is an independent component.
# This particular file snippet, and this file snippet only, is BSD licensed.
# Modules you write using this snippet, which is embedded dynamically by Ansible
# still belong to the author of the module, and may assign their own license
# to the complete work.
#
# Copyright (c), Michael DeHaan <michael.dehaan@gmail.com>, 2012-2013
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright notice,
#      this list of conditions and the following disclaimer in the documentation
#      and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
# IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
# USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

import platform

# from: https://github.com/ansible/ansible/blob/0a439df4b0a3173868884aa4778fd99ec80b505f/lib/ansible/module_utils/basic.py
def get_distribution():
    ''' return the distribution name '''
    if platform.system() == 'Linux':
        try:
            supported_dists = platform._supported_dists + ('arch',)
            distribution = platform.linux_distribution(supported_dists=supported_dists)[0].capitalize()
            if not distribution and os.path.isfile('/etc/system-release'):
                distribution = platform.linux_distribution(supported_dists=['system'])[0].capitalize()
                if 'Amazon' in distribution:
                    distribution = 'Amazon'
                else:
                    distribution = 'OtherLinux'
        except:
            # For openSUSE platform does not return any information about the distribution
            import distro
            distribution = distro.linux_distribution()[0].capitalize()
    else:
        distribution = None
    return distribution

# from: https://github.com/ansible/ansible/blob/0a439df4b0a3173868884aa4778fd99ec80b505f/lib/ansible/module_utils/basic.py
def get_distribution_version():
    ''' return the distribution version '''
    if platform.system() == 'Linux':
        try:
            distribution_version = platform.linux_distribution()[1]
            if not distribution_version and os.path.isfile('/etc/system-release'):
                distribution_version = platform.linux_distribution(supported_dists=['system'])[1]
        except:
            # For openSUSE platform does not return any information about the distribution
            import distro
            distribution_version = distro.linux_distribution()[1]
    else:
        distribution_version = None
    return distribution_version
