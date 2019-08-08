import contextlib
import errno
import json
import os
import pwd
import shutil
import subprocess
import sys
import tempfile
import time

from . import copied_from_ansible

def get_distribution():
    return copied_from_ansible.get_distribution()

def get_distribution_version_major():
    return copied_from_ansible.get_distribution_version().split('.')[0]

def get_irods_platform_string():
    return get_distribution() + '_' + get_distribution_version_major()

def raise_not_implemented_for_distribution():
    raise NotImplementedError, 'not implemented for distribution [{0}]'.format(copied_from_ansible.get_distribution()), sys.exc_info()[2]

def raise_not_implemented_for_distribution_major_version():
    raise NotImplementedError, 'not implemented for distribution [{0}] major version [{1}]'.format(copied_from_ansible.get_distribution(), get_distribution_version_major()), sys.exc_info()[2]

def subprocess_get_output(*args, **kwargs):
    kwargs['stdout'] = subprocess.PIPE
    kwargs['stderr'] = subprocess.PIPE
    check_rc = False
    if 'check_rc' in kwargs:
        check_rc = kwargs['check_rc']
        del kwargs['check_rc']
    data = None
    if 'data' in kwargs:
        kwargs['stdin'] = subprocess.PIPE
        data = kwargs['data']
        del kwargs['data']
    p = subprocess.Popen(*args, **kwargs)
    out, err = p.communicate(data)
    if check_rc:
        if p.returncode != 0:
            raise RuntimeError('''subprocess_get_output() failed
args: {0}
kwargs: {1}
returncode: {2}
stdout: {3}
stderr: {4}
'''.format(args, kwargs, p.returncode, out, err))
    return p.returncode, out, err

def install_os_packages_apt(packages):
    args = ['sudo', 'apt-get', 'install', '-y'] + list(packages)
    subprocess_get_output(args, check_rc=True)

def install_os_packages_yum(packages):
    args = ['sudo', 'yum', 'install', '-y'] + list(packages)
    subprocess_get_output(args, check_rc=True)

def install_os_packages_zypper(packages):
    args = ['sudo', 'zypper', '--non-interactive', 'install'] + list(packages)
    subprocess_get_output(args, check_rc=True)

def install_os_packages(packages):
    dispatch_map = {
        'Ubuntu': install_os_packages_apt,
        'Centos': install_os_packages_yum,
        'Centos linux': install_os_packages_yum,
        'Opensuse ': install_os_packages_zypper,
    }
    try:
        dispatch_map[get_distribution()](packages)
    except KeyError:
        raise_not_implemented_for_distribution()

def install_os_packages_from_files_apt(files):
    '''files are installed individually in the order supplied, so inter-file dependencies must be handled by the caller'''
    for f in files:
        subprocess_get_output(['sudo', 'gdebi', '-n', f], check_rc=True)

def install_os_packages_from_files_yum(files):
    subprocess_get_output(['sudo', 'rpm', '--rebuilddb'], check_rc=True)
    subprocess_get_output(['sudo', 'yum', 'update'], check_rc=True)
    args = ['sudo', 'yum', 'localinstall', '-y', '--nogpgcheck'] + list(files)
    subprocess_get_output(args, check_rc=True)

def install_os_packages_from_files_zypper(files):
    install_os_packages_zypper(files)

def install_os_packages_from_files(files):
    dispatch_map = {
        'Ubuntu': install_os_packages_from_files_apt,
        'Centos': install_os_packages_from_files_yum,
        'Centos linux': install_os_packages_from_files_yum,
        'Opensuse ': install_os_packages_from_files_zypper,
    }
    try:
        dispatch_map[get_distribution()](files)
    except KeyError:
        raise_not_implemented_for_distribution()

def install_irods_core_dev_repository_apt():
    subprocess_get_output('wget -qO - https://core-dev.irods.org/irods-core-dev-signing-key.asc | sudo apt-key add -', shell=True, check_rc=True)
    subprocess_get_output('echo "deb [arch=amd64] https://core-dev.irods.org/apt/ $(lsb_release -sc) main" | sudo tee /etc/apt/sources.list.d/renci-irods-core-dev.list', shell=True, check_rc=True)

def install_irods_core_dev_repository_yum():
    subprocess_get_output(['sudo', 'rpm', '--import', 'https://core-dev.irods.org/irods-core-dev-signing-key.asc'], check_rc=True)
    subprocess_get_output('wget -qO - https://core-dev.irods.org/renci-irods-core-dev.yum.repo | sudo tee /etc/yum.repos.d/renci-irods-core-dev.yum.repo', shell=True, check_rc=True)

def install_irods_core_dev_repository_zypper():
    subprocess_get_output(['sudo', 'rpm', '--import', 'https://core-dev.irods.org/irods-core-dev-signing-key.asc'], check_rc=True)
    subprocess_get_output('wget -qO - https://core-dev.irods.org/renci-irods-core-dev.zypp.repo | sudo tee /etc/zypp/repos.d/renci-irods-core-dev.zypp.repo', shell=True, check_rc=True)

def install_irods_core_dev_repository():
    dispatch_map = {
        'Ubuntu': install_irods_core_dev_repository_apt,
        'Centos': install_irods_core_dev_repository_yum,
        'Centos linux': install_irods_core_dev_repository_yum,
        'Opensuse ': install_irods_core_dev_repository_zypper,
    }
    try:
        dispatch_map[get_distribution()]()
    except KeyError:
        raise_not_implemented_for_distribution()

def get_package_suffix():
    d = copied_from_ansible.get_distribution()
    if d in ['Ubuntu']:
        return 'deb'
    if d in ['Centos', 'Centos linux', 'Opensuse ']:
        return 'rpm'
    raise_not_implemented_for_distribution()

def get_irods_version():
    '''Returns irods version as tuple of int's'''
    version = get_irods_version_from_json()
    if version:
        return version
    version = get_irods_version_from_bash()
    if version:
        return version
    raise RuntimeError('Unable to determine iRODS version')

def get_irods_version_from_json():
    try:
        with open('/var/lib/irods/VERSION.json.dist') as f:
            version_string = json.load(f)['irods_version']
    except IOError as e1:
        if e1.errno != 2:
            raise
        try:
            with open('/var/lib/irods/VERSION.json') as f:
                version_string = json.load(f)['irods_version']
        except IOError as e2:
            if e2.errno != 2:
                raise
            return None
    return tuple(map(int, version_string.split('.')))

def get_irods_version_from_bash():
    try:
        with open('/var/lib/irods/VERSION') as f:
            for line in f:
                key, _, value = line.rstrip('\n').partition('=')
                if key == 'IRODSVERSION':
                    return tuple(map(int, value.split('.')))
            return None
    except IOError as e:
        if e.errno != 2:
            raise
        return None

@contextlib.contextmanager
def euid_and_egid_set(name):
    initial_euid = os.geteuid()
    initial_egid = os.getegid()
    pw = pwd.getpwnam(name)
    euid = pw.pw_uid
    egid = pw.pw_gid
    os.setegid(egid)
    os.seteuid(euid)
    try:
        yield
    finally:
        os.seteuid(initial_euid)
        os.setegid(initial_egid)

def git_clone(repository, commitish=None, local_dir=None):
    '''Returns checkout directory'''
    if local_dir is None:
        local_dir = tempfile.mkdtemp()
    subprocess_get_output(['git', 'clone', '--recursive', repository, local_dir], check_rc=True)
    if commitish is not None:
        subprocess_get_output(['git', 'checkout', commitish], cwd=local_dir, check_rc=True)
    subprocess_get_output(['git', 'submodule', 'update', '--init', '--recursive'], cwd=local_dir, check_rc=True)
    return local_dir

def install_database(database_type):
    dispatch_map = {
        'Ubuntu': install_database_debian,
        'Centos': install_database_redhat,
        'Centos linux': install_database_redhat,
        'Opensuse ': install_database_suse,
    }
    try:
        dispatch_map[copied_from_ansible.get_distribution()](database_type)
    except KeyError:
        raise_not_implemented_for_distribution()

def install_database_debian(database_type):
    if database_type == 'postgres':
        install_os_packages(['postgresql'])
    elif database_type == 'mysql':
        subprocess_get_output(['sudo', 'debconf-set-selections'], data='mysql-server mysql-server/root_password password password', check_rc=True)
        subprocess_get_output(['sudo', 'debconf-set-selections'], data='mysql-server mysql-server/root_password_again password password', check_rc=True)
        install_os_packages(['mysql-server'])
        subprocess_get_output(['sudo', 'su', '-', 'root', '-c', "echo '[mysqld]' > /etc/mysql/conf.d/irods.cnf"], check_rc=True)
        subprocess_get_output(['sudo', 'su', '-', 'root', '-c', "echo 'log_bin_trust_function_creators=1' >> /etc/mysql/conf.d/irods.cnf"], check_rc=True)
        subprocess_get_output(['sudo', 'service', 'mysql', 'restart'], check_rc=True)
        install_mysql_pcre()
    elif database_type == 'oracle':
        with tempfile.NamedTemporaryFile() as f:
            f.write('''
export LD_LIBRARY_PATH=/usr/lib/oracle/11.2/client64/lib:$LD_LIBRARY_PATH
export ORACLE_HOME=/usr/lib/oracle/11.2/client64
export PATH=$ORACLE_HOME/bin:$PATH
''')
            f.flush()
            subprocess_get_output(['sudo', 'su', '-c', "cat '{0}' >> /etc/profile.d/oracle.sh".format(f.name)], check_rc=True)
        subprocess_get_output(['sudo', 'su', '-c', "echo 'ORACLE_HOME=/usr/lib/oracle/11.2/client64' >> /etc/environment"], check_rc=True)
        subprocess_get_output(['sudo', 'mkdir', '-p', '/usr/lib/oracle/11.2/client64/network/admin'], check_rc=True)
        tns_contents = '''
ICAT =
  (DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = default-cloud-hostname-oracle.example.org)(PORT = 1521))
    (CONNECT_DATA =
      (SERVER = DEDICATED)
      (SERVICE_NAME = ICAT.example.org)
    )
  )
'''
        subprocess_get_output(['sudo', 'su', '-c', "echo '{0}' > /usr/lib/oracle/11.2/client64/network/admin/tnsnames.ora".format(tns_contents)], check_rc=True)
    else:
        raise NotImplementedError('install_database_debian not implemented for database type [{0}]'.format(database_type))

def install_database_redhat(database_type):
    if database_type == 'postgres':
        install_os_packages(['postgresql-server'])
        subprocess_get_output(['sudo', 'su', '-', 'postgres', '-c', 'initdb'], check_rc=True)
        subprocess_get_output(['sudo', 'su', '-', 'postgres', '-c', 'pg_ctl -D /var/lib/pgsql/data -l logfile start'], check_rc=True)
        time.sleep(5)
    elif database_type == 'mysql':
        if get_distribution_version_major() == '6':
            install_os_packages(['mysql-server'])
            subprocess_get_output(['sudo', 'service', 'mysqld', 'start'], check_rc=True)
            subprocess_get_output(['mysqladmin', '-u', 'root', 'password', 'password'], check_rc=True)
            subprocess_get_output(['sudo', 'sed', '-i', r's/\[mysqld\]/\[mysqld\]\nlog_bin_trust_function_creators=1/', '/etc/my.cnf'], check_rc=True)
            subprocess_get_output(['sudo', 'service', 'mysqld', 'restart'], check_rc=True)
            install_mysql_pcre()
        elif get_distribution_version_major() == '7':
            install_os_packages(['mariadb-server'])
            subprocess_get_output(['sudo', 'systemctl', 'start', 'mariadb'], check_rc=True)
            subprocess_get_output(['mysqladmin', '-u', 'root', 'password', 'password'], check_rc=True)
            subprocess_get_output(['sudo', 'sed', '-i', r's/\[mysqld\]/\[mysqld\]\nlog_bin_trust_function_creators=1/', '/etc/my.cnf'], check_rc=True)
            subprocess_get_output(['sudo', 'systemctl', 'restart', 'mariadb'], check_rc=True)
            install_mysql_pcre()
        else:
            raise_not_implemented_for_distribution_major_version()
    elif database_type == 'oracle':
        with tempfile.NamedTemporaryFile() as f:
            f.write('''
export LD_LIBRARY_PATH=/usr/lib/oracle/11.2/client64/lib:$LD_LIBRARY_PATH
export ORACLE_HOME=/usr/lib/oracle/11.2/client64
export PATH=$ORACLE_HOME/bin:$PATH
''')
            f.flush()
            subprocess_get_output(['sudo', 'su', '-c', "cat '{0}' >> /etc/profile.d/oracle.sh".format(f.name)], check_rc=True)
        subprocess_get_output(['sudo', 'su', '-c', "echo 'ORACLE_HOME=/usr/lib/oracle/11.2/client64' >> /etc/environment"], check_rc=True)
        subprocess_get_output(['sudo', 'mkdir', '-p', '/usr/lib/oracle/11.2/client64/network/admin'], check_rc=True)
        tns_contents = '''
ICAT =
  (DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = default-cloud-hostname-oracle.example.org)(PORT = 1521))
    (CONNECT_DATA =
      (SERVER = DEDICATED)
      (SERVICE_NAME = ICAT.example.org)
    )
  )
'''
        subprocess_get_output(['sudo', 'su', '-c', "echo '{0}' > /usr/lib/oracle/11.2/client64/network/admin/tnsnames.ora".format(tns_contents)], check_rc=True)
    else:
        raise NotImplementedError('install_database_redhat not implemented for database type [{0}]'.format(database_type))

def install_database_suse(database_type):
    if database_type == 'postgres':
        install_os_packages(['postgresql-server'])
        subprocess_get_output(['sudo', 'su', '-', 'postgres', '-c', 'initdb'], check_rc=True)
        subprocess_get_output(['sudo', 'su', '-', 'postgres', '-c', "echo 'standard_conforming_strings = off' >> /var/lib/pgsql/data/postgresql.conf"], check_rc=True)
        subprocess_get_output(['sudo', 'su', '-', 'postgres', '-c', 'pg_ctl -D /var/lib/pgsql/data -l logfile start'], check_rc=True)
        time.sleep(5)
    elif self.icat_database_type == 'mysql':
        install_os_packages(['mysql-community-server'])
        subprocess_get_output(['sudo', 'su', '-', 'root', '-c', "echo '[mysqld]' > /etc/my.cnf.d/irods.cnf"], check_rc=True)
        subprocess_get_output(['sudo', 'su', '-', 'root', '-c', "echo 'log_bin_trust_function_creators=1' >> /etc/my.cnf.d/irods.cnf"], check_rc=True)
        subprocess_get_output(['sudo', 'service', 'mysql', 'restart'], check_rc=True)
        subprocess_get_output(['mysqladmin', '-u', 'root', 'password', 'password'], check_rc=True)
        subprocess_get_output(['sudo', 'service', 'mysql', 'restart'], check_rc=True)
        subprocess_get_output(['libmysqlclient-devel', 'autoconf', 'git'], 'mysql')
    else:
        raise NotImplementedError('install_database_suse not implemented for database type [{0}]'.format(database_type))

def get_mysql_pcre_build_dependencies():
    distribution = copied_from_ansible.get_distribution()
    if distribution == 'Ubuntu':
        return ['libpcre3-dev', 'libmysqlclient-dev', 'build-essential', 'libtool', 'autoconf', 'git']
    if distribution in ['Centos', 'Centos linux']:
        return ['pcre-devel', 'gcc', 'make', 'automake', 'mysql-devel', 'autoconf', 'git']
    if distribution == 'Opensuse ':
        return ['libmysqlclient-devel', 'autoconf', 'git']
    raise_not_implemented_for_distribution()

def get_mysql_service_name():
    distribution = copied_from_ansible.get_distribution()
    if distribution == 'Ubuntu':
        return 'mysql'
    if distribution == 'Centos':
        return 'mysqld'
    if distribution == 'Centos linux':
        return 'mariadb'
    if distribution == 'Opensuse ':
        return 'mysql'
    raise_not_implemented_for_distribution()

def install_mysql_pcre():
    install_os_packages(get_mysql_pcre_build_dependencies())
    local_pcre_git_dir = os.path.expanduser('~/lib_mysqludf_preg')
    subprocess_get_output(['git', 'clone', 'https://github.com/mysqludf/lib_mysqludf_preg.git', local_pcre_git_dir], check_rc=True)
    subprocess_get_output(['git', 'checkout', 'lib_mysqludf_preg-1.1'], cwd=local_pcre_git_dir, check_rc=True)
    subprocess_get_output(['autoreconf', '--force', '--install'], cwd=local_pcre_git_dir, check_rc=True)
    subprocess_get_output(['sudo', './configure'], cwd=local_pcre_git_dir, check_rc=True)
    subprocess_get_output(['sudo', 'make', 'install'], cwd=local_pcre_git_dir, check_rc=True)
    subprocess_get_output('mysql --user=root --password="password" < installdb.sql', shell=True, cwd=local_pcre_git_dir, check_rc=True)
    subprocess_get_output(['sudo', 'service', get_mysql_service_name(), 'restart'], check_rc=True)

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def gather_files_satisfying_predicate(source_directory, output_directory, predicate):
    mkdir_p(output_directory)
    gathered_files = []
    for basename in os.listdir(source_directory):
        fullpath = os.path.join(source_directory, basename)
        if os.path.isfile(fullpath):
            if predicate(fullpath):
                shutil.copy2(fullpath, output_directory)
                gathered_files.append(fullpath)
    return gathered_files

def append_os_specific_directory(root_directory):
    return os.path.join(root_directory, get_irods_platform_string())

def make_symbolic_link_as_root(target, link_name):
    if not os.path.lexists(link_name):
        subprocess_get_output(['sudo', 'ln', '-s', target, link_name], check_rc=True)
    else:
        existing_target = os.readlink(link_name)
        if existing_target != target:
            raise RuntimeError('link {0} already exists with target {1} instead of {2}'.format(link_name, existing_target, target))

def install_irods_dev_and_runtime_packages(irods_packages_root_directory):
    irods_packages_directory = append_os_specific_directory(irods_packages_root_directory)
    dev_package_basename = filter(lambda x:'irods-dev' in x, os.listdir(irods_packages_directory))[0]
    dev_package = os.path.join(irods_packages_directory, dev_package_basename)
    install_os_packages_from_files([dev_package])
    runtime_package_basename = filter(lambda x:'irods-runtime' in x, os.listdir(irods_packages_directory))[0]
    runtime_package = os.path.join(irods_packages_directory, runtime_package_basename)
    install_os_packages_from_files([runtime_package])

def register_logging_stream_handler(stream, minimum_log_level):
    logging.getLogger().setLevel(minimum_log_level)
    logging_handler = logging.StreamHandler(stream)
    logging_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)7s - %(filename)30s:%(lineno)4d - %(message)s',
        '%Y-%m-%dT%H:%M:%SZ'))
    logging_handler.formatter.converter = time.gmtime
    logging_handler.setLevel(minimum_log_level)
    logging.getLogger().addHandler(logging_handler)

def copy_file_if_exists(source_file, dest_file_or_directory):
    try:
        shutil.copy2(source_file, dest_file_or_directory)
    except IOError as e:
        if e.errno != errno.ENOENT:
            raise
