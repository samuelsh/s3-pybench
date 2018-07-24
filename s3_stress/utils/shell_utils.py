import os
import random
import shlex
import subprocess
from string import printable, digits, ascii_letters

"""
Helper utils providing:
1. Subprocess wrapper methods
2. String utils
3. File system utils
"""

__author__ = 'samuels'

SSH_PATH = "/usr/bin/ssh"


class StringUtils:
    def __init__(self):
        pass

    @staticmethod
    def get_random_string(length):
        return ''.join(random.choice(printable) for _ in range(length))

    @staticmethod
    def get_random_string_nospec(length):
        return ''.join(random.choice(digits + ascii_letters) for _ in range(length))

    @staticmethod
    def random_string_generator():
        while 1:
            yield ''.join(random.choice(digits + ascii_letters) for _ in range(random.randint(1, 64)))

    @staticmethod
    def string_from_file_generator(file_names):
        while 1:
            for file_name in file_names:
                yield file_name.rstrip('\n')


class ShellUtils:
    def __init__(self):
        pass

    @staticmethod
    def pipe_grep(source_process, grep_pattern):
        p2 = subprocess.Popen(["grep", grep_pattern], stdin=source_process.stdout, stdout=subprocess.PIPE)
        source_process.stdout.close()  # Allow source_process to receive a SIGPIPE if p2 exits.
        output = p2.communicate()[0]
        return output.strip()

    @staticmethod
    def run_bash_function(library_path, function_name, params):
        params = shlex.split('"source %s; %s %s"' % (library_path, function_name, params))
        cmdline = ['bash', '-c'] + params
        p = subprocess.Popen(cmdline,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            raise RuntimeError("'%s' failed, error code: '%s', stdout: '%s', stderr: '%s'" % (
                ' '.join(cmdline), p.returncode, stdout.rstrip(), stderr.rstrip()))
        return stdout.strip()  # This is the stdout from the shell command

    @staticmethod
    def run_remote_bash_function(remote_host, library_path, function_name, params):
        cmdline = ['ssh', '-nx', remote_host, 'bash', '-c', '. %s; %s %s' % (library_path, function_name, params)]
        p = subprocess.Popen(cmdline,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            raise RuntimeError("%r failed, status code %s stdout %r stderr %r" % (
                function_name, p.returncode, stdout, stderr))
        return stdout.strip()  # This is the stdout from the shell command

    @staticmethod
    def run_shell_script(script, params, stdout=True):
        devnull = open(os.devnull, 'w')
        cmdline = [script]
        cmdline = cmdline + params.split(' ')
        if not stdout:
            p = subprocess.call(cmdline, stdout=devnull)
        else:
            p = subprocess.call(cmdline)
        return p

    @staticmethod
    def run_shell_script_remote(remote_host, script, params, stdout=True):
        devnull = open(os.devnull, 'w')
        if not stdout:
            p = subprocess.call(['ssh', '-nx', remote_host, script, params], stdout=devnull)
        else:
            p = subprocess.call(['ssh', '-nx', remote_host, script, params])
        return p

    @staticmethod
    def run_shell_command(cmd, params, stdout=subprocess.PIPE):
        cmdline = [cmd]
        cmdline = cmdline + shlex.split(params)
        p = subprocess.Popen(cmdline, stdout=stdout, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            raise RuntimeError("'%s' failed, error code: %d stdout: '%s' stderr: '%s'" % (
                ' '.join(cmdline), p.returncode, stdout, stderr.rstrip()), 32)
        return stdout.strip()  # This is the stdout from the shell command

    @staticmethod
    def get_shell_remote_command(remote_host, remote_cmd):
        """
        :rtype: subprocess.Popen
        """
        p = subprocess.Popen([SSH_PATH, '-nx', remote_host, remote_cmd], stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        return p

    @staticmethod
    def run_shell_remote_command(remote_host, remote_cmd):
        remote_cmd = remote_cmd.split(' ')
        p = subprocess.Popen([SSH_PATH, '-o ConnectTimeout=30', '-o BatchMode=yes', '-o StrictHostKeyChecking=no',
                              remote_host] + remote_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            raise RuntimeError("%r failed, status code %s stdout %r stderr %r" % (
                remote_cmd, p.returncode, stdout, stderr))
        return stdout.strip()  # This is the stdout from the shell command

    @staticmethod
    def run_shell_remote_command_multiline(remote_host, remote_cmd):
        p = subprocess.Popen(['ssh', '-nx', remote_host, remote_cmd], stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            raise RuntimeError("%r failed, status code %s stdout %r stderr %r" % (
                remote_cmd, p.returncode, stdout, stderr))
        return stdout.splitlines()  # This is the stdout from the shell command

    @staticmethod
    def run_shell_remote_command_background(remote_host, remote_cmd):
        subprocess.Popen(['ssh', '-nx', remote_host, remote_cmd])

    @staticmethod
    def run_shell_remote_command_no_exception(remote_host, remote_cmd):
        p = subprocess.Popen(['ssh', '-o ConnectTimeout=30', '-o BatchMode=yes', '-o StrictHostKeyChecking=no',
                              remote_host, remote_cmd], stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        p.communicate()
        if p.returncode != 0:
            return False  # This is the stdout from the shell command
        return True


class FSUtils:
    def __init__(self):
        pass


def mount(server, export, mount_point, mtype=3):
    try:
        ShellUtils.run_shell_command("mount", "-o nfsvers={0} {1}:/{2} {3}".format(mtype, server, export, mount_point))
    except OSError:
        return False
    return True


def umount(mount_point):
    try:
        ShellUtils.run_shell_command("umount", "-fl {0}".format(mount_point))
    except OSError:
        return False
    return True


def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)
