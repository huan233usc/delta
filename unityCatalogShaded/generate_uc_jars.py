#!/usr/bin/env python3

#
# Copyright (2024) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import argparse
import os
import glob
import shutil
import subprocess
import shlex
from os import path

uc_lib_dir_name = "lib"
uc_src_dir_name = "unitycatalog_src"  # this is a git dir

# Use master branch
uc_src_branch = "main"

# Relative to uc_src directory.
uc_src_compiled_jar_rel_glob_patterns = [
    "server-shaded/target/unitycatalog-server-shaded-assembly-*.jar",  # Shaded server with all dependencies
    "connectors/spark/target/scala-2.12/unitycatalog-spark_2.12-*.jar",  # Scala 2.12 version
    "target/clients/java/target/unitycatalog-client-*.jar"
]

uc_root_dir = path.abspath(path.dirname(__file__))
uc_src_dir = path.join(uc_root_dir, uc_src_dir_name)
uc_lib_dir = path.join(uc_root_dir, uc_lib_dir_name)


def uc_jars_exist():
    for compiled_jar_rel_glob_pattern in uc_src_compiled_jar_rel_glob_patterns:
        jar_file_name_pattern = path.basename(path.normpath(compiled_jar_rel_glob_pattern))
        lib_jar_abs_pattern = path.join(uc_lib_dir, jar_file_name_pattern)
        results = glob.glob(lib_jar_abs_pattern)

        if len(results) > 1:
            raise Exception("More jars than expected: " + str(results))
        
        if len(results) == 0:
            return False

    return True


def prepare_uc_source():
    with WorkingDirectory(uc_root_dir):
        print(">>> Cloning Unity Catalog repo")
        shutil.rmtree(uc_src_dir_name, ignore_errors=True)

        run_cmd("git clone --depth 1 --branch %s https://github.com/unitycatalog/unitycatalog.git %s" %
                (uc_src_branch, uc_src_dir_name))
        
        # Patch build.sbt to support Scala 2.12
        print(">>> Patching Unity Catalog build.sbt for Scala 2.12")
        build_sbt_path = path.join(uc_src_dir, "build.sbt")
        with open(build_sbt_path, 'r') as f:
            content = f.read()
        
        # Add Scala 2.12 version variable
        content = content.replace(
            'lazy val scala213 = "2.13.16"',
            'lazy val scala212 = "2.12.18"\nlazy val scala213 = "2.13.16"'
        )
        
        # Update spark connector to support Scala 2.12 cross-compilation
        content = content.replace(
            'crossScalaVersions := Seq(scala213),',
            'crossScalaVersions := Seq(scala212, scala213),'
        )
        
        # Use Spark 3.5.3 and Delta 3.2.1 for Scala 2.12 compatibility
        content = content.replace(
            'lazy val sparkVersion = "4.0.0"',
            'lazy val sparkVersion = "3.5.3"'
        )
        content = content.replace(
            'lazy val deltaVersion = "4.0.0"',
            'lazy val deltaVersion = "3.2.1"'
        )
        
        with open(build_sbt_path, 'w') as f:
            f.write(content)
        
        print(">>> Build configuration patched successfully")


def generate_uc_jars():
    print(">>> Compiling Unity Catalog JARs with Scala 2.12")
    with WorkingDirectory(uc_src_dir):
        # Build Unity Catalog with tests skipped, using Scala 2.12 for spark connector
        run_cmd("build/sbt clean")
        run_cmd("build/sbt -DskipTests ++2.12.18 spark/package")  # Compile spark connector with Scala 2.12
        run_cmd("build/sbt -DskipTests client/package")  # Compile client
        # Build the shaded server JAR with all dependencies (server is Scala 2.12)
        run_cmd("build/sbt -DskipTests serverShaded/assembly")

    print(">>> Copying JARs to lib directory")
    shutil.rmtree(uc_lib_dir, ignore_errors=True)
    os.mkdir(uc_lib_dir)

    for compiled_jar_rel_glob_pattern in uc_src_compiled_jar_rel_glob_patterns:
        compiled_jar_abs_pattern = path.join(uc_src_dir, compiled_jar_rel_glob_pattern)
        results = glob.glob(compiled_jar_abs_pattern)
        
        # Filter out test jars, sources, javadocs
        results = list(filter(
            lambda result: all(x not in result for x in ["tests.jar", "sources.jar", "javadoc.jar"]),
            results
        ))

        if len(results) == 0:
            raise Exception("Could not find the jar: " + compiled_jar_rel_glob_pattern)
        if len(results) > 1:
            raise Exception("More jars created than expected: " + str(results))

        compiled_jar_abs_path = results[0]
        compiled_jar_name = path.basename(path.normpath(compiled_jar_abs_path))
        lib_jar_abs_path = path.join(uc_lib_dir, compiled_jar_name)
        shutil.copyfile(compiled_jar_abs_path, lib_jar_abs_path)
        print(f">>> Copied {compiled_jar_name}")

    if not uc_jars_exist():
        raise Exception("JAR copying failed")


def run_cmd(cmd, throw_on_error=True, env=None, stream_output=True, **kwargs):
    if isinstance(cmd, str):
        cmd = shlex.split(cmd)
    cmd_env = os.environ.copy()
    if env:
        cmd_env.update(env)

    if stream_output:
        child = subprocess.Popen(cmd, env=cmd_env, **kwargs)
        exit_code = child.wait()
        if throw_on_error and exit_code != 0:
            raise Exception("Non-zero exitcode: %s" % (exit_code))
        print("----\n")
        return exit_code
    else:
        child = subprocess.Popen(
            cmd,
            env=cmd_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **kwargs)
        (stdout, stderr) = child.communicate()
        exit_code = child.wait()
        if throw_on_error and exit_code != 0:
            raise Exception(
                "Non-zero exitcode: %s\n\nSTDOUT:\n%s\n\nSTDERR:%s" %
                (exit_code, stdout, stderr))
        return (exit_code, stdout, stderr)


class WorkingDirectory(object):
    def __init__(self, working_directory):
        self.working_directory = working_directory
        self.old_workdir = os.getcwd()

    def __enter__(self):
        os.chdir(self.working_directory)

    def __exit__(self, tpe, value, traceback):
        os.chdir(self.old_workdir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--force",
        required=False,
        default=False,
        action="store_true",
        help="Force the generation even if already generated, useful for testing.")
    args = parser.parse_args()

    if args.force or not uc_jars_exist():
        prepare_uc_source()
        generate_uc_jars()
        print(">>> Unity Catalog JARs generation completed successfully!")
    else:
        print(">>> Unity Catalog JARs already exist, skipping generation.")

