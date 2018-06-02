stepback: true

command_type: system

ignore:
  - /.evergreen/dependencies
  - /.evergreen/toolchain

exec_timeout_secs: 1800

timeout:
  - command: shell.exec
    params:
      script: ls -la

functions:
  "buildModule" :
    command: shell.exec
    type: test
    params:
      script: |
        ${prepare_shell}
        $PERL ${repo_directory}/.evergreen/testing/build.pl

  "cleanUp":
    command: shell.exec
    params:
      script: |
        ${prepare_shell}
        rm -rf ~/.cpanm
        rm -rf perl5
        rm -rf ${repo_directory}

  "downloadBuildArtifacts" :
    command: shell.exec
    params:
      script: |
        ${prepare_shell}
        cd ${repo_directory}
        curl https://s3.amazonaws.com/mciuploads/${aws_artifact_prefix}/${repo_directory}/${build_id}/build.tar.gz -o build.tar.gz --fail --show-error --silent --max-time 240
        tar -zxmf build.tar.gz

  "downloadPerl5Lib" :
    command: shell.exec
    params:
      script: |
        ${prepare_shell}
        curl https://s3.amazonaws.com/mciuploads/${aws_toolchain_prefix}/${os}/${perlver}/${target}/perl5lib.tar.gz -o perl5lib.tar.gz --fail --show-error --silent --max-time 240
        tar -zxf perl5lib.tar.gz

  "dynamicVars":
    - command: shell.exec
      params:
        script: |
            set -o errexit
            set -o xtrace
            cat <<EOT > expansion.yml
            prepare_shell: |
                export PATH="${addpaths}:$PATH"
                export PERL="${perlpath}"
                export REPO_DIR="${repo_directory}"
                set -o errexit
                set -o xtrace
            EOT
            cat expansion.yml
    - command: expansions.update
      params:
        file: expansion.yml

  "fetchSource" :
    - command: git.get_project
      params:
        directory: src
    - command: shell.exec
      params:
        script: |
          ${prepare_shell}
          mv src ${repo_directory}

  "testDriver" :
    command: shell.exec
    type: test
    params:
      script: |
        ${prepare_shell}
        export MONGOD=$(echo "${MONGODB_URI}" | tr -d '[:space:]')
        SSL=${ssl} $PERL ${repo_directory}/.evergreen/testing/test.pl

  "testLive" :
    command: shell.exec
    type: test
    params:
      script: |
        ${prepare_shell}
        export MONGOD="${uri}"
        $PERL ${repo_directory}/.evergreen/testing/live-test.pl

  "uploadBuildArtifacts":
    - command: s3.put
      params:
        aws_key: ${aws_key}
        aws_secret: ${aws_secret}
        local_file: ${repo_directory}/build.tar.gz
        remote_file: ${aws_artifact_prefix}/${repo_directory}/${build_id}/build.tar.gz
        bucket: mciuploads
        permissions: public-read
        content_type: application/x-gzip

  "whichPerl":
    command: shell.exec
    params:
      script: |
        ${prepare_shell}
        $PERL -v

post:
  - func: cleanUp

tasks:
  - name: build
    commands:
      - func: dynamicVars
      - func: cleanUp
      - func: fetchSource
      - func: downloadPerl5Lib
        vars:
          target: '${repo_directory}'
      - func: whichPerl
      - func: buildModule
      - func: uploadBuildArtifacts
  - name: check
    commands:
      - func: dynamicVars
      - func: cleanUp
      - func: fetchSource
      - func: downloadPerl5Lib
        vars:
          target: '${repo_directory}'
      - func: whichPerl
      - func: downloadBuildArtifacts
      - func: testDriver
    depends_on:
      - name: build
  - name: test_atlas_free
    commands:
      - func: dynamicVars
      - func: cleanUp
      - func: fetchSource
      - func: downloadPerl5Lib
        vars:
          target: '${repo_directory}'
      - func: whichPerl
      - func: downloadBuildArtifacts
      - func: testLive
        vars:
          uri: '${atlas_free}'
    depends_on:
      - name: check
  - name: test_atlas_replica
    commands:
      - func: dynamicVars
      - func: cleanUp
      - func: fetchSource
      - func: downloadPerl5Lib
        vars:
          target: '${repo_directory}'
      - func: whichPerl
      - func: downloadBuildArtifacts
      - func: testLive
        vars:
          uri: '${atlas_replica}'
    depends_on:
      - name: check
  - name: test_atlas_sharded
    commands:
      - func: dynamicVars
      - func: cleanUp
      - func: fetchSource
      - func: downloadPerl5Lib
        vars:
          target: '${repo_directory}'
      - func: whichPerl
      - func: downloadBuildArtifacts
      - func: testLive
        vars:
          uri: '${atlas_sharded}'
    depends_on:
      - name: check
  - name: test_atlas_tls11
    commands:
      - func: dynamicVars
      - func: cleanUp
      - func: fetchSource
      - func: downloadPerl5Lib
        vars:
          target: '${repo_directory}'
      - func: whichPerl
      - func: downloadBuildArtifacts
      - func: testLive
        vars:
          uri: '${atlas_tls11}'
    depends_on:
      - name: check
  - name: test_atlas_tls12
    commands:
      - func: dynamicVars
      - func: cleanUp
      - func: fetchSource
      - func: downloadPerl5Lib
        vars:
          target: '${repo_directory}'
      - func: whichPerl
      - func: downloadBuildArtifacts
      - func: testLive
        vars:
          uri: '${atlas_tls12}'
    depends_on:
      - name: check

buildvariants:
  - name: os_ubuntu1604_perl_24
    display_name: Ubuntu 16.04 Perl 5.24
    expansions:
      addpaths: /opt/perl/24/bin
      os: ubuntu1604
      perlpath: /opt/perl/24/bin/perl
      perlver: 24
    run_on:
      - ubuntu1604-test
    tasks:
      - build
      - check
      - test_atlas_free
      - test_atlas_replica
      - test_atlas_sharded
      - test_atlas_tls11
      - test_atlas_tls12
