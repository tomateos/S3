const { spawn } = require('child_process');
const async = require('async');
const assert = require('assert');
/* eslint no-console: ["error", { allow: ["log", "error"] }] */
function spawnAndLog(cmd, isKillingServer, cb) {
    const child = spawn(cmd, { shell: true });
    console.log('CMD: ', cmd);
    child.stdout.on('data', data => {
        console.log(data.toString());
    });
    child.on('error', err => {
        cb(err);
    });
    child.on('exit', exitCode => {
        console.log(`child exited with code: ${exitCode}`);
        if (exitCode === 0) {
            if (isKillingServer) {
                const killServer = spawn('kill -9 $(lsof -t -i:8000)',
                  { shell: true });
                return killServer.on('exit', exitKillServer => {
                    if (exitKillServer === 0) {
                        console.log(`killServer exited with code: ${exitCode}`);
                        return cb();
                    }
                    return cb(`Failed killing server: ${cmd}`);
                });
            }
            return cb();
        }
        return cb(`Failed: ${cmd}`);
    });
}

// const coco = '0';
// process.env.CIRCLE_NODE_INDEX
if (process.env.CIRCLE_NODE_INDEX === '0') {
    // log(execSync('npm run --silent lint -- --max-warnings 0'));
    // log('1!!!');
    async.series([
        next => spawnAndLog('npm run --silent lint -- --max-warnings 0', false,
        next),
        next => spawnAndLog('npm run --silent lint_md', false, next),
        next => spawnAndLog('flake8 $(git ls-files "*.py")', false, next),
        next => spawnAndLog('yamllint $(git ls-files "*.yml")', false, next),

        next => spawnAndLog('mkdir -p $CIRCLE_TEST_REPORTS/unit', false, next),
        next => spawnAndLog('npm run unit_coverage', false, next),
        next => spawnAndLog('npm run start_dmd &' +
         ' bash wait_for_local_port.bash 9990 40 && ' +
         'npm run multiple_backend_test', false, next),
        // Run S3 with multiple data backends ; run ft_tests
        next => spawnAndLog('S3BACKEND=mem S3DATA=multiple npm start ' +
          '> $CIRCLE_ARTIFACTS/server_multiple_java.txt ' +
          '& bash wait_for_local_port.bash 8000 40 ' +
          '&& cd ./tests/functional/jaws && mvn test', true, next),
        next => spawnAndLog('S3BACKEND=mem S3DATA=multiple npm start ' +
           '> $CIRCLE_ARTIFACTS/server_multiple_fog.txt ' +
           '& bash wait_for_local_port.bash 8000 40 ' +
           '&& cd tests/functional/fog && rspec tests.rb', true, next),
        next => spawnAndLog(
          'S3BACKEND=mem MPU_TESTING=yes S3DATA=multiple npm start ' +
          '> $CIRCLE_ARTIFACTS/server_multiple_awssdk.txt ' +
          '& bash wait_for_local_port.bash 8000 40 ' +
          '&& S3DATA=multiple npm run ft_awssdk', true, next),
        // Run S3 with multiple data backends + KMS Encryption; run ft_awssdk
        // next => spawnAndLog(
        //   'S3BACKEND=mem MPU_TESTING=yes S3DATA=multiple npm start ' +
        //   '> $CIRCLE_ARTIFACTS/server_multiple_kms_awssdk.txt ' +
        //   '& bash wait_for_local_port.bash 8000 40 ' +
        //   '&& S3DATA=multiple ENABLE_KMS_ENCRYPTION=true npm run ft_awssdk',
        //   next),
        // // Run S3 with mem Backend ; run ft_tests
        // next => spawnAndLog(
        //   'S3BACKEND=mem npm start ' +
        //   '> $CIRCLE_ARTIFACTS/server_mem_java.txt ' +
        //   '& bash wait_for_local_port.bash 8000 40 ' +
        //   '&& cd ./tests/functional/jaws && mvn test', next),
        // next => spawnAndLog(
        //   'S3BACKEND=mem npm start' +
        //   '> $CIRCLE_ARTIFACTS/server_mem_fog.txt ' +
        //   '& bash wait_for_local_port.bash 8000 40 ' +
        //   '&& cd tests/functional/fog && rspec tests.rb', next),
        // next => spawnAndLog(
        //   'S3BACKEND=mem MPU_TESTING=yes npm start ' +
        //   '> $CIRCLE_ARTIFACTS/server_mem_awssdk.txt ' +
        //   '& bash wait_for_local_port.bash 8000 40 ' +
        //   '&& npm run ft_awssdk', next),
        // next => spawnAndLog(
        //   'S3BACKEND=mem npm start' +
        //   '> $CIRCLE_ARTIFACTS/server_mem_s3cmd.txt ' +
        //   '& bash wait_for_local_port.bash 8000 40 ' +
        //   '&& npm run ft_s3cmd', next),
        // next => spawnAndLog(
        //   'S3BACKEND=mem npm start' +
        //   '> $CIRCLE_ARTIFACTS/server_mem_s3curl.txt ' +
        //   '& bash wait_for_local_port.bash 8000 40 ' +
        //   '&& npm run ft_s3curl', next),
        // next => spawnAndLog(
        //   'S3BACKEND=mem npm start ' +
        //   '> $CIRCLE_ARTIFACTS/server_mem_rawnode.txt ' +
        //   '& bash wait_for_local_port.bash 8000 40 ' +
        //   '&& npm run ft_node', next),
        // Run S3 with mem Backend + KMS Encryption ; run ft_tests
    ], err => {
        assert.equal(err, null, `Expected success but got error ${err}`);
        console.log('END!!!');
        process.exit();
    });
}
