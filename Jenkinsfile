#!/usr/bin/env groovy

def config = jobConfig {
    cron = '@midnight'
    nodeLabel = 'docker-oraclejdk8-ce-kafka'
    testResultSpecs = ['junit': '**/build/test-results/**/TEST-*.xml']
    slackChannel = '#kafka-warn'
    timeoutHours = 4
    runMergeCheck = false
}


def job = {

    withCredentials([usernamePassword(
        credentialsId: 'Jenkins Nexus Account',
        usernameVariable: 'ORG_GRADLE_PROJECT_mavenUsername',
        passwordVariable: 'ORG_GRADLE_PROJECT_mavenPassword'
    )]) {

        // Per KAFKA-7524, Scala 2.12 is the default, yet we currently support the previous minor version.
        stage("Check compilation compatibility with Scala 2.11") {
            sh "gradle"
            sh "./gradlew clean compileJava compileScala compileTestJava compileTestScala " +
                    "--no-daemon --stacktrace -PscalaVersion=2.11"
        }

        stage("Compile and validate") {
            sh "./gradlew clean assemble spotlessScalaCheck checkstyleMain checkstyleTest spotbugsMain " +
                    "--no-daemon --stacktrace --continue -PxmlSpotBugsReport=true"
        }

        if (config.publish && config.isDevJob) {
            stage("Publish to nexus") {
                def mavenUrl = 'https://nexus.confluent.io/repository/maven-snapshots/'
                sh "./gradlew -PmavenUrl=${mavenUrl} --no-daemon uploadArchivesAll"
            }
        }

    }

    stage("Run Tests and build cp-downstream-builds") {
    	def runTestsStepName = "Step run-tests"
    	def downstreamBuildsStepName = "Step cp-downstream-builds"
        def testTargets = [
                     runTestsStepName: {
		        stage('Run tests') {
				echo "Running unit and integration tests"
				sh "./gradlew unitTest integrationTest " +
				   "--no-daemon --stacktrace --continue -PtestLoggingEvents=started,passed,skipped,failed -PmaxParallelForks=4 -PignoreFailures=true"
			}
			stage('Upload results') {
				// Kafka failed test stdout files
				archiveArtifacts artifacts: '**/testOutput/*.stdout', allowEmptyArchive: true

				def summary = junit '**/build/test-results/**/TEST-*.xml'
				def total = summary.getTotalCount()
				def failed = summary.getFailCount()
				def skipped = summary.getSkipCount()
				summary = "Test results:\n\t"
				summary = summary + ("Passed: " + (total - failed - skipped))
				summary = summary + (", Failed: " + failed)
				summary = summary + (", Skipped: " + skipped)
				return summary;
			}
                    },
                    downstreamBuildsStepName: {
                        echo "Building cp-downstream-builds"
                        if (config.isPrJob) {
                            def muckrakeBranch = env.CHANGE_TARGET
                            def forkRepo = "${env.CHANGE_FORK ?: "confluentinc"}/ce-kafka.git"
                            def forkBranch = env.CHANGE_BRANCH
                            echo "Schedule test-cp-downstream-builds with :"
                            echo "Muckrake branch : ${muckrakeBranch}"
                            echo "PR fork repo : ${forkRepo}"
                            echo "PR fork branch : ${forkBranch}"
                            buildResult = build job: 'test-cp-downstream-builds', parameters: [
                                    [$class: 'StringParameterValue', name: 'BRANCH', value: muckrakeBranch],
                                    [$class: 'StringParameterValue', name: 'TEST_PATH', value: "muckrake/tests/dummy_test.py"],
                                    [$class: 'StringParameterValue', name: 'KAFKA_REPO', value: forkRepo],
                                    [$class: 'StringParameterValue', name: 'KAFKA_BRANCH', value: forkBranch]],
                                    propagate: true, wait: true
			    return ", downstream build result: " + buildResult.getResult();
                        } else {
			    return ""
			}
                    }
        ]

        result = parallel testTargets
	// combine results of the two targets into one result string
	return result.runTestsStepName + result.downstreamBuildsStepName
    }
}

runJob config, job
