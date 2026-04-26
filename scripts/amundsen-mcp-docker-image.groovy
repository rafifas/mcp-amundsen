pipeline {

  agent {
    label 'jenkins-slave-docker'
  }

  options {
    ansiColor('xterm')
    timestamps()
    skipStagesAfterUnstable()
    buildDiscarder(logRotator(numToKeepStr: '31'))
    disableConcurrentBuilds()
  }

  environment {
    PATH = "$PATH:/usr/lib/google-cloud-sdk/bin"
  }

  parameters {
    string(name: 'BRANCH', defaultValue: 'master', description: 'Branch to clone from amundsen_mcp repository')
  }

  stages {
    stage('Housekeeping') {
      steps {
        cleanWs()
      }
    }

    stage('Pull Code from Gitlab') {
      steps{
        script{
          git.checkout('mcp-amundsen', '${BRANCH}', 'jenkins-github-key')
        }
      }
    }

    stage('Build Docker Image'){
      steps {
        script{
          sh"""
            docker build . -t asia-southeast1-docker.pkg.dev/tools-build/vidio/amundsen-mcp:${BUILD_NUMBER}
          """
        }
      }
    }
  }
  post {
    success {
      script {
        sh"""
          gcloud auth configure-docker asia-southeast1-docker.pkg.dev

          docker tag asia-southeast1-docker.pkg.dev/tools-build/vidio/amundsen-mcp:${BUILD_NUMBER} asia-southeast1-docker.pkg.dev/tools-build/vidio/amundsen-mcp:latest

          docker push asia-southeast1-docker.pkg.dev/tools-build/vidio/amundsen-mcp:${BUILD_NUMBER}
          docker push asia-southeast1-docker.pkg.dev/tools-build/vidio/amundsen-mcp:latest

          echo "docker_image: asia-southeast1-docker.pkg.dev/tools-build/vidio/amundsen-mcp:${BUILD_NUMBER}" > docker_image.yaml
        """
        archiveArtifacts artifacts: 'docker_image.yaml', fingerprint: true
      }
    }
    cleanup {
      script {
        sh "docker system prune -f"
        sh "docker rmi asia-southeast1-docker.pkg.dev/tools-build/vidio/amundsen-mcp:${BUILD_NUMBER}"
        sh "docker rmi asia-southeast1-docker.pkg.dev/tools-build/vidio/amundsen-mcp:latest"
        cleanWs()
      }
    }
  }
}
