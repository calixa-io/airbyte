/**
 * run it on CLI
 * export KUBE_NAMESPACE=airbyte
 * while true; do echo "---------------------------------"; node ./airbyte_kill_stuck_pods.js; echo "---------------------------------"; echo ""; sleep 30; done
 */
const process = require('process');
const { exec } = require('child_process');

const KUBE_NAMESPACE = process.env["KUBE_NAMESPACE"]
const LIST_CMD = `kubectl -n ${KUBE_NAMESPACE} -L airbyte --field-selector status.phase=Running get pods -o=json`
const DELETE_CMD = `kubectl -n ${KUBE_NAMESPACE} delete pod `

function log(message, severity, details) {
  console.log(JSON.stringify({
    timestamp: (new Date()).toISOString(),
    severity: severity,
    message: message,
    ...details
  }))
}

function info(message, details = {}) {
  log(message, "info", details);
}

function error_(message, details = {}) {
  log(message, "error", details);
}

function dig(data) {
  let head = data
  for (let i = 1; i < arguments.length; i++) {
    let k = arguments[i];
    head = head[k]
    if (head === undefined) {
      return undefined
    }
  }

  return head
}

function find_stuck_pods(json) {
  let stuck_pods = []

  json['items'].forEach(item => {
    const airbyte_label = dig(item, 'metadata', 'labels', 'airbyte')

    if (airbyte_label === 'job-pod') {
      const container_statuses = dig(item, 'status', 'containerStatuses')
      // if the "main" container is terminated, consider the pod as stuck,
      // since the overall pod is still reported as "running"
      for (let i = 0; i < container_statuses.length; i++) {
        let name = container_statuses[i]['name']

        if (name === 'main') {
          let status = container_statuses[i]['state']

          if (status['terminated']) {

            let finished_at = status['terminated']['finishedAt']
            let age_millis = Date.now() - Date.parse(finished_at)

            //  there appears to be a race condition, so we check that now - status['finishedAt'] > 5 min
            if (age_millis > 60 * 5 * 1000) {
              let stuck_pod_name = dig(item, 'metadata', 'name')
              info(`Found stuck pod ${stuck_pod_name} (${finished_at}, age millis = ${age_millis})`)
              stuck_pods.push(stuck_pod_name)
            }
          }
          break
        }
      }
    }
  })

  return stuck_pods
}

function delete_pods(pod_names) {
  for (let i = 0; i < pod_names.length; i++) {
    let pod_name = pod_names[i]
    const delete_cmd = DELETE_CMD + pod_name
    info(`Deleting pod ${pod_name}. Executing: '${delete_cmd}'`)
    exec(delete_cmd, (error, stdout, stderr) => {
      if (error) {
        error_(`Error while executing ${delete_cmd}: ${error}`);
        process.exit(87)
      }
      info(`Deleted ${pod_name} with message: ${stdout}`)
      if (i === pod_names.length - 1) {
        info('Done deleting pods. Exiting...')
      }
    })
  }
}

/**
 * MAIN
 */

if (KUBE_NAMESPACE === undefined || KUBE_NAMESPACE.trim().length == 0) {
  error_(`KUBE_NAMESPACE not set (KUBE_NAMESPACE=${KUBE_NAMESPACE})`)
  process.exit(88)
}

info(`Listing running Airbyte pods. Executing: '${LIST_CMD}'`)
exec(LIST_CMD, {maxBuffer: 1024 * 1024 * 50}, (error, stdout, stderr) => {
  if (error) {
    error_(`Error while executing ${LIST_CMD}: ${error}`);
    process.exit(85)
  }

  try {
    const data = JSON.parse(stdout);
    let stuck_pods = find_stuck_pods(data)
    if (stuck_pods.length > 0) {
      info(`Found ${stuck_pods.length} stuck pod(s): ${stuck_pods}`)
      delete_pods(stuck_pods)
    } else {
      info('Did not find any stuck pods. Exiting...')
    }
  } catch (e) {
    console.log(e.stack)
    process.exit(86);
  }
});
