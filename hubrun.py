from paramiko.client import SSHClient, AutoAddPolicy

from datetime import datetime
import random
import time
import string
import sys
import requests
import secrets

import logging
logger = logging.getLogger('hubrun')
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler('hubrun_log.log')
formatter = logging.Formatter('%(asctime)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def send_slack_message(source, docs='?', elapsed_time='an unknown amount of', err_msg=False):
    if err_msg:
        message = f"{source} failed with the following error\n ```{err_msg}```"
    else:
        message = f"{source} updated in {elapsed_minutes} minutes with {docs} documents"

    logger.info(message)
    requests.post(secrets.SLACK_HOOK_URL, json={'text': message})

def get_document_count(release_name, previous=False):
    versions_url = f'https://biothings-releases.s3.amazonaws.com/{release_name}/versions.json'

    # chronological so -1 for most recent and -2 for second most recent
    release_index = -2 if previous else -1
    latest_url   = requests.get(versions_url).json()['versions'][release_index]['url']
    changes_url  = requests.get(latest_url).json()['changes']['json']['url']

    return int(requests.get(changes_url).json()['new']['_count'])

def create_build_name(plugin_name):
    letters_and_digits = string.ascii_lowercase + string.digits
    random_str = ''.join((random.choice(letters_and_digits) for i in range(8)))
    time_str = datetime.now().strftime("%Y%m%d%H%M%S")

    return "{}_{}_{}".format(plugin_name, time_str, random_str).lower()

def get_previous_build_name(plugin_name):
    build_request = requests.get(f'http://localhost:19180/builds?conf_name={plugin_name}')
    payload = build_request.json()
    try:
        return payload['result'][0]['_id']
    except:
        return

def job_manager_busy():
    job_manager_url     = 'http://localhost:19180/job_manager'
    job_manager_payload = requests.get(job_manager_url)
    job_manager_json    = job_manager_payload.json()

    running_commands = job_manager_json['result']['queue']['process']['running']
    return any(running_commands)

def run_command(ssh_client, command):
    logger.info(command)
    restart_url = 'http://localhost:19180/restart'
    job_manager_wait_retries = 0

    while job_manager_busy():
        logger.info('job mgr busy')
        job_manager_wait_retries += 1
        time.sleep(60)
        if job_manager_wait_retries >= 10:
            logger.info('took too long')
            return False

    ssh_client.exec_command(command)

    if not wait_for_job_manager():
        logger.info('restarting')
        requests.put(restart_url)
        time.sleep(90)
        return False

    return True


def wait_for_job_manager():
    retries = 0
    time_waited = 0

    # check after 30s, then every minute for 5 minutes, then every 5m for three hours
    wait_times = [30]
    wait_times.extend([60] * 5)
    wait_times.extend([300] * 36)

    while retries < len(wait_times):
        wait_time = wait_times[retries]
        retries += 1
        time.sleep(wait_time)
        time_waited += wait_time
        print(f'{time_waited / 60} minutes', end='\r')
        if retries % 3 == 2:
            logger.info('waiting')
        if not job_manager_busy():
            if retries > 1:
                logger.info(f'{retries} times {time_waited // 60} minutes')
            return True

    logger.info('waited too long')
    return False
    
def main():
    plugins = ['protocolsio', 'pdb', 'covid_imperial_college', 'figshare', 'clinical_trials', 'dataverse', 'biorxiv', 'litcovid']
    #random.shuffle(plugins)
    dumpers = {'figshare': 'covid_figshare', 'pdb': 'covid_pdb_datasets', 'clinical_trials': 'covid_who_clinical_trials', 'dataverse': 'dataverses'}
    commands = [
            "dump(src='{source_name}')",
            "merge(build_name='{plugin}', target_name='{build_name}', force=False)",
            "index(indexer_env='su07', target_name='{build_name}', index_name=None)",
            "snapshot(snapshot_env='s3_outbreak_from_su07', index='{build_name}', snapshot=None)",
            "publish_snapshot(publisher_env='s3_outbreak', snapshot='{build_name}', build_name='{build_name}', previous_build='{previous_build_name}')",
            "install('{release_name}')"
    ]

    if len(sys.argv) > 1:
        if sys.argv[1] == '-not':
            plugins = [plugin for plugin in plugins if plugin not in sys.argv[2:]]
        if sys.argv[1] == '-just':
            plugins = sys.argv[2:]

    logger.info("Generating new releases for {}".format(', '.join(plugins)))
    outbreak_client = SSHClient()
    outbreak_client.set_missing_host_key_policy(AutoAddPolicy())
    outbreak_client.connect(secrets.HUB_HOST, port=secrets.HUB_PORT, username=secrets.HUB_USERNAME, password=secrets.HUB_PASSWORD)
    messages = []

    for plugin in plugins:
        err_msg = False
        doc_count = '?'
        try:
            start_time          = time.time()
            release_name        = f"outbreak-{plugin}"
            source_name         = dumpers.get(plugin) or plugin
            build_name          = create_build_name(plugin)
            previous_build_name = get_previous_build_name(plugin)
            for command in commands:
                build_command = command.format(plugin=plugin, build_name=build_name, previous_build_name=previous_build_name, source_name=source_name, release_name=release_name)
                
                run_command(outbreak_client, build_command)

                if command.startswith('publish'):
                    # publish adds a version to versions.json
                    # we review the document counts from the new versus the old here
                    doc_count          = get_document_count(release_name)
                    previous_doc_count = get_document_count(release_name, previous=True)
                    if doc_count < previous_doc_count:
                        raise Exception(f"New document count ({doc_count}) less than older document count ({previous_doc_count})")


        except Exception as e:
            try:
                command = build_command.split('(')[0]
            except (NameError, TypeError):
                command = '?'
            err_msg = f"`{command}` {getattr(e, 'message', '')} {e}"

        end_time = time.time()
        send_slack_message(plugin, doc_count, round((end_time - start_time) / 60), err_msg)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        raise e
        send_slack_message('builder', err_msg=f"{getattr(e, 'message', '')} {e}")
