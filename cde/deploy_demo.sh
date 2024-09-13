#!/bin/sh

docker_user=$1
cde_user=$2
cdp_data_lake_storage=$3

cde_user_formatted=${cde_user//[-._]/}
d=$(date)
fmt="%-30s %s\n"

echo "##########################################################"
printf "${fmt}" "CDE Ice Demo Setup launched."
printf "${fmt}" "launch time:" "${d}"
printf "${fmt}" "performed by CDP User:" "${cde_user}"
printf "${fmt}" "performed by Docker User:" "${docker_user}"
echo "##########################################################"

echo "CREATE DOCKER RUNTIME RESOURCE"
cde job delete --name ice-demo-setup-$cde_user
cde credential delete --name docker-creds-$cde_user"-ice-demo"
cde credential create --name docker-creds-$cde_user"-ice-demo" --type docker-basic --docker-server hub.docker.com --docker-username $docker_user
cde resource create --name dex-spark-runtime-$cde_user --image pauldefusco/dex-spark-runtime-3.2.3-7.2.15.8:1.20.0-b15-great-expectations-data-quality --image-engine spark3 --type custom-runtime-image
echo "CREATE FILE RESOURCE FOR DEMOS"
cde resource delete --name ice-demo-setup-$cde_user
cde resource create --name ice-demo-setup-$cde_user --type files
cde resource upload --name ice-demo-setup-$cde_user --local-path utils.py --local-path setup.py --local-path cell_towers_1.csv --local-path cell_towers_2.csv
echo "CREATE AND RUN SETUP JOB"
cde job create --name ice-demo-setup-$cde_user --type spark --mount-1-resource ice-demo-setup-$cde_user --application-file setup.py --runtime-image-resource-name dex-spark-runtime-$cde_user
cde job run --name ice-demo-setup-$cde_user --arg $cdp_data_lake_storage

function loading_icon_job() {
  local loading_animation=( '—' "\\" '|' '/' )

  echo "${1} "

  tput civis
  trap "tput cnorm" EXIT

  while true; do
    job_status=$(cde run list --filter 'job[like]%ice-demo-setup-'$cde_user | jq -r '[last] | .[].status')
    if [[ $job_status == "succeeded" ]]; then
      echo "Setup Job Execution Completed"
      break
    else
      for frame in "${loading_animation[@]}" ; do
        printf "%s\b" "${frame}"
        sleep 1
      done
    fi
  done
  printf " \b\n"
}

loading_icon_job "Setup Job in Progress"

echo "##########################################################"
printf "${fmt}" "CDE ${cde_demo} Ice Demo deployment completed."
printf "${fmt}" "completion time:" "${e}"
printf "${fmt}" "please visit CDE Job Runs UI to view in-progress demo"
echo "##########################################################"
