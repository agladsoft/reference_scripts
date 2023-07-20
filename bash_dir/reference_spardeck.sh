#!/bin/bash

xls_path="${XL_IDP_PATH_REFERENCE}/reference_spardeck/"

done_path="${xls_path}"/done
if [ ! -d "$done_path" ]; then
  mkdir "${done_path}"
fi

json_path="${xls_path}"/json
if [ ! -d "$json_path" ]; then
  mkdir "${json_path}"
fi

find "${xls_path}" -maxdepth 1 -type f \( -name "*.xls*" -or -name "*.XLS*" \) ! -newermt '3 seconds ago' -print0 | while read -d $'\0' file
do
  if [[ "${file}" == *"error_"* ]];
  then
    continue
  fi

	mime_type=$(file -b --mime-type "$file")
  echo "'${file} - ${mime_type}'"

	# Will convert csv to json
	python3 ${XL_IDP_PATH_REFERENCE_SCRIPTS}/scripts_for_bash_with_inheritance/reference_spardeck.py "${file}" "${json_path}"

  if [ $? -eq 0 ]
	then
	  mv "${file}" "${done_path}"
	else
	  mv "${csv_name}" "${xls_path}/error_$(basename "${file}")"
	fi
done