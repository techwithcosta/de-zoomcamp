TAXI_TYPE=$1 # "yellow"
YEAR=$2 # 2020

URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"

for MONTH in {1..12}; do
    FMONTH=$(printf "%02d" ${MONTH})
    URL="${URL_PREFIX}${TAXI_TYPE}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"
    
    LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
    LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv.gz"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

    # Check if the file exists on the server
    if wget -q --spider ${URL}; then

        mkdir -p ${LOCAL_PREFIX}

        # Download the file
        echo "Downloading ${FMONTH}/${YEAR}"
        wget ${URL} -q -O ${LOCAL_PATH}

        # Check if the file has a .csv extension before compressing
        if [[ "${LOCAL_PATH}" != *.gz ]]; then
            echo "Compressing ${FMONTH}/${YEAR}"
            gzip ${LOCAL_PATH}
        else
            echo "Skipping compression for ${FMONTH}/${YEAR} as it is already a .gz file"
        fi
    else
        echo "File does not exist for month ${MONTH} in year ${YEAR}"
    fi
    echo ""
done
