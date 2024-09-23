#
# Load CSV tables from GS to BigQuery
#
# Provide schema JSONs in schema_path
# Run from current directory
# 

import os
import sys
import getopt
import json

# ----------------------------------------------------
'''
default config values
To override default config values, copy the keys to be overriden to a json file,
and indicate this file as --config parameter

Known issue: we always load tables to a dataset in the default user's project

Expected locations for CSVs are:
    standard vocabulary tables
        - one directory for all, 
        - each file has the same name as the target table without prefix, in UPPER case
    custom mapping files
        - one directory for all,
        - the name of the directory should be CUSTOM_MAPPING, in upper case
        - the path in the config is the path to this directory
            i.e. in the given default config the location of the custom mapping files is 
            gs://some_path/CUSTOM_MAPPING/*.csv
'''
# ----------------------------------------------------

config_default = {

    "local_athena_csv_path":    "somewhere",
    "gs_athena_csv_path":       "gs://some_path",
    "athena_csv_delimiter":     "\t",
    "athena_csv_quote":         "",

    "local_mapping_csv_path":   "custom_mapping_csv",
    "gs_mapping_csv_path":      "gs://some_path",
    "mapping_csv_delimiter":    ",",
    "mapping_csv_quote":        "\"",

    "schemas_dir_all_csv":      "omop_schemas_vocab_bq",

    "variables": 
    {
        "@bq_target_project":        "bq_target_project",
        "@bq_target_dataset":        "bq_target_dataset"
    },

    "vocabulary_tables":
    [
        "domain",
        "relationship",
        "concept_class",
        "drug_strength",
        "concept_synonym",
        "concept_ancestor",
        
        "concept",
        "concept_relationship",
        "vocabulary"
    ],

    "bq_athena_temp_table_prefix":  "tmp_",
    "custom_mapping_table":     "custom_mapping"
}

# ----------------------------------------------------
# read_params()
# ----------------------------------------------------

def read_params():

    print('Read params...')
    params = {
        "steps":    ["mandatory: indicate '-a' for 'athena', '-m' for (custom) 'mapping' or both"],
        "config_file":   "optional: indicate '-c' for 'config', json file name. Defaults are hard-coded"
    }
    
    # Parsing command line arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:],"ampc:",["athena", "mapping", "config="])
        if len(opts) == 0:
            raise getopt.GetoptError("read_params() error", "Mandatory argument is missing.")
    except getopt.GetoptError:
        print("Please indicate correct params:")
        print(params)
        print("for example:\npython load_to_bq_vocab.py --athena --config vocabulary_refresh.conf")
        sys.exit(2)

    st = []
    for opt, arg in opts:

        if opt == '-c' or opt == '--config':
            if os.path.isfile(arg):
                params['config_file'] = arg
            else:
                params['config_file'] = ''             

        if opt == '-a' or opt == '--athena':
            st.append('athena')
        if opt == '-m' or opt == '--mapping':
            st.append('mapping')
        params['steps'] = st

    print(params)
    return params

# ----------------------------------------------------
# read_config()
# ----------------------------------------------------

def read_config(config_file):
    
    print('Read config...')
    config = {}
    config_read = {}

    if os.path.isfile(config_file):
        with open(config_file) as f:
            config_read = json.load(f)
    
    for k in config_default:
        # config_read, 즉 config_file을 읽은 것에서부터 `k`의 Value를 가져오고, 없으면 default value를 가져온다.
        s = config_read.get(k, config_default[k])
        config[k] = s

    print(config)
    print('Loading tables...')
    print(config['vocabulary_tables'])
    # config_file에 있는 값은 전부 가져오고, 없는 값은 default를 이용해서 전부 채워진 `config`를 반환한다.
    return config

''' 
----------------------------------------------------
    load_table()
    return codes: 0, 1, 2
----------------------------------------------------
'''

def load_table(table, gs_path, field_delimiter, quote, config):

    return_code = 0

    schema_path = '{dir}/{table}.json'
    # bq_table = '{project}.{dataset}.{prefix}{table}'
    bq_table = '{dataset}.{prefix}{table}'

    bq_load_command = \
        "bq --location=US load --replace " + \
        " --source_format=CSV  " + \
        " --allow_quoted_newlines=True " + \
        " --skip_leading_rows=1 " + \
        " --field_delimiter=\"{field_delimiter}\" " + \
        " --quote=\"{quote}\" " + \
        " {table_name} " + \
        " \"{files_path}\" " + \
        "\"{schema_path}\" "
        # " --quote=\"\\\"\" " + \ # doesn't work for CONCEPT and CONCEPT_SYNONYM, sometimes is required for other tables
        # " --autodetect " + \ # does not work for empty tables

    table_path = bq_table.format( \
        # project=config['bq_target_project'], 
        dataset=config['variables']['@bq_target_dataset'], \
        prefix=config['bq_athena_temp_table_prefix'], table=table)
    table_schema = schema_path.format(dir=config['schemas_dir_all_csv'], table=table)
    
    # process 7: 
    # table_path = {dataset}.tmp_custom_mapping
    # gs_path = gs://mimic_iv_to_omop/custom_mapping/*.csv
    # table_schema = omop_schemas_vocab_bq/custom_mapping.json
    # 
    # 즉, tmp_custom_mapping이라는 테이블에는 `gs_path`경로에 있는 모든 csv 파일들을 넣는다.
    if os.path.isfile(table_schema):
        bqc = bq_load_command.format( \
            table_name = table_path, \
            files_path = gs_path, \
            schema_path = table_schema,
            field_delimiter=field_delimiter,
            quote=quote
        )
        print('To BQ: ' + bqc)

        try:
            os.system(bqc)
        except Exception as e:
            return_code = 2 # error during execution of the command
            raise e
    else:
        return_code = 1 # file not found
        print ('Schema file {f} is not found.'.format(f=table_schema))

    return return_code

'''
----------------------------------------------------
    main()
    return code:
        0 = success
        lower byte = number of Athena tables failed to load
        upper byte = if Custom mapping table failed to load
----------------------------------------------------
'''

def main():
    # params = {'steps': ['athena'], 'config_file': 'vocabulary_refresh.conf'} in process 3.
    # params = {'steps': ['mapping'], 'config_file': 'vocabulary_refresh.conf'} in process 7.
    params = read_params()

    # config_file에 있는 값은 전부 가져오고, 없는 값은 default를 이용해서 전부 채워진 `config`를 반환한다.
    config = read_config(params.get('config_file'))

    return_code = 0
    gs_file_path = '{gs_path}/{table}{ext}'

    for step in params['steps']:

        rca = 0
        if step == 'athena': # in process 3 of `vocabulary_refresh.py`.
            for table in config['vocabulary_tables']: # `Standardized Vocabularies` 스키마의 테이블 이름 리스트

                # 각 테이블의 경로를 생성한다.
                gs_path = gs_file_path.format(
                    gs_path=config['gs_athena_csv_path'], table=table.upper(), ext='.csv')

                # 지정된 경로에 테이블을 로딩한다. rc는 `return code`로 로딩 결과를 나타낸다.
                rc = load_table(table, gs_path, config['athena_csv_delimiter'], \
                    config['athena_csv_quote'], config)
                if rc != 0:
                    rca += 1
                    continue
        rcm = 0
        if step == 'mapping':

            # `custom_mapping_csv` 디렉터리 내 모든 csv 파일들
            gs_path = gs_file_path.format(
                gs_path=config['gs_mapping_csv_path'], table='*', ext='.csv')

            # 사용자 지정 매핑 테이블을 해당 경로에 쿼리로 불러온다.
            rc = load_table(config['custom_mapping_table'], gs_path, config['mapping_csv_delimiter'], \
                config['mapping_csv_quote'], config)
            if rc != 0:
                rcm += 1

    return_code = rca + rcm * 0xff
    return return_code

# ----------------------------------------------------
# go
# ----------------------------------------------------
return_code = main()

exit(return_code)

